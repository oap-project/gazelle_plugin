/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <utility>

#include <arrow/filesystem/path_util.h>
#include <arrow/ipc/reader.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include "shuffle/splitter.h"

namespace sparkcolumnarplugin {
namespace shuffle {

SplitOptions SplitOptions::Defaults() { return SplitOptions(); }

// ----------------------------------------------------------------------
// Splitter

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions, const gandiva::ExpressionVector& expr_vector,
    SplitOptions options) {
  if (short_name == "hash") {
    return HashSplitter::Create(num_partitions, std::move(schema), expr_vector,
                                std::move(options));
  } else if (short_name == "rr") {
    return RoundRobinSplitter::Create(num_partitions, std::move(schema),
                                      std::move(options));
  } else if (short_name == "range") {
    return FallbackRangeSplitter::Create(num_partitions, std::move(schema),
                                         std::move(options));
  } else if (short_name == "single") {
    return RoundRobinSplitter::Create(1, std::move(schema), std::move(options));
  }
  return arrow::Status::NotImplemented("Partitioning " + short_name +
                                       " not supported yet.");
}

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions, SplitOptions options) {
  return Make(short_name, std::move(schema), num_partitions, {}, std::move(options));
}

arrow::Status Splitter::Init() {
  const auto& fields = schema_->fields();
  ARROW_ASSIGN_OR_RAISE(column_type_id_, ToSplitterTypeId(schema_->fields()));

  std::vector<Type::typeId> remove_null_id;
  remove_null_id.reserve(column_type_id_.size());
  std::copy_if(std::cbegin(column_type_id_), std::cend(column_type_id_),
               std::back_inserter(remove_null_id),
               [](Type::typeId id) { return id != Type::typeId::SHUFFLE_NULL; });
  last_type_id_ =
      *std::max_element(std::cbegin(remove_null_id), std::cend(remove_null_id));

  partition_lengths_.reserve(num_partitions_);
  partition_writer_.resize(num_partitions_);

  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary dir with
  // prefix "columnar-shuffle"
  if (options_.data_file.length() == 0) {
    ARROW_ASSIGN_OR_RAISE(options_.data_file, CreateTempShuffleFile(configured_dirs_[0]));
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::DoSplit(const arrow::RecordBatch& rb,
                                const std::vector<int32_t>& writer_idx) {
  auto num_rows = rb.num_rows();
  auto num_cols = rb.num_columns();
  auto src_addr = std::vector<SrcBuffers>(Type::NUM_TYPES);

  auto src_binary_arr = SrcBinaryArrays();
  auto src_large_binary_arr = SrcLargeBinaryArrays();

  arrow::TypedBufferBuilder<bool> null_bitmap_builder_;
  RETURN_NOT_OK(null_bitmap_builder_.Append(num_rows, true));

  std::shared_ptr<arrow::Buffer> dummy_buf;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&dummy_buf));
  auto dummy_buf_p = const_cast<uint8_t*>(dummy_buf->data());

  // Get the pointer of each buffer id
  for (auto i = 0; i < num_cols; ++i) {
    const auto& buffers = rb.column_data(i)->buffers;
    if (column_type_id_[i] == Type::SHUFFLE_BINARY) {
      src_binary_arr.push_back(
          std::static_pointer_cast<arrow::BinaryArray>(rb.column(i)));
    } else if (column_type_id_[i] == Type::SHUFFLE_LARGE_BINARY) {
      src_large_binary_arr.push_back(
          std::static_pointer_cast<arrow::LargeBinaryArray>(rb.column(i)));
    } else if (column_type_id_[i] != Type::SHUFFLE_NULL) {
      if (rb.column_data(i)->GetNullCount() == 0) {
        // null bitmap may be nullptr
        src_addr[column_type_id_[i]].push_back(
            {.validity_addr = dummy_buf_p,
             .value_addr = const_cast<uint8_t*>(buffers[1]->data())});
      } else {
        src_addr[column_type_id_[i]].push_back(
            {.validity_addr = const_cast<uint8_t*>(buffers[0]->data()),
             .value_addr = const_cast<uint8_t*>(buffers[1]->data())});
      }
    }
  }

  auto read_offset = 0;

#define WRITE_FIXEDWIDTH(TYPE_ID, T)                                                 \
  if (!src_addr[TYPE_ID].empty()) {                                                  \
    for (i = read_offset; i < num_rows; ++i) {                                       \
      ARROW_ASSIGN_OR_RAISE(auto result, partition_writer_[writer_idx[i]]->Write<T>( \
                                             TYPE_ID, src_addr[TYPE_ID], i))         \
      if (!result) {                                                                 \
        break;                                                                       \
      }                                                                              \
    }                                                                                \
  }

#define WRITE_DECIMAL                                                          \
  if (!src_addr[Type::SHUFFLE_DECIMAL128].empty()) {                           \
    for (i = read_offset; i < num_rows; ++i) {                                 \
      ARROW_ASSIGN_OR_RAISE(auto result,                                       \
                            partition_writer_[writer_idx[i]]->WriteDecimal128( \
                                src_addr[Type::SHUFFLE_DECIMAL128], i))        \
      if (!result) {                                                           \
        break;                                                                 \
      }                                                                        \
    }                                                                          \
  }

#define WRITE_BINARY(func, T, src_arr)                                          \
  if (!src_arr.empty()) {                                                       \
    for (i = read_offset; i < num_rows; ++i) {                                  \
      ARROW_ASSIGN_OR_RAISE(auto result,                                        \
                            partition_writer_[writer_idx[i]]->func(src_arr, i)) \
      if (!result) {                                                            \
        break;                                                                  \
      }                                                                         \
    }                                                                           \
  }

  while (read_offset < num_rows) {
    auto i = read_offset;
    WRITE_FIXEDWIDTH(Type::SHUFFLE_1BYTE, uint8_t);
    WRITE_FIXEDWIDTH(Type::SHUFFLE_2BYTE, uint16_t);
    WRITE_FIXEDWIDTH(Type::SHUFFLE_4BYTE, uint32_t);
    WRITE_FIXEDWIDTH(Type::SHUFFLE_8BYTE, uint64_t);
    WRITE_FIXEDWIDTH(Type::SHUFFLE_BIT, bool);
    WRITE_DECIMAL
    WRITE_BINARY(WriteBinary, arrow::BinaryType, src_binary_arr);
    WRITE_BINARY(WriteLargeBinary, arrow::LargeBinaryType, src_large_binary_arr);
    read_offset = i;
  }

#undef WRITE_FIXEDWIDTH
#undef WRITE_DECIMAL
#undef WRITE_BINARY

  return arrow::Status::OK();
}  // namespace shuffle

arrow::Status Splitter::Split(const arrow::RecordBatch& rb) {
  ARROW_ASSIGN_OR_RAISE(auto writer_idx, GetNextBatchPartitionWriterIndex(rb));
  return DoSplit(rb, writer_idx);
}

arrow::Status Splitter::Stop() {
  // open data file output stream
  ARROW_ASSIGN_OR_RAISE(data_file_os_,
                        arrow::io::FileOutputStream::Open(options_.data_file, true));

  // stop PartitionWriter and collect metrics
  for (const auto& writer : partition_writer_) {
    if (writer != nullptr) {
      RETURN_NOT_OK(writer->Stop());
      auto length = writer->GetPartitionLength();
      partition_lengths_.push_back(length);
      total_bytes_written_ += length;
      total_bytes_spilled_ += writer->GetBytesSpilled();
      total_write_time_ += writer->GetWriteTime();
      total_spill_time_ += writer->GetSpillTime();
    } else {
      partition_lengths_.push_back(0);
    }
  }

  // close data file output Stream
  RETURN_NOT_OK(data_file_os_->Close());

  return arrow::Status::OK();
}

arrow::Status Splitter::CreatePartitionWriter(int32_t partition_id) {
  if (partition_writer_[partition_id] == nullptr) {
    ARROW_ASSIGN_OR_RAISE(auto spilled_file_dir,
                          GetSpilledShuffleFileDir(configured_dirs_[dir_selection_],
                                                   sub_dir_selection_[dir_selection_]))
    sub_dir_selection_[dir_selection_] =
        (sub_dir_selection_[dir_selection_] + 1) % options_.num_sub_dirs;
    dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
    ARROW_ASSIGN_OR_RAISE(
        partition_writer_[partition_id],
        PartitionWriter::Create(partition_id, options_.buffer_size,
                                options_.compression_type, last_type_id_, column_type_id_,
                                schema_, data_file_os_, spilled_file_dir));
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// RoundRobinSplitter

arrow::Result<std::shared_ptr<RoundRobinSplitter>> RoundRobinSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema, SplitOptions options) {
  std::shared_ptr<RoundRobinSplitter> res(
      new RoundRobinSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Result<std::vector<int32_t>> RoundRobinSplitter::GetNextBatchPartitionWriterIndex(
    const arrow::RecordBatch& rb) {
  auto num_rows = rb.num_rows();

  std::vector<int32_t> res;
  res.reserve(num_rows);
  for (auto i = 0; i < num_rows; ++i) {
    RETURN_NOT_OK(CreatePartitionWriter(pid_selection_));
    res.push_back(pid_selection_);
    pid_selection_ = (pid_selection_ + 1) % num_partitions_;
  }
  return res;
}

// ----------------------------------------------------------------------
// HashSplitter

arrow::Result<std::shared_ptr<HashSplitter>> HashSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
    const gandiva::ExpressionVector& expr_vector, SplitOptions options) {
  std::shared_ptr<HashSplitter> res(
      new HashSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  RETURN_NOT_OK(res->CreateProjector(expr_vector));
  return res;
}

arrow::Status HashSplitter::CreateProjector(
    const gandiva::ExpressionVector& expr_vector) {
  // same seed as spark's
  auto hash = gandiva::TreeExprBuilder::MakeLiteral((int32_t)42);
  for (const auto& expr : expr_vector) {
    switch (expr->root()->return_type()->id()) {
      case arrow::NullType::type_id:
        break;
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::Int32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash32_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::Int64Type::type_id:
      case arrow::DoubleType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash64_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::StringType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hashbuf_spark", {expr->root(), hash}, arrow::int32());
        break;
      default:
        hash = gandiva::TreeExprBuilder::MakeFunction("hash32", {expr->root(), hash},
                                                      arrow::int32());
        /*return arrow::Status::NotImplemented("HashSplitter::CreateProjector doesn't
           support type ", expr->result()->type()->ToString());*/
    }
  }
  auto hash_expr =
      gandiva::TreeExprBuilder::MakeExpression(hash, arrow::field("pid", arrow::int32()));
  return gandiva::Projector::Make(schema_, {hash_expr}, &projector_);
}

arrow::Result<std::vector<int32_t>> HashSplitter::GetNextBatchPartitionWriterIndex(
    const arrow::RecordBatch& rb) {
  arrow::ArrayVector outputs;
  TIME_NANO_OR_RAISE(total_compute_pid_time_,
                     projector_->Evaluate(rb, arrow::default_memory_pool(), &outputs));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("Projector result should have one field, actual is ",
                                  std::to_string(outputs.size()));
  }

  auto num_rows = rb.num_rows();
  auto pid_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  std::vector<int32_t> res;
  res.reserve(num_rows);
  for (auto i = 0; i < num_rows; ++i) {
    // positive mod
    auto pid = pid_arr->Value(i) % num_partitions_;
    if (pid < 0) pid = (pid + num_partitions_) % num_partitions_;
    RETURN_NOT_OK(CreatePartitionWriter(pid));
    res.push_back(pid);
  }
  return res;
}

// ----------------------------------------------------------------------
// FallBackRangeSplitter

arrow::Result<std::shared_ptr<FallbackRangeSplitter>> FallbackRangeSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema, SplitOptions options) {
  auto res = std::shared_ptr<FallbackRangeSplitter>(
      new FallbackRangeSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status FallbackRangeSplitter::Init() {
  input_schema_ = std::move(schema_);
  ARROW_ASSIGN_OR_RAISE(schema_, input_schema_->RemoveField(0))
  return Splitter::Init();
}

arrow::Status FallbackRangeSplitter::Split(const arrow::RecordBatch& rb) {
  ARROW_ASSIGN_OR_RAISE(auto writer_idx, GetNextBatchPartitionWriterIndex(rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb.RemoveColumn(0));
  return DoSplit(*remove_pid, writer_idx);
}

arrow::Result<std::vector<int32_t>>
FallbackRangeSplitter::GetNextBatchPartitionWriterIndex(const arrow::RecordBatch& rb) {
  auto num_rows = rb.num_rows();
  std::vector<int32_t> res;
  res.reserve(num_rows);

  if (rb.column(0)->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("RecordBatch field 0 should be ",
                                  arrow::int32()->ToString(), ", actual is ",
                                  rb.column(0)->type()->ToString());
  }
  auto pid_arr = reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i];
    if (pid >= num_partitions_) {
      return arrow::Status::Invalid("Partition id ", std::to_string(pid),
                                    " is equal or greater than ",
                                    std::to_string(num_partitions_));
    }
    RETURN_NOT_OK(CreatePartitionWriter(pid));
    res.push_back(pid);
  }
  return res;
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
