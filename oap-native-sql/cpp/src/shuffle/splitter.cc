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
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include <utility>

#include "shuffle/splitter.h"

namespace sparkcolumnarplugin {
namespace shuffle {

// ----------------------------------------------------------------------
// Splitter

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions, const gandiva::ExpressionVector& expr_vector) {
  if (short_name == "hash") {
    return HashSplitter::Create(num_partitions, std::move(schema), expr_vector);
  } else if (short_name == "rr") {
    return RoundRobinSplitter::Create(num_partitions, std::move(schema));
  } else if (short_name == "range") {
    return FallbackRangeSplitter::Create(num_partitions, std::move(schema));
  } else if (short_name == "single") {
    return RoundRobinSplitter::Create(1, std::move(schema));
  }
  return arrow::Status::NotImplemented("Partitioning " + short_name +
                                       " not supported yet.");
}

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
    int num_partitions) {
  return Make(short_name, std::move(schema), num_partitions, {});
}

Splitter::~Splitter() = default;

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

  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs())
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  partition_writer_.resize(num_partitions_);

  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

  return arrow::Status::OK();
}

arrow::Status Splitter::DoSplit(const arrow::RecordBatch& rb,
                                std::vector<int32_t> writer_idx) {
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

#define WRITE_DECIMAL                                                         \
  if (!src_addr[Type::SHUFFLE_DECIMAL128].empty()) {                          \
    for (i = read_offset; i < num_rows; ++i) {                                \
      ARROW_ASSIGN_OR_RAISE(auto result,                                      \
                            partition_writer_[writer_idx[i]]->WriteDecimal128( \
                                src_addr[Type::SHUFFLE_DECIMAL128], i))       \
      if (!result) {                                                          \
        break;                                                                \
      }                                                                       \
    }                                                                         \
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
}

arrow::Status Splitter::Stop() {
  for (const auto& writer : partition_writer_) {
    if (writer != nullptr) {
      RETURN_NOT_OK(writer->Stop());
      ARROW_ASSIGN_OR_RAISE(auto b, writer->GetBytesWritten());
      total_bytes_written_ += b;
      total_write_time_ += writer->GetWriteTime();
    }
  }
  std::sort(std::begin(partition_file_info_), std::end(partition_file_info_));
  return arrow::Status::OK();
}

arrow::Result<std::string> Splitter::CreateDataFile() {
  int m = configured_dirs_.size();
  ARROW_ASSIGN_OR_RAISE(auto data_file,
                        CreateTempShuffleFile(fs_, configured_dirs_[dir_selection_],
                                              sub_dir_selection_[dir_selection_]))
  sub_dir_selection_[dir_selection_] =
      (sub_dir_selection_[dir_selection_] + 1) % num_sub_dirs_;
  dir_selection_ = (dir_selection_ + 1) % m;
  return data_file;
}

arrow::Status Splitter::Split(const arrow::RecordBatch& rb) {
  ARROW_ASSIGN_OR_RAISE(auto writers, GetNextBatchPartitionWriterIndex(rb));
  return DoSplit(rb, std::move(writers));
}

// ----------------------------------------------------------------------
// RoundRobinSplitter

arrow::Result<std::shared_ptr<RoundRobinSplitter>> RoundRobinSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<RoundRobinSplitter> res(
      new RoundRobinSplitter(num_partitions, std::move(schema)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Result<std::vector<int>> RoundRobinSplitter::GetNextBatchPartitionWriterIndex(
    const arrow::RecordBatch& rb) {
  auto num_rows = rb.num_rows();

  std::vector<int32_t> res;
  res.reserve(num_rows);
  for (auto i = 0; i < num_rows; ++i) {
    if (partition_writer_[pid_selection_] == nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto file_path, CreateDataFile())
      partition_file_info_.emplace_back(pid_selection_, std::move(file_path));

      ARROW_ASSIGN_OR_RAISE(
          partition_writer_[pid_selection_],
          PartitionWriter::Create(pid_selection_, buffer_size_, last_type_id_,
                                  column_type_id_, schema_,
                                  partition_file_info_.back().second, compression_type_));
    }
    res.push_back(pid_selection_);
    pid_selection_ = (pid_selection_ + 1) % num_partitions_;
  }
  return res;
}

// ----------------------------------------------------------------------
// HashSplitter

arrow::Result<std::shared_ptr<HashSplitter>> HashSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
    const gandiva::ExpressionVector& expr_vector) {
  std::shared_ptr<HashSplitter> res(new HashSplitter(num_partitions, std::move(schema)));
  RETURN_NOT_OK(res->Init());
  RETURN_NOT_OK(res->CreateProjector(expr_vector));
  return res;
}

arrow::Status HashSplitter::CreateProjector(
    const gandiva::ExpressionVector& expr_vector) {
  // same seed as spark's
  auto hash = gandiva::TreeExprBuilder::MakeLiteral((int32_t)42);
  for (const auto& expr : expr_vector) {
    if (!expr->result()->type()->Equals(arrow::null())) {
      hash = gandiva::TreeExprBuilder::MakeFunction("hash32", {expr->root(), hash},
                                                    arrow::int32());
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
    if (partition_writer_[pid] == nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto file_path, CreateDataFile())
      partition_file_info_.emplace_back(pid, std::move(file_path));

      ARROW_ASSIGN_OR_RAISE(
          partition_writer_[pid],
          PartitionWriter::Create(pid, buffer_size_, last_type_id_, column_type_id_,
                                  schema_, partition_file_info_.back().second,
                                  compression_type_))
    }
    res.push_back(pid);
  }
  return res;
}

// ----------------------------------------------------------------------
// FallBackRangeSplitter

arrow::Result<std::shared_ptr<FallbackRangeSplitter>> FallbackRangeSplitter::Create(
    int32_t num_partitions, std::shared_ptr<arrow::Schema> schema) {
  auto res = std::shared_ptr<FallbackRangeSplitter>(
      new FallbackRangeSplitter(num_partitions, std::move(schema)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status FallbackRangeSplitter::Init() {
  input_schema_ = std::move(schema_);
  ARROW_ASSIGN_OR_RAISE(schema_, input_schema_->RemoveField(0))
  return Splitter::Init();
}

arrow::Status FallbackRangeSplitter::Split(const arrow::RecordBatch& rb) {
  ARROW_ASSIGN_OR_RAISE(auto writers, GetNextBatchPartitionWriterIndex(rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb.RemoveColumn(0));
  return DoSplit(*remove_pid, std::move(writers));
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
    if (partition_writer_[pid] == nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto file_path, CreateDataFile())
      partition_file_info_.emplace_back(pid, std::move(file_path));

      ARROW_ASSIGN_OR_RAISE(
          partition_writer_[pid],
          PartitionWriter::Create(pid, buffer_size_, last_type_id_, column_type_id_,
                                  schema_, partition_file_info_.back().second,
                                  compression_type_))
    }
    res.push_back(pid);
  }
  return res;
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
