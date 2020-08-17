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

#include "shuffle/partition_writer.h"

#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <chrono>
#include <memory>
#include "utils.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace shuffle {

arrow::Result<std::shared_ptr<PartitionWriter>> PartitionWriter::Create(
    int32_t pid, int64_t capacity, Type::typeId last_type,
    const std::vector<Type::typeId>& column_type_id,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& temp_file_path,
    arrow::Compression::type compression_type) {
  auto buffers = TypeBufferInfos(Type::NUM_TYPES);
  auto binary_bulders = BinaryBuilders();
  auto large_binary_bulders = LargeBinaryBuilders();

  for (auto type_id : column_type_id) {
    switch (type_id) {
      case Type::SHUFFLE_BINARY: {
        std::unique_ptr<arrow::BinaryBuilder> builder;
        builder.reset(new arrow::BinaryBuilder(arrow::default_memory_pool()));
        binary_bulders.push_back(std::move(builder));
      } break;
      case Type::SHUFFLE_LARGE_BINARY: {
        std::unique_ptr<arrow::LargeBinaryBuilder> builder;
        builder.reset(new arrow::LargeBinaryBuilder(arrow::default_memory_pool()));
        large_binary_bulders.push_back(std::move(builder));
      } break;
      case Type::SHUFFLE_NULL: {
        buffers[type_id].push_back(std::unique_ptr<BufferInfo>(
            new BufferInfo{.validity_buffer = nullptr, .value_buffer = nullptr}));
      } break;
      default: {
        std::shared_ptr<arrow::Buffer> validity_buffer;
        std::shared_ptr<arrow::Buffer> value_buffer;
        uint8_t* validity_addr;
        uint8_t* value_addr;

        ARROW_ASSIGN_OR_RAISE(validity_buffer, arrow::AllocateEmptyBitmap(capacity))
        if (type_id == Type::SHUFFLE_BIT) {
          ARROW_ASSIGN_OR_RAISE(value_buffer, arrow::AllocateEmptyBitmap(capacity))
        } else {
          ARROW_ASSIGN_OR_RAISE(value_buffer,
                                arrow::AllocateBuffer(capacity * (1 << type_id)))
        }
        validity_addr = validity_buffer->mutable_data();
        value_addr = value_buffer->mutable_data();
        buffers[type_id].push_back(std::unique_ptr<BufferInfo>(
            new BufferInfo{.validity_buffer = std::move(validity_buffer),
                              .value_buffer = std::move(value_buffer),
                              .validity_addr = validity_addr,
                              .value_addr = value_addr}));
      } break;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto file_os,
                        arrow::io::FileOutputStream::Open(temp_file_path, true));

  return std::make_shared<PartitionWriter>(
      pid, capacity, last_type, column_type_id, schema, std::move(file_os),
      std::move(buffers), std::move(binary_bulders), std::move(large_binary_bulders),
      compression_type);
}

arrow::Status PartitionWriter::Stop() {
  if (write_offset_[last_type_] != 0) {
    TIME_MICRO_OR_RAISE(write_time_, WriteArrowRecordBatch());
    std::fill(std::begin(write_offset_), std::end(write_offset_), 0);
  }
  if (file_writer_opened_) {
    RETURN_NOT_OK(file_writer_->Close());
    file_writer_opened_ = false;
  }
  if (!file_os_->closed()) {
    ARROW_ASSIGN_OR_RAISE(file_footer_, file_os_->Tell());
    return file_os_->Close();
  }
  return arrow::Status::OK();
}

arrow::Status PartitionWriter::WriteArrowRecordBatch() {
  std::vector<std::shared_ptr<arrow::Array>> arrays(schema_->num_fields());
  for (int i = 0; i < schema_->num_fields(); ++i) {
    auto type_id = column_type_id_[i];
    if (type_id == Type::SHUFFLE_BINARY) {
      auto builder = std::move(binary_builders_.front());
      binary_builders_.pop_front();
      RETURN_NOT_OK(builder->Finish(&arrays[i]));
      binary_builders_.push_back(std::move(builder));
    } else if (type_id == Type::SHUFFLE_LARGE_BINARY) {
      auto builder = std::move(large_binary_builders_.front());
      large_binary_builders_.pop_front();
      RETURN_NOT_OK(builder->Finish(&arrays[i]));
      large_binary_builders_.push_back(std::move(builder));
    } else {
      auto buf_msg_ptr = std::move(buffers_[type_id].front());
      buffers_[type_id].pop_front();
      auto arr = arrow::ArrayData::Make(
          schema_->field(i)->type(), write_offset_[last_type_],
          std::vector<std::shared_ptr<arrow::Buffer>>{buf_msg_ptr->validity_buffer,
                                                      buf_msg_ptr->value_buffer});
      arrays[i] = arrow::MakeArray(arr);
      buffers_[type_id].push_back(std::move(buf_msg_ptr));
    }
  }
  auto record_batch =
      arrow::RecordBatch::Make(schema_, write_offset_[last_type_], std::move(arrays));

  if (!file_writer_opened_) {
    auto res = arrow::ipc::NewStreamWriter(file_os_.get(), schema_,
                                           GetIpcWriteOptions(compression_type_));
    RETURN_NOT_OK(res.status());
    file_writer_ = *res;
    file_writer_opened_ = true;
  }
  RETURN_NOT_OK(file_writer_->WriteRecordBatch(*record_batch));

  return arrow::Status::OK();
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
