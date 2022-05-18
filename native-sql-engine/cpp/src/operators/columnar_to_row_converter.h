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

#pragma once

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

#include <boost/align.hpp>

#include "gandiva/decimal_type_util.h"

namespace sparkcolumnarplugin {
namespace columnartorow {

class ColumnarToRowConverter {
 public:
  ColumnarToRowConverter(arrow::MemoryPool* memory_pool)
      : memory_pool_(memory_pool), buffer_(nullptr) {}

  arrow::Status Init(const std::shared_ptr<arrow::RecordBatch>& rb);
  arrow::Status Write();

  uint8_t* GetBufferAddress() { return buffer_address_; }
  const std::vector<int32_t>& GetOffsets() { return offsets_; }
  const std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>>&
  GetLengths() {
    return lengths_;
  }

 protected:
  std::vector<int32_t> buffer_cursor_;
  std::shared_ptr<arrow::RecordBatch> rb_;
  std::shared_ptr<arrow::Buffer> buffer_;
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  int32_t nullBitsetWidthInBytes_;
  int32_t num_cols_;
  int32_t num_rows_;
  uint8_t* buffer_address_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>> lengths_;
};

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin
