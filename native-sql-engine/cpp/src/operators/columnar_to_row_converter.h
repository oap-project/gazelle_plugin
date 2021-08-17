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

#include "gandiva/decimal_type_util.h"

namespace sparkcolumnarplugin {
namespace columnartorow {

class ColumnarToRowConverter {
 public:
  ColumnarToRowConverter(std::shared_ptr<arrow::RecordBatch> rb, uint8_t* buffer_address,
                         int64_t memory_size = 0)
      : rb_(rb), buffer_address_(buffer_address), memory_size_(memory_size) {}
  ~ColumnarToRowConverter();

  arrow::Status Init();
  arrow::Status Write();

  uint8_t* GetBufferAddress() { return buffer_address_; }  // for test
  int64_t* GetOffsets() { return offsets_; }
  int64_t* GetLengths() { return lengths_; }

 protected:
  std::vector<int64_t> buffer_cursor_;
  std::shared_ptr<arrow::RecordBatch> rb_;
  int64_t nullBitsetWidthInBytes_;
  int64_t num_cols_;
  int64_t num_rows_;
  uint8_t* buffer_address_;
  int64_t memory_size_;
  int64_t* offsets_;
  int64_t* lengths_;
};

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin
