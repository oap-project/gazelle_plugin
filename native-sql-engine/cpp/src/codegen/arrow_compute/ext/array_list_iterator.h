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

#include <arrow/status.h>
#include <gandiva/projector.h>

#include <cstdint>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/result_iterator.h"
#include "precompile/array.h"
#include "precompile/type_traits.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

// TODO: move this class into common utility
// Noticed here, array list here is as below format
// [[col_0:0, col0:1, col0:2, col0:3], [col_1:0, col1:1, col1:2, col1:3], [col_2:0,
// col2:1, col2:2, col2:3], ...]
class ArrayListIterator : public ResultIterator<arrow::RecordBatch> {
 public:
  ArrayListIterator(std::vector<arrow::ArrayVector>& cached,
                    std::shared_ptr<arrow::Schema> schema)
      : cached_(cached), schema_(schema) {
    num_columns_ = cached_.size();
    if (num_columns_ > 0) {
      num_batches_ = cached_[0].size();
    }
  }

  bool HasNext() override { return array_idx_ < num_batches_; }

  arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
    arrow::ArrayVector arrs;
    auto length = 0;
    for (auto arr_list : cached_) {
      arrs.push_back(arr_list[array_idx_]);
      length = arr_list[array_idx_]->length();
    }
    array_idx_++;
    *out = arrow::RecordBatch::Make(schema_, length, arrs);
    return arrow::Status::OK();
  }

 private:
  std::vector<arrow::ArrayVector> cached_;
  int num_columns_;
  std::shared_ptr<arrow::Schema> schema_;
  int array_idx_ = 0;
  int num_batches_ = 0;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin