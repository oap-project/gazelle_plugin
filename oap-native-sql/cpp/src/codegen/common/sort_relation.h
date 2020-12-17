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

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/relation_column.h"
#include "precompile/type_traits.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndexS;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::FixedSizeBinaryArray;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;

class SortRelation {
 public:
  SortRelation(
      const std::shared_ptr<FixedSizeBinaryArray>& indices_in,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list)
      : indices_(indices_in),
        total_length_(indices_in->length()),
        indices_begin_((ArrayItemIndexS*)indices_in->value_data()) {
    sort_relation_key_list_ = sort_relation_key_list;
    sort_relation_payload_list_ = sort_relation_payload_list;
  }

  ~SortRelation() {}

  ArrayItemIndexS GetItemIndexWithShift(int shift) {
    return indices_begin_[offset_ + shift];
  }

  bool Next() {
    offset_++;
    range_cache_ = -1;
    if (offset_ >= total_length_) return false;
    return true;
  }

  bool NextNewKey() {
    offset_ += GetSameKeyRange();
    range_cache_ = -1;
    if (offset_ >= total_length_) return false;
    return true;
  }

  int GetSameKeyRange() {
    if (range_cache_ != -1) return range_cache_;
    int range = 0;
    bool is_same = true;
    while (is_same) {
      if (CheckRangeBound(range + 1)) {
        auto cur_idx = GetItemIndexWithShift(range);
        auto cur_idx_plus_one = GetItemIndexWithShift(range + 1);
        for (auto col : sort_relation_key_list_) {
          if (!(is_same = col->IsEqualTo(cur_idx.array_id, cur_idx.id,
                                         cur_idx_plus_one.array_id, cur_idx_plus_one.id)))
            break;
        }
      } else {
        is_same = false;
      }
      if (!is_same) break;
      range++;
    }
    range += 1;
    range_cache_ = range;
    return range;
  }

  bool CheckRangeBound(int shift) { return (offset_ + shift) < total_length_; }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(sort_relation_payload_list_[idx]);
    return arrow::Status::OK();
  }

 protected:
  uint64_t offset_ = 0;
  const ArrayItemIndexS* indices_begin_;
  const uint64_t total_length_;
  std::shared_ptr<FixedSizeBinaryArray> indices_;
  int range_cache_ = -1;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_key_list_;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_payload_list_;
};

arrow::Status MakeSortRelation(
    uint32_t key_type_id, arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_column,
    const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_column,
    std::shared_ptr<SortRelation>* out);
