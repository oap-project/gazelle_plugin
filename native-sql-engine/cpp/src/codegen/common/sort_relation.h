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

#include <arrow/buffer.h>
#include <arrow/compute/api.h>
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
  SortRelation(arrow::compute::ExecContext* ctx, uint64_t items_total,
               const std::vector<int>& size_array,
               const std::vector<std::shared_ptr<RelationColumn>>&
                   sort_relation_key_list,
               const std::vector<std::shared_ptr<RelationColumn>>&
                   sort_relation_payload_list)
      : ctx_(ctx), items_total_(items_total) {
    sort_relation_key_list_ = sort_relation_key_list;
    sort_relation_payload_list_ = sort_relation_payload_list;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf_ = *std::move(maybe_buffer);
    indices_begin_ =
        reinterpret_cast<ArrayItemIndexS*>(indices_buf_->mutable_data());
    uint64_t idx = 0;
    int array_id = 0;
    for (auto size : size_array) {
      for (int id = 0; id < size; id++) {
        indices_begin_[idx].array_id = array_id;
        indices_begin_[idx].id = id;
        idx++;
      }
      array_id++;
    }

    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
  }

  ~SortRelation() {}

  ArrayItemIndexS GetItemIndexWithShift(int shift) {
    return indices_begin_[offset_ + shift];
  }

  bool Next() {
    if (!CheckRangeBound(1)) return false;
    offset_++;
    range_cache_ = -1;
    return true;
  }

  bool NextNewKey() {
    auto range = GetSameKeyRange();
    if (!CheckRangeBound(range)) return false;
    offset_ += range;
    range_cache_ = -1;

    return true;
  }

  int GetSameKeyRange() {
    if (range_cache_ != -1) return range_cache_;
    int range = 0;
    if (!CheckRangeBound(range)) return range;
    bool is_same = true;
    while (is_same) {
      if (CheckRangeBound(range + 1)) {
        auto cur_idx = GetItemIndexWithShift(range);
        auto cur_idx_plus_one = GetItemIndexWithShift(range + 1);
        for (auto col : sort_relation_key_list_) {
          if (!(is_same = col->IsEqualTo(cur_idx.array_id, cur_idx.id,
                                         cur_idx_plus_one.array_id,
                                         cur_idx_plus_one.id)))
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

  bool CheckRangeBound(int shift) { return offset_ + shift < items_total_; }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(sort_relation_payload_list_[idx]);
    return arrow::Status::OK();
  }

 protected:
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Buffer> indices_buf_;
  ArrayItemIndexS* indices_begin_;
  const uint64_t items_total_;
  uint64_t offset_ = 0;
  int range_cache_ = -1;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_key_list_;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_payload_list_;
};
