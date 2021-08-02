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
  SortRelation(
      arrow::compute::ExecContext* ctx, std::shared_ptr<LazyBatchIterator> lazy_in,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list,
      uint64_t items_total, const std::vector<int>& size_array)
      : ctx_(ctx), items_total_(items_total) {
    lazy_in_ = lazy_in;
    sort_relation_key_list_ = sort_relation_key_list;
    sort_relation_payload_list_ = sort_relation_payload_list;

    if (lazy_in_ != nullptr) {
      is_lazy_input_ = true;
    }

    if (!is_lazy_input_) {
      int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
      auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
      indices_buf_ = *std::move(maybe_buffer);
      indices_begin_ = reinterpret_cast<ArrayItemIndexS*>(indices_buf_->mutable_data());
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
    } else {
      ArrayAdvanceTo(0);
    }
  }

  ~SortRelation() = default;

  static std::shared_ptr<SortRelation> CreateLegacy(
      arrow::compute::ExecContext* ctx,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list,
      uint64_t items_total, const std::vector<int>& size_array) {
    return std::make_shared<SortRelation>(ctx, nullptr, sort_relation_key_list,
                                          sort_relation_payload_list, items_total,
                                          size_array);
  }

  static std::shared_ptr<SortRelation> CreateLazy(
      arrow::compute::ExecContext* ctx, std::shared_ptr<LazyBatchIterator> lazy_in,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list) {
    return std::make_shared<SortRelation>(ctx, lazy_in, sort_relation_key_list,
                                          sort_relation_payload_list, -1L,
                                          std::vector<int>());
  }

  void ArrayRelease(int array_id) {
    for (auto col : sort_relation_payload_list_) {
      col->ReleaseArray(array_id);
    }
  }

  int32_t ArrayAdvance(int32_t array_offset) {
    int32_t result = -1;
    for (auto col : sort_relation_payload_list_) {
      int32_t granted = col->Advance(array_offset);
      if (result == -1) {
        result = granted;
        continue;
      }
      if (granted != result) {
        return -1;  // error
      }
    }
    return result;
  }

  void ArrayAdvanceTo(int array_id) {
    if (array_id <= fetched_batches_) {
      return;
    }
    int32_t fetching = (array_id / 500 + 1) * 500;
    for (auto col : sort_relation_payload_list_) {
      col->AdvanceTo(fetching);
    }
    fetched_batches_ = fetching;
  }

  void Advance(int shift) {
    int64_t batch_length = lazy_in_->GetNumRowsOfBatch(requested_batches);
    int64_t batch_remaining = (batch_length - 1) - offset_in_current_batch_;
    if (shift <= batch_remaining) {
      offset_in_current_batch_ = offset_in_current_batch_ + shift;
      return;
    }
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (true) {
      int64_t current_batch_length = lazy_in_->GetNumRowsOfBatch(batch_i);
      if (remaining <= current_batch_length) {
        requested_batches = batch_i;
        ArrayAdvanceTo(requested_batches);
	ReleaseAllRead();
        offset_in_current_batch_ = remaining - 1;
        return;
      }
      remaining -= current_batch_length;
      batch_i++;
    }
  }

  void ReleaseAllRead() {
    if (requested_batches > released_batches_ + 500) {
      return;
    }
    for (int32_t i = released_batches_ + 1; i < requested_batches; i++) {
      ArrayRelease(i);
      released_batches_ = i;
    }
  }

  ArrayItemIndexS GetItemIndexWithShift(int shift) {
    if (!is_lazy_input_) {
      return indices_begin_[offset_ + shift];
    }
    int64_t batch_length = lazy_in_->GetNumRowsOfBatch(requested_batches);
    int64_t batch_remaining = (batch_length - 1) - offset_in_current_batch_;
    if (shift <= batch_remaining) {
      ArrayItemIndexS s(requested_batches, offset_in_current_batch_ + shift);
      return s;
    }
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (true) {
      int64_t current_batch_length = lazy_in_->GetNumRowsOfBatch(batch_i);
      if (remaining <= current_batch_length) {
        ArrayAdvanceTo(batch_i);
        ArrayItemIndexS s(batch_i, remaining - 1);
        return s;
      }
      remaining -= current_batch_length;
      batch_i++;
    }
  }

  bool CheckRangeBound(int shift) {
    if (!is_lazy_input_) {
      return offset_ + shift < items_total_;
    }
    int64_t batch_length = lazy_in_->GetNumRowsOfBatch(requested_batches);
    if (batch_length == -1L) {
      return false;
    }
    int64_t batch_remaining = (batch_length - 1) - offset_in_current_batch_;
    if (shift <= batch_remaining) {
      return true;
    }
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (remaining >= 0) {
      int64_t current_batch_length = lazy_in_->GetNumRowsOfBatch(batch_i);
      if (current_batch_length == -1L) {
        return false;
      }
      ArrayAdvanceTo(batch_i);
      remaining -= current_batch_length;
      batch_i++;
    }
    return true;
  }

  // IS THIS POSSIBLY BUGGY AS THE FIRST ELEMENT DID NOT GET CHECKED?
  bool Next() {
    if (!is_lazy_input_) {
      if (!CheckRangeBound(1)) return false;
      offset_++;
      range_cache_ = -1;
      return true;
    }
    if (!CheckRangeBound(1)) return false;
    Advance(1);
    offset_++;
    range_cache_ = -1;
    return true;
  }

  bool NextNewKey() {
    if (!is_lazy_input_) {
      auto range = GetSameKeyRange();
      if (!CheckRangeBound(range)) return false;
      offset_ += range;
      range_cache_ = -1;
      return true;
    }
    auto range = GetSameKeyRange();
    if (!CheckRangeBound(range)) return false;
    Advance(range);
    offset_ += range;
    range_cache_ = -1;
    return true;
  }

  int GetSameKeyRange() {
    if (!is_lazy_input_) {
      if (range_cache_ != -1) return range_cache_;
      int range = 0;
      if (!CheckRangeBound(range)) return range;
      bool is_same = true;
      while (is_same) {
        if (CheckRangeBound(range + 1)) {
          auto cur_idx = GetItemIndexWithShift(range);
          auto cur_idx_plus_one = GetItemIndexWithShift(range + 1);
          for (auto col : sort_relation_key_list_) {
            if (!(is_same =
                      col->IsEqualTo(cur_idx.array_id, cur_idx.id,
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
    if (range_cache_ != -1) return range_cache_;
    if (!CheckRangeBound(0)) return 0;
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

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(sort_relation_payload_list_[idx]);
    return arrow::Status::OK();
  }

 protected:
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<LazyBatchIterator> lazy_in_;
  uint64_t offset_ = 0;
  int64_t offset_in_current_batch_ = 0;
  int32_t requested_batches = 0;
  int range_cache_ = -1;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_key_list_;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_payload_list_;

  // required by legacy method
  bool is_lazy_input_ = false;
  int32_t fetched_batches_ = -1;
  int32_t released_batches_ = -1;

  std::shared_ptr<arrow::Buffer> indices_buf_;
  ArrayItemIndexS* indices_begin_;
  const uint64_t items_total_;
};
