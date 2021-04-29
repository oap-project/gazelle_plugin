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
#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include <cmath>

#include "sparsehash/dense_hash_map"

using google::dense_hash_map;

#define NOTFOUND -1

template <typename T, typename Enable = void>
class SparseHashMap {};

template <typename Scalar>
class SparseHashMap<Scalar, std::enable_if_t<!std::is_floating_point<Scalar>::value>> {
 public:
  SparseHashMap() { dense_map_.set_empty_key(0); }
  SparseHashMap(arrow::MemoryPool* pool) {
    dense_map_.set_empty_key(std::numeric_limits<Scalar>::max());
  }
  template <typename Func1, typename Func2>
  arrow::Status GetOrInsert(const Scalar& value, Func1&& on_found, Func2&& on_not_found,
                            int32_t* out_memo_index) {
    if (dense_map_.find(value) == dense_map_.end()) {
      auto index = size_++;
      dense_map_[value] = index;
      *out_memo_index = index;
      on_not_found(index);
    } else {
      auto index = dense_map_[value];
      *out_memo_index = index;
      on_found(index);
    }
    return arrow::Status::OK();
  }
  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_ = size_++;
      on_not_found(null_index_);
    } else {
      on_found(null_index_);
    }
    return null_index_;
  }
  int32_t Get(const Scalar& value) {
    if (dense_map_.find(value) == dense_map_.end()) {
      return NOTFOUND;
    } else {
      auto ret = dense_map_[value];
      return ret;
    }
  }
  int32_t GetNull() {
    if (!null_index_set_) {
      return NOTFOUND;
    } else {
      auto ret = null_index_;
      return ret;
    }
  }

 private:
  dense_hash_map<Scalar, int32_t> dense_map_;
  int32_t size_ = 0;
  bool null_index_set_ = false;
  int32_t null_index_;
};

template <typename Scalar>
class SparseHashMap<Scalar, std::enable_if_t<std::is_floating_point<Scalar>::value>> {
 public:
  SparseHashMap() { dense_map_.set_empty_key(0); }
  SparseHashMap(arrow::MemoryPool* pool) {
    dense_map_.set_empty_key(std::numeric_limits<Scalar>::max());
  }
  template <typename Func1, typename Func2>
  arrow::Status GetOrInsert(const Scalar& value, Func1&& on_found, Func2&& on_not_found,
                            int32_t* out_memo_index) {
    if (dense_map_.find(value) == dense_map_.end()) {
      if (!nan_index_set_) {
        auto index = size_++;
        dense_map_[value] = index;
        *out_memo_index = index;
        on_not_found(index);
        if (std::isnan(value)) {
          nan_index_set_ = true;
          nan_index_ = index;
        }
      } else {
        if (std::isnan(value)) {
          *out_memo_index = nan_index_;
          on_found(nan_index_);
        } else {
          auto index = size_++;
          dense_map_[value] = index;
          *out_memo_index = index;
          on_not_found(index);
        }
      }
    } else {
      auto index = dense_map_[value];
      *out_memo_index = index;
      on_found(index);
    }
    return arrow::Status::OK();
  }
  template <typename Func1, typename Func2>
  int32_t GetOrInsertNull(Func1&& on_found, Func2&& on_not_found) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_ = size_++;
      on_not_found(null_index_);
    } else {
      on_found(null_index_);
    }
    return null_index_;
  }
  int32_t Get(const Scalar& value) {
    if (dense_map_.find(value) == dense_map_.end()) {
      return NOTFOUND;
    } else {
      auto ret = dense_map_[value];
      return ret;
    }
  }
  int32_t GetNull() {
    if (!null_index_set_) {
      return NOTFOUND;
    } else {
      auto ret = null_index_;
      return ret;
    }
  }

 private:
  dense_hash_map<Scalar, int32_t> dense_map_;
  int32_t size_ = 0;
  bool null_index_set_ = false;
  int32_t null_index_;
  bool nan_index_set_ = false;
  int32_t nan_index_;
};
