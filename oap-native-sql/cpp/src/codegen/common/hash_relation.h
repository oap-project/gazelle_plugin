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
#include "precompile/type_traits.h"
#include "precompile/unsafe_array.h"
#include "third_party/row_wise_memory/hashMap.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;
using sparkcolumnarplugin::precompile::UnsafeArray;

class HashRelationColumn {
 public:
  virtual bool IsNull(int array_id, int id) = 0;
  virtual arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelationColumn AppendColumn is abstract.");
  };
  virtual arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) {
    return arrow::Status::NotImplemented(
        "HashRelationColumn GetArrayVector is abstract.");
  }
};

template <typename T, typename Enable = void>
class TypedHashRelationColumn {};

template <typename DataType>
class TypedHashRelationColumn<DataType, enable_if_number<DataType>>
    : public HashRelationColumn {
 public:
  using T = typename TypeTraits<DataType>::CType;
  TypedHashRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_in = std::make_shared<ArrayType>(in);
    array_vector_.push_back(typed_in);
    return arrow::Status::OK();
  }
  arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) override {
    for (auto arr : array_vector_) {
      (*out).push_back(arr->cache_);
    }
    return arrow::Status::OK();
  }
  T GetValue(int array_id, int id) { return array_vector_[array_id]->GetView(id); }

 private:
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> array_vector_;
};

template <typename DataType>
class TypedHashRelationColumn<DataType, enable_if_string_like<DataType>>
    : public HashRelationColumn {
 public:
  TypedHashRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_in = std::make_shared<StringArray>(in);
    array_vector_.push_back(typed_in);
    return arrow::Status::OK();
  }
  arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) override {
    for (auto arr : array_vector_) {
      (*out).push_back(arr->cache_);
    }
    return arrow::Status::OK();
  }
  std::string GetValue(int array_id, int id) {
    return array_vector_[array_id]->GetString(id);
  }

 private:
  std::vector<std::shared_ptr<StringArray>> array_vector_;
};

/////////////////////////////////////////////////////////////////////////

class HashRelation {
 public:
  HashRelation(
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_list) {
    hash_relation_column_list_ = hash_relation_list;
  }

  HashRelation(
      arrow::compute::FunctionContext* ctx,
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column)
      : HashRelation(hash_relation_column) {
    hash_table_ = createUnsafeHashMap(1024 * 1024 * 4, 1024 * 1024);
  }

  ~HashRelation() {
    if (hash_table_ != nullptr) {
      destroyHashMap(hash_table_);
      hash_table_ = nullptr;
    }
  }

  virtual arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelation AppendKeyColumn is abstract.");
  }

  arrow::Status AppendKeyColumn(
      std::shared_ptr<arrow::Array> in,
      const std::vector<std::shared_ptr<UnsafeArray>>& payloads) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    if (typed_array->null_count() == 0) {
      for (int i = 0; i < typed_array->length(); i++) {
        std::shared_ptr<UnsafeRow> payload = std::make_shared<UnsafeRow>();
        initTempUnsafeRow(payload.get(), payloads.size());
        for (auto payload_arr : payloads) {
          payload_arr->Append(i, &payload);
        }
        RETURN_NOT_OK(Insert(typed_array->GetView(i), payload, num_arrays_, i));
        releaseTempUnsafeRow(payload.get());
      }
    } else {
      for (int i = 0; i < typed_array->length(); i++) {
        std::shared_ptr<UnsafeRow> payload = std::make_shared<UnsafeRow>();
        initTempUnsafeRow(payload.get(), payloads.size());
        for (auto payload_arr : payloads) {
          payload_arr->Append(i, &payload);
        }
        if (typed_array->IsNull(i)) {
          RETURN_NOT_OK(InsertNull(num_arrays_, i));
        } else {
          RETURN_NOT_OK(Insert(typed_array->GetView(i), payload, num_arrays_, i));
        }
        releaseTempUnsafeRow(payload.get());
      }
    }
    num_arrays_++;
    return arrow::Status::OK();
  }

  int Get(int32_t v, std::shared_ptr<UnsafeRow> payload) {
    assert(hash_table_ != nullptr);
    return safeLookup(hash_table_, payload.get(), v);
  }

  int GetNull() {
    if (!null_index_set_) {
      return HASH_NEW_KEY;
    } else {
      auto ret = null_index_;
      return ret;
    }
  }

  arrow::Status AppendPayloadColumn(int idx, std::shared_ptr<arrow::Array> in) {
    return hash_relation_column_list_[idx]->AppendColumn(in);
  }

  arrow::Status GetArrayVector(int idx, std::vector<std::shared_ptr<arrow::Array>>* out) {
    return hash_relation_column_list_[idx]->GetArrayVector(out);
  }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(hash_relation_column_list_[idx]);
    return arrow::Status::OK();
  }

  std::vector<ArrayItemIndex> GetItemListByIndex(int i) {
    return memo_index_to_arrayid_[i];
  }

  uint64_t GetHashRelationLength() { return num_items_; }

 protected:
  uint64_t num_items_ = 0;
  uint64_t num_arrays_ = 0;
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  std::vector<std::shared_ptr<HashRelationColumn>> hash_relation_column_list_;
  arrow::Status Insert(int32_t v, std::shared_ptr<UnsafeRow> payload, uint32_t array_id,
                       uint32_t id) {
    assert(hash_table_ != nullptr);
    int i = getOrInsert(hash_table_, payload.get(), v, num_items_);
    if (i < num_items_) {
      memo_index_to_arrayid_[i].emplace_back(array_id, id);
    } else {
      num_items_++;
      memo_index_to_arrayid_.push_back({ArrayItemIndex(array_id, id)});
    }
    return arrow::Status::OK();
  }

  arrow::Status InsertNull(uint32_t array_id, uint32_t id) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_ = num_items_++;
      memo_index_to_arrayid_.push_back({ArrayItemIndex(array_id, id)});
    } else {
      memo_index_to_arrayid_[null_index_].emplace_back(array_id, id);
    }
    return arrow::Status::OK();
  }

  unsafeHashMap* hash_table_ = nullptr;
  using ArrayType = sparkcolumnarplugin::precompile::Int32Array;
  bool null_index_set_ = false;
  int32_t null_index_;
};

template <typename T, typename Enable = void>
class TypedHashRelation {};

arrow::Status MakeHashRelationColumn(uint32_t data_type_id,
                                     std::shared_ptr<HashRelationColumn>* out);

arrow::Status MakeHashRelation(
    uint32_t key_type_id, arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
    std::shared_ptr<HashRelation>* out);