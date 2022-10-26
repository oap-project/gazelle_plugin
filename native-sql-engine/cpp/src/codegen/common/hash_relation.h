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

#include <arrow/compute/api.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include <map>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/type_traits.h"
#include "precompile/unsafe_array.h"
#include "third_party/murmurhash/murmurhash32.h"
#include "third_party/row_wise_memory/hashMap.h"
#include "utils/macros.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_number_or_decimal;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::is_number_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;
using sparkcolumnarplugin::precompile::UnsafeArray;
using sparkcolumnarplugin::thirdparty::murmurhash32::hash32;

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
  virtual bool HasNull() = 0;
};

template <typename T, typename Enable = void>
class TypedHashRelationColumn {};

template <typename DataType>
class TypedHashRelationColumn<DataType, enable_if_number_or_decimal<DataType>>
    : public HashRelationColumn {
 public:
  using T = typename TypeTraits<DataType>::CType;
  TypedHashRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) override {
    auto typed_in = std::make_shared<ArrayType>(in);
    if (typed_in->null_count() > 0) has_null_ = true;
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
  bool HasNull() { return has_null_; }

 private:
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> array_vector_;
  bool has_null_ = false;
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
    if (typed_in->null_count() > 0) has_null_ = true;
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
  bool HasNull() { return has_null_; }

 private:
  std::vector<std::shared_ptr<StringArray>> array_vector_;
  bool has_null_ = false;
};

/////////////////////////////////////////////////////////////////////////

class HashRelation {
 public:
  HashRelation(arrow::compute::ExecContext* ctx) : ctx_(ctx) {}

  HashRelation(
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_list) {
    hash_relation_column_list_ = hash_relation_list;
  }

  HashRelation(
      arrow::compute::ExecContext* ctx,
      const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
      int key_size = -1)
      : HashRelation(hash_relation_column) {
    key_size_ = key_size;
    ctx_ = ctx;
    arrayid_list_.reserve(64);
  }

  ~HashRelation() {
    if (hash_table_ != nullptr && !unsafe_set) {
      destroyHashMap(hash_table_);
      hash_table_ = nullptr;
    }
  }

  arrow::Status InitHashTable(int init_key_capacity, int initial_bytesmap_capacity) {
    if (init_key_capacity < 0 || initial_bytesmap_capacity < 0) {
      THROW_NOT_OK(arrow::Status::Invalid(
          "initialization size is overflowed, init_key_capacity is ", init_key_capacity,
          ", initial_bytesmap_capacity is ", initial_bytesmap_capacity));
    }
    hash_table_ = createUnsafeHashMap(ctx_->memory_pool(), init_key_capacity,
                                      initial_bytesmap_capacity, key_size_);
    return arrow::Status::OK();
  }

  virtual arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("HashRelation AppendKeyColumn is abstract.");
  }

  arrow::Status Minimize() {
    if (hash_table_ == nullptr) {
      return arrow::Status::OK();
    }
    if (shrinkToFit(hash_table_)) {
      return arrow::Status::OK();
    }
    return arrow::Status::Invalid("Error minimizing hash table");
  }

  void setHashMapType(bool isBHJ) { isBHJ_ = isBHJ; }

  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in,
                                const std::vector<std::shared_ptr<UnsafeArray>>& payloads,
                                bool semi = false) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    std::shared_ptr<UnsafeRow> payload = std::make_shared<UnsafeRow>(payloads.size());
    for (int i = 0; i < typed_array->length(); i++) {
      payload->reset();
      for (auto payload_arr : payloads) {
        payload_arr->Append(i, &payload);
      }
      // chendi: Since spark won't join rows contain null, we will skip null
      // row.
      if (payload->isNullExists()) continue;
      if (!semi) {
        RETURN_NOT_OK(Insert(typed_array->GetView(i), payload, num_arrays_, i));

      } else {
        RETURN_NOT_OK(InsertSkipDup(typed_array->GetView(i), payload, num_arrays_, i));
      }
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  template <typename KeyArrayType,
            typename std::enable_if_t<!std::is_same<KeyArrayType, StringArray>::value>* =
                nullptr>
  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in,
                                std::shared_ptr<KeyArrayType> original_key,
                                bool semi = false) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    if (original_key->null_count() == 0) {
      for (int i = 0; i < typed_array->length(); i++) {
        RETURN_NOT_OK(
            Insert(typed_array->GetView(i), original_key->GetView(i), num_arrays_, i));
      }
    } else {
      if (semi) {
        for (int i = 0; i < typed_array->length(); i++) {
          if (original_key->IsNull(i)) {
            RETURN_NOT_OK(InsertNull(num_arrays_, i));
          } else {
            RETURN_NOT_OK(InsertSkipDup(typed_array->GetView(i), original_key->GetView(i),
                                        num_arrays_, i));
          }
        }
      } else {
        for (int i = 0; i < typed_array->length(); i++) {
          if (original_key->IsNull(i)) {
            RETURN_NOT_OK(InsertNull(num_arrays_, i));
          } else {
            RETURN_NOT_OK(Insert(typed_array->GetView(i), original_key->GetView(i),
                                 num_arrays_, i));
          }
        }
      }
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  arrow::Status AppendKeyColumn(std::shared_ptr<arrow::Array> in,
                                std::shared_ptr<StringArray> original_key,
                                bool semi = false) {
    // This Key should be Hash Key
    auto typed_array = std::make_shared<ArrayType>(in);
    if (original_key->null_count() == 0) {
      for (int i = 0; i < original_key->length(); i++) {
        auto str = original_key->GetView(i);
        RETURN_NOT_OK(
            Insert(typed_array->GetView(i), str.data(), str.size(), num_arrays_, i));
      }

    } else {
      if (semi) {
        for (int i = 0; i < typed_array->length(); i++) {
          if (original_key->IsNull(i)) {
            RETURN_NOT_OK(InsertNull(num_arrays_, i));
          } else {
            auto str = original_key->GetView(i);
            RETURN_NOT_OK(InsertSkipDup(typed_array->GetView(i), str.data(), str.size(),
                                        num_arrays_, i));
          }
        }
      } else {
        for (int i = 0; i < typed_array->length(); i++) {
          if (original_key->IsNull(i)) {
            RETURN_NOT_OK(InsertNull(num_arrays_, i));
          } else {
            auto str = original_key->GetView(i);
            RETURN_NOT_OK(
                Insert(typed_array->GetView(i), str.data(), str.size(), num_arrays_,
                i));
          }
        }
      }
    }

    num_arrays_++;
    // DumpHashMap();
    return arrow::Status::OK();
  }

  template <typename CType,
            typename std::enable_if_t<is_number_or_decimal_type<CType>::value>* = nullptr>
  int Get(int32_t v, CType payload) {
    bool hasMatch = false;
    arrayid_list_.clear();
    auto range = hash_table_new_.equal_range(std::to_string(payload));
    for (auto i = range.first; i != range.second; ++i) {
      hasMatch = true;
      arrayid_list_.push_back(i->second);
    }
    if (!hasMatch) return -1;
    return 0;
  }

  int Get(int32_t v, std::string payload) {
    bool hasMatch = false;
    arrayid_list_.clear();
    auto range = hash_table_new_.equal_range(payload);
    for (auto i = range.first; i != range.second; ++i) {
      hasMatch = true;
      arrayid_list_.push_back(i->second);
    }
    if (!hasMatch) return -1;
    return 0;
  }

  int Get(int32_t v, std::shared_ptr<UnsafeRow> payload) {
    bool hasMatch = false;
    arrayid_list_.clear();
    auto range = hash_table_new_.equal_range(std::string(payload->data, payload->cursor));
    for (auto i = range.first; i != range.second; ++i) {
      hasMatch = true;
      arrayid_list_.push_back(i->second);
    }
    if (!hasMatch) return -1;
    return 0;
  }

  template <typename CType,
            typename std::enable_if_t<is_number_or_decimal_type<CType>::value>* = nullptr>
  int IfExists(int32_t v, CType payload) {
    if (hash_table_new_.find(std::to_string(payload)) == hash_table_new_.end()) {
      return -1;
    }
    return 0;
  }

  int IfExists(int32_t v, std::string payload) {
    return hash_table_new_.find(payload) == hash_table_new_.end() ? -1 : 0;
  }

  int IfExists(int32_t v, std::shared_ptr<UnsafeRow> payload) {
    return hash_table_new_.find(std::string(payload->data, payload->cursor)) ==
                   hash_table_new_.end()
               ? -1
               : 0;
  }

  template <typename CType,
            typename std::enable_if_t<is_number_or_decimal_type<CType>::value>* = nullptr>
  int Get(CType payload) {
    if (sizeof(payload) <= 8) {
      if (has_cached_ && *(CType*)recent_cached_key_ == payload) {
        return recent_cached_key_probe_res_;
      }
      has_cached_ = true;
      *(CType*)recent_cached_key_ = payload;
    }
    int32_t v = hash32(payload, true);
    bool hasMatch = false;
    arrayid_list_.clear();
    auto range = hash_table_new_.equal_range(std::to_string(payload));
    for (auto i = range.first; i != range.second; ++i) {
      hasMatch = true;
      arrayid_list_.push_back(i->second);
    }

    if (!hasMatch) {
      arrayid_list_.clear();
      recent_cached_key_probe_res_ = -1;
      return -1;
    }
    recent_cached_key_probe_res_ = 0;
    return 0;
  }

  int Get(std::string payload) {
    bool hasMatch = false;
    arrayid_list_.clear();
    auto range = hash_table_new_.equal_range(payload);
    for (auto i = range.first; i != range.second; ++i) {
      hasMatch = true;
      arrayid_list_.push_back(i->second);
    }
    if (!hasMatch) return -1;
    return 0;
  }

  template <typename CType,
            typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
  int IfExists(CType payload) {
    int32_t v = hash32(payload, true);
    return hash_table_new_.find(std::to_string(payload)) == hash_table_new_.end() ? -1
                                                                                  : 0;
  }

  int IfExists(std::string payload) {
    int32_t v = hash32(payload, true);
    return hash_table_new_.find(payload) == hash_table_new_.end() ? -1 : 0;
  }

  int GetNull() {
    // since vanilla spark doesn't support to join with two nulls
    // we should always return -1 here;
    // return null_index_set_ ? 0 : HASH_NEW_KEY;
    return HASH_NEW_KEY;
  }

  // This method is specificlly used by Anti Join,
  // because whether to join null should be considered.
  int GetRealNull() { return null_index_set_ ? 0 : HASH_NEW_KEY; }

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

  arrow::Status UnsafeGetHashTableObject(int64_t* addrs, int* sizes) {
    if (hash_table_ == nullptr) {
      return arrow::Status::Invalid("UnsafeGetHashTableObject hash_table is null");
    }
    // dump(hash_table_);
    addrs[0] = (int64_t)hash_table_;
    sizes[0] = (int)sizeof(unsafeHashMap);

    addrs[1] = (int64_t)(hash_table_->keyArray);
    sizes[1] = (int)(hash_table_->arrayCapacity * hash_table_->bytesInKeyArray);

    addrs[2] = (int64_t)(hash_table_->bytesMap);
    sizes[2] = (int)(hash_table_->cursor);

    addrs[3] = (int64_t)(&null_index_set_);
    sizes[3] = (int)sizeof(bool);
    return arrow::Status::OK();
  }

  arrow::Status UnsafeSetHashTableObject(int len, int64_t* addrs, int* sizes) {
    assert(len == 4);
    hash_table_ = (unsafeHashMap*)addrs[0];
    hash_table_->cursor = sizes[2];
    hash_table_->keyArray = (char*)addrs[1];
    hash_table_->bytesMap = (char*)addrs[2];
    null_index_set_ = *(bool*)addrs[3];
    unsafe_set = true;
    // dump(hash_table_);
    return arrow::Status::OK();
  }

  arrow::Status DumpHashMap() {
    dump(hash_table_);
    return arrow::Status::OK();
  }

  virtual std::vector<ArrayItemIndex> GetItemListByIndex(int i) { return arrayid_list_; }

  void TESTGrowAndRehashKeyArray() { growAndRehashKeyArray(hash_table_); }

  int GetHashTableSize() {
    assert(hash_table_ != nullptr);
    return hash_table_->numKeys;
  }

 protected:
  bool unsafe_set = false;
  arrow::compute::ExecContext* ctx_;
  uint64_t num_arrays_ = 0;
  std::vector<std::shared_ptr<HashRelationColumn>> hash_relation_column_list_;
  unsafeHashMap* hash_table_ = nullptr;
  std::multimap<std::string, ArrayItemIndex> hash_table_new_;
  bool isBHJ_ = false;
  using ArrayType = sparkcolumnarplugin::precompile::Int32Array;
  bool null_index_set_ = false;
  std::vector<ArrayItemIndex> null_index_list_;
  std::vector<ArrayItemIndex> arrayid_list_;
  int key_size_;
  char recent_cached_key_[8] = {0};
  bool has_cached_ = false;
  int recent_cached_key_probe_res_ = -1;

  arrow::Status Insert(int32_t v, std::shared_ptr<UnsafeRow> payload, uint32_t array_id,
                       uint32_t id) {
    hash_table_new_.emplace(std::make_pair(std::string(payload->data, payload->cursor),
                                           ArrayItemIndex(array_id, id)));
    return arrow::Status::OK();
  }

  template <typename CType>
  arrow::Status Insert(int32_t v, CType payload, uint32_t array_id, uint32_t id) {
    hash_table_new_.emplace(
        std::make_pair(std::to_string(payload), ArrayItemIndex(array_id, id)));
    return arrow::Status::OK();
  }

  arrow::Status Insert(int32_t v, const char* payload, size_t payload_len,
                       uint32_t array_id, uint32_t id) {
    hash_table_new_.emplace(std::make_pair(std::string(payload, payload_len),
                                           ArrayItemIndex(array_id, id)));
    return arrow::Status::OK();
  }

  arrow::Status InsertSkipDup(int32_t v, std::shared_ptr<UnsafeRow> payload,
                              uint32_t array_id, uint32_t id) {
    if (hash_table_new_.find(std::string(payload->data, payload->cursor)) ==
        hash_table_new_.end()) {
      hash_table_new_.emplace(std::string(payload->data, payload->cursor),
                              ArrayItemIndex(array_id, id));
    }
    return arrow::Status::OK();
  }

  template <typename CType>
  arrow::Status InsertSkipDup(int32_t v, CType payload, uint32_t array_id, uint32_t id) {
    if (hash_table_new_.find(std::to_string(payload)) == hash_table_new_.end()) {
      hash_table_new_.emplace(std::to_string(payload), ArrayItemIndex(array_id, id));
    }
    return arrow::Status::OK();
  }

  arrow::Status InsertSkipDup(int32_t v, const char* payload, size_t payload_len,
                              uint32_t array_id, uint32_t id) {
    if (hash_table_new_.find(std::string(payload, payload_len)) ==
        hash_table_new_.end()) {
      hash_table_new_.emplace(std::string(payload, payload_len),
                              ArrayItemIndex(array_id, id));
    }
    return arrow::Status::OK();
  }

  arrow::Status InsertNull(uint32_t array_id, uint32_t id) {
    if (!null_index_set_) {
      null_index_set_ = true;
      null_index_list_ = {ArrayItemIndex(array_id, id)};
    } else {
      null_index_list_.emplace_back(array_id, id);
    }
    return arrow::Status::OK();
  }
};

template <typename T, typename Enable = void>
class TypedHashRelation {};

arrow::Status MakeHashRelationColumn(uint32_t data_type_id,
                                     std::shared_ptr<HashRelationColumn>* out);

arrow::Status MakeHashRelation(
    uint32_t key_type_id, arrow::compute::ExecContext* ctx,
    const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
    std::shared_ptr<HashRelation>* out);
