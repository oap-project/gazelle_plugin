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

#include "precompile/type_traits.h"

using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;

class RelationColumn {
 public:
  virtual bool IsNull(int array_id, int id) = 0;
  virtual bool IsEqualTo(int x_array_id, int x_id, int y_array_id, int y_id) = 0;
  virtual arrow::Status AppendColumn(std::shared_ptr<arrow::Array> in) {
    return arrow::Status::NotImplemented("RelationColumn AppendColumn is abstract.");
  };
  virtual arrow::Status GetArrayVector(std::vector<std::shared_ptr<arrow::Array>>* out) {
    return arrow::Status::NotImplemented("RelationColumn GetArrayVector is abstract.");
  }
};

template <typename T, typename Enable = void>
class TypedRelationColumn {};

template <typename DataType>
class TypedRelationColumn<DataType, enable_if_number<DataType>> : public RelationColumn {
 public:
  using T = typename TypeTraits<DataType>::CType;
  TypedRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  bool IsEqualTo(int x_array_id, int x_id, int y_array_id, int y_id) {
    auto is_null_x = IsNull(x_array_id, x_id);
    auto is_null_y = IsNull(y_array_id, y_id);
    if (is_null_x && is_null_y) return true;
    if (is_null_x || is_null_y) return false;
    return GetValue(x_array_id, x_id) == GetValue(y_array_id, y_id);
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
class TypedRelationColumn<DataType, enable_if_string_like<DataType>>
    : public RelationColumn {
 public:
  TypedRelationColumn() {}
  bool IsNull(int array_id, int id) override {
    return array_vector_[array_id]->IsNull(id);
  }
  bool IsEqualTo(int x_array_id, int x_id, int y_array_id, int y_id) {
    auto is_null_x = IsNull(x_array_id, x_id);
    auto is_null_y = IsNull(y_array_id, y_id);
    if (is_null_x && is_null_y) return true;
    if (is_null_x || is_null_y) return false;
    return GetValue(x_array_id, x_id) == GetValue(y_array_id, y_id);
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

arrow::Status MakeRelationColumn(uint32_t data_type_id,
                                 std::shared_ptr<RelationColumn>* out);