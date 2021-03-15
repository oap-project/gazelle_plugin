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
#include <arrow/type_fwd.h>

#include <cstdint>
#include <iostream>
#include <vector>

#include "precompile/type_traits.h"
#include "third_party/row_wise_memory/unsafe_row.h"

namespace sparkcolumnarplugin {
namespace precompile {
class UnsafeArray {
 public:
  UnsafeArray() {}
  virtual ~UnsafeArray() {}

  virtual arrow::Status Append(int i, std::shared_ptr<UnsafeRow>* unsafe_row) {
    return arrow::Status::NotImplemented("UnsafeArray Append is abstract.");
  }
};

template <typename DataType, typename Enable = void>
class TypedUnsafeArray {};

template <typename DataType>
class TypedUnsafeArray<DataType, enable_if_number_or_decimal<DataType>>
    : public UnsafeArray {
 public:
  TypedUnsafeArray(int i, const std::shared_ptr<arrow::Array>& in) : idx_(i) {
    typed_array_ = std::make_shared<ArrayType>(in);
    if (typed_array_->null_count() == 0) {
      skip_null_check_ = true;
    } else {
      skip_null_check_ = false;
    }
  }
  ~TypedUnsafeArray() {}
  arrow::Status Append(int i, std::shared_ptr<UnsafeRow>* unsafe_row) override {
    if (!skip_null_check_ && typed_array_->IsNull(i)) {
      setNullAt((*unsafe_row).get(), idx_);
    } else {
      auto v = typed_array_->GetView(i);
      appendToUnsafeRow((*unsafe_row).get(), idx_, v);
    }
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  using CType = typename TypeTraits<DataType>::CType;
  std::shared_ptr<ArrayType> typed_array_;
  int idx_;
  bool skip_null_check_ = false;
};

template <typename DataType>
class TypedUnsafeArray<DataType, enable_if_string_like<DataType>> : public UnsafeArray {
 public:
  TypedUnsafeArray(int i, const std::shared_ptr<arrow::Array>& in) : idx_(i) {
    typed_array_ = std::make_shared<ArrayType>(in);
    if (typed_array_->null_count() == 0) {
      skip_null_check_ = true;
    } else {
      skip_null_check_ = false;
    }
  }
  ~TypedUnsafeArray() {}
  arrow::Status Append(int i, std::shared_ptr<UnsafeRow>* unsafe_row) override {
    if (!skip_null_check_ && typed_array_->IsNull(i)) {
      setNullAt((*unsafe_row).get(), idx_);
    } else {
      auto v = typed_array_->GetString(i);
      appendToUnsafeRow((*unsafe_row).get(), idx_, v);
    }
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  using CType = typename TypeTraits<DataType>::CType;
  std::shared_ptr<ArrayType> typed_array_;
  int idx_;
  bool skip_null_check_ = false;
};

arrow::Status MakeUnsafeArray(std::shared_ptr<arrow::DataType> type, int idx,
                              const std::shared_ptr<arrow::Array>& in,
                              std::shared_ptr<UnsafeArray>* out);
}  // namespace precompile
}  // namespace sparkcolumnarplugin