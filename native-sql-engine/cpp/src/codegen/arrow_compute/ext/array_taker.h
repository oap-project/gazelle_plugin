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

#include <cstdint>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/array.h"
#include "precompile/type_traits.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using namespace sparkcolumnarplugin::precompile;

class TakerBase {
 public:
  virtual ~TakerBase() {}

  virtual arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    return arrow::Status::NotImplemented("TakerBase AddArray is abstract.");
  }

  virtual arrow::Status PopArray() {
    return arrow::Status::NotImplemented("TakerBase PopArray is abstract.");
  }

  virtual arrow::Status ClearArrays() {
    return arrow::Status::NotImplemented("TakerBase ClearArrays is abstract.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    return arrow::Status::NotImplemented("TakerBase Finish is abstract.");
  }

  virtual arrow::Status TakeFromIndices(ArrayItemIndexS* indices_begin, int64_t length,
                                        arrow::ArrayData* out_arr) {
    return arrow::Status::NotImplemented("TakerBase TakeFromIndices is abstract.");
  }
};

template <typename DataType, typename CType, typename Enable = void>
class ArrayTaker {};

template <typename T>
using is_number_or_date = std::integral_constant<bool, arrow::is_number_type<T>::value ||
                                                           arrow::is_date_type<T>::value>;
template <typename DataType, typename R = void>
using enable_if_number_or_date = std::enable_if_t<is_number_or_date<DataType>::value, R>;

template <typename DataType, typename CType>
class ArrayTaker<DataType, CType, enable_if_number_or_date<DataType>> : public TakerBase {
 public:
  ArrayTaker(arrow::compute::ExecContext* ctx, arrow::MemoryPool* pool)
      : ctx_(ctx), pool_(pool) {}

  ~ArrayTaker() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.push_back(typed_arr_);
    if (!has_null_ && typed_arr_->null_count() > 0) has_null_ = true;
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status ClearArrays() override {
    cached_arr_.clear();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status TakeFromIndices(ArrayItemIndexS* indices_begin, int64_t length,
                                arrow::ArrayData* out) {
    arrow::ArrayData out_data;
    out_data.length = length;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<DataType>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(size_ * length, pool_));
    if (has_null_) {
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(length, pool_));
    }
    auto array_data = out_data.GetMutableValues<CType>(1);

    int64_t position = 0;
    int64_t null_count = 0;
    if (!has_null_) {
      out_data.null_count = 0;
      while (position < length) {
        auto item = indices_begin + position;
        auto val = cached_arr_[item->array_id]->GetView(item->id);
        array_data[position] = val;
        position++;
      }
    } else {
      auto out_is_valid = out_data.buffers[0]->mutable_data();
      while (position < length) {
        auto item = indices_begin + position;
        if (!cached_arr_[item->array_id]->IsNull(item->id)) {
          arrow::BitUtil::SetBitTo(out_is_valid, position, true);
          array_data[position] = cached_arr_[item->array_id]->GetView(item->id);
        } else {
          null_count++;
          arrow::BitUtil::SetBitTo(out_is_valid, position, false);
          array_data[position] = CType{};
        }
        position++;
      }
      out_data.null_count = null_count;
    }
    *out = std::move(out_data);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::ExecContext* ctx_;
  bool has_null_ = false;
  int size_ = sizeof(CType);
  arrow::MemoryPool* pool_;
};

template <typename DataType, typename CType>
class ArrayTaker<DataType, CType, arrow::enable_if_boolean<DataType>> : public TakerBase {
 public:
  ArrayTaker(arrow::compute::ExecContext* ctx, arrow::MemoryPool* pool)
      : ctx_(ctx), pool_(pool) {}

  ~ArrayTaker() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.push_back(typed_arr_);
    if (!has_null_ && typed_arr_->null_count() > 0) has_null_ = true;
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status ClearArrays() override {
    cached_arr_.clear();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status TakeFromIndices(ArrayItemIndexS* indices_begin, int64_t length,
                                arrow::ArrayData* out) {
    arrow::ArrayData out_data;
    out_data.length = length;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<DataType>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBitmap(length, pool_));
    if (has_null_) {
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(length, pool_));
    }
    auto array_data = out_data.buffers[1]->mutable_data();

    int64_t position = 0;
    int64_t null_count = 0;
    if (!has_null_) {
      out_data.null_count = 0;
      while (position < length) {
        auto item = indices_begin + position;
        bool val = cached_arr_[item->array_id]->GetView(item->id);
        arrow::BitUtil::SetBitTo(array_data, position, val);
        position++;
      }
    } else {
      auto out_is_valid = out_data.buffers[0]->mutable_data();
      while (position < length) {
        auto item = indices_begin + position;
        if (!cached_arr_[item->array_id]->IsNull(item->id)) {
          arrow::BitUtil::SetBitTo(array_data, position,
                                   cached_arr_[item->array_id]->GetView(item->id));
          arrow::BitUtil::SetBitTo(out_is_valid, position, true);
        } else {
          null_count++;
          arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        }
        position++;
      }
      out_data.null_count = null_count;
    }
    *out = std::move(out_data);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::ExecContext* ctx_;
  bool has_null_ = false;
  int size_ = sizeof(CType);
  arrow::MemoryPool* pool_;
};

template <typename DataType, typename CType>
class ArrayTaker<DataType, CType, enable_if_decimal<DataType>> : public TakerBase {
 public:
  ArrayTaker(arrow::compute::ExecContext* ctx, arrow::MemoryPool* pool,
             std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx), pool_(pool) {
    auto typed_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
    precision_ = typed_type->precision();
    scale_ = typed_type->scale();
  }

  ~ArrayTaker() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::make_shared<Decimal128Array>(arr);
    cached_arr_.push_back(typed_arr_);
    if (!has_null_ && typed_arr_->null_count() > 0) has_null_ = true;
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status ClearArrays() override {
    cached_arr_.clear();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status TakeFromIndices(ArrayItemIndexS* indices_begin, int64_t length,
                                arrow::ArrayData* out) {
    arrow::ArrayData out_data;
    out_data.length = length;
    out_data.buffers.resize(2);
    out_data.type = arrow::decimal128(precision_, scale_);
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(size_ * length, pool_));
    if (has_null_) {
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(length, pool_));
    }
    auto array_data = out_data.GetMutableValues<CType>(1);

    int64_t position = 0;
    int64_t null_count = 0;
    if (!has_null_) {
      out_data.null_count = 0;
      while (position < length) {
        auto item = indices_begin + position;
        array_data[position] = cached_arr_[item->array_id]->GetView(item->id);
        position++;
      }
    } else {
      auto out_is_valid = out_data.buffers[0]->mutable_data();
      while (position < length) {
        auto item = indices_begin + position;
        if (!cached_arr_[item->array_id]->IsNull(item->id)) {
          arrow::BitUtil::SetBitTo(out_is_valid, position, true);
          array_data[position] = cached_arr_[item->array_id]->GetView(item->id);
        } else {
          null_count++;
          arrow::BitUtil::SetBitTo(out_is_valid, position, false);
          array_data[position] = CType{};
        }
        position++;
      }
      out_data.null_count = null_count;
    }
    *out = std::move(out_data);
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<Decimal128Array>> cached_arr_;
  arrow::compute::ExecContext* ctx_;
  int precision_;
  int scale_;
  bool has_null_ = false;
  int size_ = sizeof(CType);
  arrow::MemoryPool* pool_;
};

template <typename DataType, typename CType>
class ArrayTaker<DataType, CType, arrow::enable_if_same<DataType, arrow::StringType>> : public TakerBase {
 public:
  ArrayTaker(arrow::compute::ExecContext* ctx, arrow::MemoryPool* pool)
      : ctx_(ctx), pool_(pool) {}

  ~ArrayTaker() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.push_back(typed_arr_);
    if (!has_null_ && typed_arr_->null_count() > 0) has_null_ = true;
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status ClearArrays() override {
    cached_arr_.clear();
    has_null_ = false;
    return arrow::Status::OK();
  }

  arrow::Status TakeFromIndices(ArrayItemIndexS* indices_begin, int64_t length,
                                arrow::ArrayData* out) {
    arrow::ArrayData out_data;
    out_data.length = length;
    out_data.buffers.resize(3);
    out_data.type = arrow::TypeTraits<DataType>::type_singleton();
    // value_offsets
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(4 * (length + 1), pool_));
    if (has_null_) {
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(length, pool_));
    }
    auto array_offset = out_data.GetMutableValues<int32_t>(1);

    int64_t position = 0;
    std::string whole_str = "";
    int32_t total_bytes = 0;
    if (!has_null_) {
      out_data.null_count = 0;
      while (position < length) {
        auto item = indices_begin + position;
        std::string str = cached_arr_[item->array_id]->GetString(item->id);
        whole_str += str;
        array_offset[position] = total_bytes;
        total_bytes += str.size();
        position++;
      }
      array_offset[position] = total_bytes;
    } else {
      int64_t null_count = 0;
      auto out_is_valid = out_data.buffers[0]->mutable_data();
      int64_t last_valid_position;
      while (position < length) {
        auto item = indices_begin + position;
        if (!cached_arr_[item->array_id]->IsNull(item->id)) {
          arrow::BitUtil::SetBitTo(out_is_valid, position, true);
          std::string str = cached_arr_[item->array_id]->GetString(item->id);
          whole_str += str;
          array_offset[position] = total_bytes;
          total_bytes += str.size();
          last_valid_position = position;
        } else {
          null_count++;
          array_offset[position] = total_bytes;
          arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        }
        position++;
      }
      out_data.null_count = null_count;
      // Set the next offset for the last valid data, so its length can be correctly decided.
      array_offset[last_valid_position + 1] = total_bytes;
    }
    
    // value_data
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[2], AllocateBuffer(total_bytes, pool_));
    uint8_t* array_data = out_data.buffers[2]->mutable_data();
    const char* chr = whole_str.c_str();
    std::memcpy(array_data, chr, total_bytes);

    *out = std::move(out_data);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::ExecContext* ctx_;
  bool has_null_ = false;
  int size_ = sizeof(CType);
  arrow::MemoryPool* pool_;
};

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)
static arrow::Status MakeArrayTaker(arrow::compute::ExecContext* ctx,
                                    std::shared_ptr<arrow::DataType> type,
                                    std::shared_ptr<TakerBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id: {                                                                \
    using CType = typename arrow::TypeTraits<InType>::CType;                             \
    auto app_ptr = std::make_shared<ArrayTaker<InType, CType>>(ctx, ctx->memory_pool()); \
    *out = std::dynamic_pointer_cast<TakerBase>(app_ptr);                                \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::Decimal128Type::type_id: {
      auto app_ptr =
          std::make_shared<ArrayTaker<arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, ctx->memory_pool(), type);
      *out = std::dynamic_pointer_cast<TakerBase>(app_ptr);
    } break;
    case arrow::StringType::type_id: {
      auto app_ptr =
          std::make_shared<ArrayTaker<arrow::StringType, std::string>>(
              ctx, ctx->memory_pool());
      *out = std::dynamic_pointer_cast<TakerBase>(app_ptr);
    } break;
    default: {
      return arrow::Status::NotImplemented("MakeArrayTaker type not supported, type is ",
                                           type->ToString());
    } break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
