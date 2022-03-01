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

#include "codegen/arrow_compute/ext/actions_impl.h"

#include <arrow/builder.h>
#include <arrow/compute/kernel.h>
#include <arrow/pretty_print.h>
#include <arrow/scalar.h>
#include <arrow/type_traits.h>

#include "precompile/gandiva.h"
#include "precompile/type_traits.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};

arrow::Status ActionBase::Submit(ArrayList in, int max_group_id,
                                 std::function<arrow::Status(int)>* on_valid,
                                 std::function<arrow::Status()>* on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(
    std::vector<std::shared_ptr<arrow::Array>> in,
    std::function<arrow::Status(uint64_t, uint64_t)>* on_valid,
    std::function<arrow::Status()>* on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(const std::shared_ptr<arrow::Array>& in,
                                 std::stringstream* ss,
                                 std::function<arrow::Status(int)>* out) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(const std::shared_ptr<arrow::Array>& in,
                                 std::function<arrow::Status(uint32_t)>* on_valid,
                                 std::function<arrow::Status()>* on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::EvaluateCountLiteral(const int& len) {
  return arrow::Status::NotImplemented("ActionBase EvaluateCountLiteral is abstract.");
}

arrow::Status ActionBase::Evaluate(int dest_group_id) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::Evaluate(const arrow::ArrayVector& in) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::Evaluate(int dest_group_id, void* data) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::Evaluate(int dest_group_id, void* data1, void* data2) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::Evaluate(int dest_group_id, void* data1, void* data2,
                                   void* data3) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::EvaluateNull(int dest_group_id) {
  return arrow::Status::NotImplemented("ActionBase Evaluate is abstract.");
}

arrow::Status ActionBase::Finish(ArrayList* out) {
  return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
}

arrow::Status ActionBase::Finish(uint64_t offset, uint64_t length, ArrayList* out) {
  return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
}

arrow::Status ActionBase::FinishAndReset(ArrayList* out) {
  return arrow::Status::NotImplemented("ActionBase FinishAndReset is abstract.");
}

uint64_t ActionBase::GetResultLength() { return 0; }

//////////////// UniqueAction ///////////////
template <typename DataType, typename CType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct UniqueAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      null_flag_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    row_id_ = 0;
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id_);
      if (cache_validity_[dest_group_id] == false) {
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_[dest_group_id] = (CType)in_->GetView(row_id_);
        } else {
          cache_validity_[dest_group_id] = true;
          null_flag_[dest_group_id] = true;
        }
      }
      row_id_++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id_++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    null_flag_.resize(max_group_id, false);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (cache_validity_[dest_group_id] == false) {
      cache_validity_[dest_group_id] = true;
      cache_.emplace(cache_.begin() + dest_group_id, *(CType*)data);
    }
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (cache_validity_[dest_group_id] == false) {
      cache_validity_[dest_group_id] = true;
      null_flag_[dest_group_id] = true;
      CType num;
      cache_.emplace(cache_.begin() + dest_group_id, num);
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto length = GetResultLength();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        if (!null_flag_[i]) {
          builder_->Append(cache_[i]);
        } else {
          builder_->AppendNull();
        }
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    // appendValues to builder_
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        if (!null_flag_[offset + i]) {
          builder_->Append(cache_[offset + i]);
        } else {
          builder_->AppendNull();
        }
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  int row_id_ = 0;
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int in_null_count_ = 0;
  // output
  std::unique_ptr<BuilderType> builder_;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::vector<bool> null_flag_;
  uint64_t length_ = 0;
};

//////////////// CountAction ///////////////
template <typename DataType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::ExecContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct CountAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::int64(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_.size() <= max_group_id) {
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_.size();
    }

    in_list_ = in_list;
    row_id = 0;
    bool has_null = false;
    for (int i = 0; i < in_list.size(); i++) {
      if (in_list_[i]->null_count()) {
        has_null = true;
        break;
      }
    }
    // prepare evaluate lambda
    if (has_null) {
      *on_valid = [this](int dest_group_id) {
        bool is_null = false;
        for (int i = 0; i < in_list_.size(); i++) {
          if (in_list_[i]->IsNull(row_id)) {
            is_null = true;
            break;
          }
        }
        if (!is_null) {
          cache_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_.size() * 2;
    }
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_.empty()) {
      cache_.resize(1, 0);
      length_ = 1;
    }
    int length = in[0]->length();
    int count_non_null = 0;
    if (in.size() == 1) {
      count_non_null = length - in[0]->null_count();
    } else {
      int count_null = 0;
      for (int id = 0; id < length; id++) {
        for (int colId = 0; colId < in.size(); colId++) {
          if (in[colId]->IsNull(id)) {
            count_null++;
            break;
          }
        }
      }
      count_non_null = length - count_null;
    }
    cache_[0] += count_non_null;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;

    cache_[dest_group_id] += 1;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto length = GetResultLength();
    for (uint64_t i = 0; i < length; i++) {
      builder_->Append(cache_[i]);
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      builder_->Append(cache_[offset + i]);
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<arrow::Int64Type>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<arrow::Int64Type>::BuilderType;
  using ScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  // input
  arrow::compute::ExecContext* ctx_;
  ArrayList in_list_;
  int32_t row_id;
  // result
  using CType = typename arrow::TypeTraits<arrow::Int64Type>::CType;
  std::vector<CType> cache_;
  std::unique_ptr<ResBuilderType> builder_;
  uint64_t length_ = 0;
};

//////////////// CountLiteralAction ///////////////
template <typename DataType>
class CountLiteralAction : public ActionBase {
 public:
  CountLiteralAction(arrow::compute::ExecContext* ctx, int arg) : ctx_(ctx), arg_(arg) {
#ifdef DEBUG
    std::cout << "Construct CountLiteralAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~CountLiteralAction() {
#ifdef DEBUG
    std::cout << "Destruct CountLiteralAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_.size() <= max_group_id) {
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_.size();
    }

    // prepare evaluate lambda
    *on_valid = [this](int dest_group_id) {
      cache_[dest_group_id] += 1;
      return arrow::Status::OK();
    };

    *on_null = [this]() { return arrow::Status::OK(); };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_.size() * 2;
    }
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status EvaluateCountLiteral(const int& len) {
    if (cache_.empty()) {
      cache_.resize(1, 0);
      length_ = 1;
    }
    cache_[0] += len;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    return arrow::Status::NotImplemented(
        "CountLiteralAction Non-Groupby Evaluate is unsupported.");
  }

  arrow::Status Evaluate(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_[dest_group_id] += 1;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto length = GetResultLength();
    for (uint64_t i = 0; i < length; i++) {
      builder_->Append(cache_[i]);
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      builder_->Append(cache_[offset + i]);
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  int arg_;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::unique_ptr<ResBuilderType> builder_;
  uint64_t length_ = 0;
};

//////////////// MinAction ///////////////
template <typename DataType, typename CType, typename Enable = void>
class MinAction {};

template <typename DataType, typename CType>
class MinAction<DataType, CType, precompile::enable_if_number<DataType>>
    : public ActionBase {
 public:
  MinAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            bool NaN_check = false)
      : ctx_(ctx), NaN_check_(NaN_check) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }
    GetFunction<CType>(in_list, max_group_id, on_valid, on_null);
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    GetMinResult<CType>(in);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    GetMinResultWithGroupBy<CType>(dest_group_id, data);
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;

  // input
  arrow::compute::ExecContext* ctx_;
  bool NaN_check_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;

  /* Returns true if left is less than right. */
  template <typename CTYPE>
  bool LessNaNSafe(CTYPE left, CTYPE right) {
    bool left_is_nan = std::isnan(left);
    bool right_is_nan = std::isnan(right);
    if (left_is_nan && right_is_nan) {
      return false;
    } else if (left_is_nan) {
      return false;
    } else if (right_is_nan) {
      return true;
    } else {
      return left < right;
    }
  }

  template <typename CTYPE>
  auto GetFunction(const ArrayList& in_list, int max_group_id,
                   std::function<arrow::Status(int)>* on_valid,
                   std::function<arrow::Status()>* on_null) ->
      typename std::enable_if_t<std::is_floating_point<CTYPE>::value> {
    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    if (NaN_check_) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
        if (!is_null) {
          auto data = in_->GetView(row_id);
          if (!cache_validity_[dest_group_id]) {
            cache_[dest_group_id] = data;
            cache_validity_[dest_group_id] = true;
          }
          if (LessNaNSafe<CTYPE>(data, cache_[dest_group_id])) {
            cache_[dest_group_id] = data;
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
        if (!is_null) {
          auto data = in_->GetView(row_id);
          if (!cache_validity_[dest_group_id]) {
            cache_[dest_group_id] = data;
            cache_validity_[dest_group_id] = true;
          }
          if (data < cache_[dest_group_id]) {
            cache_[dest_group_id] = data;
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    }

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
  }

  template <typename CTYPE>
  auto GetFunction(const ArrayList& in_list, int max_group_id,
                   std::function<arrow::Status(int)>* on_valid,
                   std::function<arrow::Status()>* on_null) ->
      typename std::enable_if_t<!std::is_floating_point<CTYPE>::value> {
    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetView(row_id);
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data;
          cache_validity_[dest_group_id] = true;
        }
        if (data < cache_[dest_group_id]) {
          cache_[dest_group_id] = data;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
  }

  template <typename CTYPE>
  auto GetMinResult(const arrow::ArrayVector& in) ->
      typename std::enable_if_t<std::is_floating_point<CTYPE>::value> {
    in_ = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = in_->null_count();
    for (int i = 0; i < in_->length(); i++) {
      if (in_null_count_ == 0 || !in_->IsNull(i)) {
        auto val = in_->GetView(i);
        if (!cache_validity_[0]) {
          cache_[0] = val;
          cache_validity_[0] = true;
        }
        if (LessNaNSafe<CTYPE>(val, cache_[0])) {
          cache_[0] = val;
        }
      }
    }
  }

  template <typename CTYPE>
  auto GetMinResult(const arrow::ArrayVector& in) ->
      typename std::enable_if_t<!std::is_floating_point<CTYPE>::value> {
    arrow::Datum minMaxOut;
    arrow::compute::ScalarAggregateOptions option;
    auto maybe_minMaxOut = arrow::compute::MinMax(*in[0].get(), option, ctx_);
    minMaxOut = *std::move(maybe_minMaxOut);
    const arrow::StructScalar& value = minMaxOut.scalar_as<arrow::StructScalar>();

    auto& typed_scalar = static_cast<const ScalarType&>(*value.value[0]);
    if ((in[0]->null_count() != in[0]->length()) && !cache_validity_[0]) {
      cache_validity_[0] = true;
      cache_[0] = typed_scalar.value;
    } else {
      if (cache_[0] > typed_scalar.value) cache_[0] = typed_scalar.value;
    }
  }

  template <typename CTYPE>
  auto GetMinResultWithGroupBy(int dest_group_id, void* data) ->
      typename std::enable_if_t<std::is_floating_point<CTYPE>::value> {
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (NaN_check_) {
      if (LessNaNSafe<CType>(*(CType*)data, cache_[dest_group_id])) {
        cache_[dest_group_id] = *(CType*)data;
      }
    } else {
      if (*(CType*)data < cache_[dest_group_id]) {
        cache_[dest_group_id] = *(CType*)data;
      }
    }
  }

  template <typename CTYPE>
  auto GetMinResultWithGroupBy(int dest_group_id, void* data) ->
      typename std::enable_if_t<!std::is_floating_point<CTYPE>::value> {
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (*(CType*)data < cache_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
    }
  }
};

/// Decimal ///
template <typename DataType, typename CType>
class MinAction<DataType, CType, precompile::enable_if_decimal<DataType>>
    : public ActionBase {
 public:
  MinAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetView(row_id);
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data;
          cache_validity_[dest_group_id] = true;
        }
        if (data < cache_[dest_group_id]) {
          cache_[dest_group_id] = data;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = typed_in->null_count();
    for (int i = 0; i < typed_in->length(); i++) {
      if (in_null_count_ > 0 && typed_in->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp = typed_in->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp));
      }
    }

    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (*(CType*)data < cache_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
    }
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    std::shared_ptr<arrow::Array> arr_out;
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;

  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;
};

/// String ///
template <typename DataType, typename CType>
class MinAction<DataType, CType, precompile::enable_if_string_like<DataType>>
    : public ActionBase {
 public:
  MinAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, "");
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetString(row_id);
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data;
          cache_validity_[dest_group_id] = true;
        }
        if (data < cache_[dest_group_id]) {
          cache_[dest_group_id] = data;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, "");
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, "");
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    in_ = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = in_->null_count();
    for (int64_t row_id = 0; row_id < in_->length(); row_id++) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetString(row_id);
        if (!cache_validity_[0]) {
          cache_[0] = data;
          cache_validity_[0] = true;
        }
        if (data < cache_[0]) {
          cache_[0] = data;
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (*(CType*)data < cache_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
    }
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    std::shared_ptr<arrow::Array> arr_out;
    for (int64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;

  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;
};

//////////////// MaxAction ///////////////
template <typename DataType, typename CType, typename Enable = void>
class MaxAction {};

template <typename DataType, typename CType>
class MaxAction<DataType, CType, precompile::enable_if_number<DataType>>
    : public ActionBase {
 public:
  MaxAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            bool NaN_check = false)
      : ctx_(ctx), NaN_check_(NaN_check) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }
    GetFunction<CType>(in_list, max_group_id, on_valid, on_null);
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    GetMaxResult<CType>(in);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    GetMaxResultWithGroupBy<CType>(dest_group_id, data);
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  bool NaN_check_;
  std::shared_ptr<ArrayType> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;

  template <typename CTYPE>
  auto GetFunction(const ArrayList& in_list, int max_group_id,
                   std::function<arrow::Status(int)>* on_valid,
                   std::function<arrow::Status()>* on_null) ->
      typename std::enable_if_t<std::is_floating_point<CTYPE>::value> {
    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    if (NaN_check_) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
        if (!is_null) {
          auto val = in_->GetView(row_id);
          if (std::isnan(val)) {
            cache_[dest_group_id] = val;
            cache_validity_[dest_group_id] = true;
          } else {
            if (!cache_validity_[dest_group_id]) {
              cache_[dest_group_id] = val;
              cache_validity_[dest_group_id] = true;
            }
            if (val > cache_[dest_group_id]) {
              cache_[dest_group_id] = val;
            }
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
        if (!is_null) {
          auto val = in_->GetView(row_id);
          if (!cache_validity_[dest_group_id]) {
            cache_[dest_group_id] = val;
            cache_validity_[dest_group_id] = true;
          }
          if (val > cache_[dest_group_id]) {
            cache_[dest_group_id] = val;
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    }

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
  }

  template <typename CTYPE>
  auto GetFunction(const ArrayList& in_list, int max_group_id,
                   std::function<arrow::Status(int)>* on_valid,
                   std::function<arrow::Status()>* on_null) ->
      typename std::enable_if_t<!std::is_floating_point<CTYPE>::value> {
    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto val = in_->GetView(row_id);
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = val;
          cache_validity_[dest_group_id] = true;
        }
        if (val > cache_[dest_group_id]) {
          cache_[dest_group_id] = val;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
  }

  template <typename TYPE>
  auto GetMaxResult(const arrow::ArrayVector& in) ->
      typename std::enable_if_t<std::is_floating_point<TYPE>::value> {
    in_ = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = in_->null_count();
    for (int i = 0; i < in_->length(); i++) {
      if (in_null_count_ == 0 || !in_->IsNull(i)) {
        auto val = in_->GetView(i);
        if (NaN_check_ && std::isnan(val)) {
          cache_[0] = val;
          cache_validity_[0] = true;
          break;
        }
        if (!cache_validity_[0]) {
          cache_[0] = val;
          cache_validity_[0] = true;
        }
        if (val > cache_[0]) {
          cache_[0] = val;
        }
      }
    }
  }

  template <typename TYPE>
  auto GetMaxResult(const arrow::ArrayVector& in) ->
      typename std::enable_if_t<!std::is_floating_point<TYPE>::value> {
    arrow::Datum minMaxOut;
    arrow::compute::ScalarAggregateOptions option;
    auto maybe_minMaxOut = arrow::compute::MinMax(*in[0].get(), option, ctx_);
    minMaxOut = *std::move(maybe_minMaxOut);
    const arrow::StructScalar& value = minMaxOut.scalar_as<arrow::StructScalar>();

    auto& typed_scalar = static_cast<const ScalarType&>(*value.value[1]);
    if ((in[0]->null_count() != in[0]->length()) && !cache_validity_[0]) {
      cache_validity_[0] = true;
      cache_[0] = typed_scalar.value;
    } else {
      if (cache_[0] < typed_scalar.value) cache_[0] = typed_scalar.value;
    }
  }

  template <typename CTYPE>
  auto GetMaxResultWithGroupBy(int dest_group_id, void* data) ->
      typename std::enable_if_t<std::is_floating_point<CTYPE>::value> {
    auto val = *(CTYPE*)data;
    if (NaN_check_ && std::isnan(val)) {
      cache_[dest_group_id] = val;
      cache_validity_[dest_group_id] = true;
    } else {
      if (!cache_validity_[dest_group_id]) {
        cache_[dest_group_id] = val;
        cache_validity_[dest_group_id] = true;
      }
      if (val > cache_[dest_group_id]) {
        cache_[dest_group_id] = val;
      }
    }
  }

  template <typename CTYPE>
  auto GetMaxResultWithGroupBy(int dest_group_id, void* data) ->
      typename std::enable_if_t<!std::is_floating_point<CTYPE>::value> {
    auto val = *(CTYPE*)data;
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = val;
      cache_validity_[dest_group_id] = true;
    }
    if (val > cache_[dest_group_id]) {
      cache_[dest_group_id] = val;
    }
  }
};

/// Decimal ///
template <typename DataType, typename CType>
class MaxAction<DataType, CType, precompile::enable_if_decimal<DataType>>
    : public ActionBase {
 public:
  MaxAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      if (!cache_validity_[dest_group_id]) {
        cache_[dest_group_id] = in_->GetView(row_id);
      }
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_validity_[dest_group_id] = true;
        auto data = in_->GetView(row_id);
        if (data > cache_[dest_group_id]) {
          cache_[dest_group_id] = data;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = typed_in->null_count();
    for (int i = 0; i < typed_in->length(); i++) {
      if (in_null_count_ > 0 && typed_in->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp = typed_in->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (*(CType*)data > cache_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
    }
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;
};

/// String ///
template <typename DataType, typename CType>
class MaxAction<DataType, CType, precompile::enable_if_string_like<DataType>>
    : public ActionBase {
 public:
  MaxAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), type, &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, "");
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      if (!cache_validity_[dest_group_id]) {
        cache_[dest_group_id] = in_->GetString(row_id);
      }
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetString(row_id);
        cache_validity_[dest_group_id] = true;
        if (data > cache_[dest_group_id]) {
          cache_[dest_group_id] = data;
        }
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, "");
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, "");
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    in_ = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = in_->null_count();
    for (int64_t row_id = 0; row_id < in_->length(); row_id++) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetString(row_id);
        if (!cache_validity_[0]) {
          cache_[0] = data;
          cache_validity_[0] = true;
        }
        if (data > cache_[0]) {
          cache_[0] = data;
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    if (!cache_validity_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
      cache_validity_[dest_group_id] = true;
    }
    if (*(CType*)data > cache_[dest_group_id]) {
      cache_[dest_group_id] = *(CType*)data;
    }
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    for (int64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
  uint64_t length_ = 0;
};

//////////////// SumAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class SumAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumAction<DataType, CType, ResDataType, ResCType,
                precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  SumAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = in_list[0];
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data_[row_id];
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_[0] += typed_scalar->value;
    // If all values are null, result for sum will be null.
    if (!cache_validity_[0] && (in[0]->length() != in[0]->null_count())) {
      cache_validity_[0] = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_[dest_group_id] += *(CType*)data;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
  uint64_t length_ = 0;
};

/// Decimal ///
template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumAction<DataType, CType, ResDataType, ResCType,
                precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  SumAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;

    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += in_->GetView(row_id);
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = typed_in->null_count();
    for (int i = 0; i < typed_in->length(); i++) {
      if (in_null_count_ > 0 && typed_in->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp = typed_in->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_[dest_group_id] += *(CType*)data;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Array> arr_isempty_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));

    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;

  uint64_t length_ = 0;
};

template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class SumActionPartial {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumActionPartial<DataType, CType, ResDataType, ResCType,
                       precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  SumActionPartial(arrow::compute::ExecContext* ctx,
                   std::shared_ptr<arrow::DataType> type,
                   std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumActionPartial" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~SumActionPartial() {
#ifdef DEBUG
    std::cout << "Destruct SumActionPartial" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = in_list[0];
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data_[row_id];
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_[0] += typed_scalar->value;
    if (!cache_validity_[0] && (in[0]->length() != in[0]->null_count())) {
      cache_validity_[0] = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_[dest_group_id] += *(CType*)data;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
      } else {
        builder_->AppendNull();
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
  uint64_t length_ = 0;
};

/// Decimal ///
template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumActionPartial<DataType, CType, ResDataType, ResCType,
                       precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  SumActionPartial(arrow::compute::ExecContext* ctx,
                   std::shared_ptr<arrow::DataType> type,
                   std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumActionPartial" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    std::unique_ptr<arrow::ArrayBuilder> array_builder_empty;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));

    auto bool_type = std::make_shared<arrow::BooleanType>();
    arrow::MakeBuilder(ctx_->memory_pool(), bool_type, &array_builder_empty);
    builder_isempty_.reset(arrow::internal::checked_cast<
                           arrow::TypeTraits<arrow::BooleanType>::BuilderType*>(
        array_builder_empty.release()));
  }
  ~SumActionPartial() {
#ifdef DEBUG
    std::cout << "Destruct SumActionPartial" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += in_->GetView(row_id);
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = typed_in->null_count();
    for (int i = 0; i < typed_in->length(); i++) {
      if (in_null_count_ > 0 && typed_in->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp = typed_in->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_[dest_group_id] += *(CType*)data;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    auto length = GetResultLength();
    cache_.resize(length);
    cache_validity_.resize(length);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_[i]);
        builder_isempty_->Append(true);
      } else {
        builder_->AppendNull();
        builder_isempty_->Append(false);
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Array> arr_isempty_out;
    builder_->Reset();
    builder_isempty_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        builder_->Append(cache_[offset + i]);
        builder_isempty_->Append(true);
      } else {
        builder_->AppendNull();
        builder_isempty_->Append(false);
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    RETURN_NOT_OK(builder_isempty_->Finish(&arr_isempty_out));
    out->push_back(arr_out);
    out->push_back(arr_isempty_out);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  CType* data_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
  std::unique_ptr<arrow::TypeTraits<arrow::BooleanType>::BuilderType> builder_isempty_;
  uint64_t length_ = 0;
};

//////////////// AvgAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class AvgAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class AvgAction<DataType, CType, ResDataType, ResCType,
                precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  AvgAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
#ifdef DEBUG
    std::cout << "Construct AvgAction" << std::endl;
#endif
  }
  ~AvgAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_sum_[0] += typed_scalar->value;

    arrow::compute::CountOptions option(arrow::compute::CountOptions::ONLY_VALID);
    maybe_output = arrow::compute::Count(*in[0].get(), option, ctx_);
    output = *std::move(maybe_output);
    auto count_typed_scalar = std::dynamic_pointer_cast<CountScalarType>(output.scalar());
    cache_count_[0] += count_typed_scalar->value;

    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += 1;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();
    for (int i = 0; i < length; i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    std::shared_ptr<arrow::Array> arr_out;
    cache_sum_.resize(length);
    cache_validity_.resize(length);
    RETURN_NOT_OK(builder_->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      cache_sum_[i + offset] /= cache_count_[i + offset];
    }
    std::shared_ptr<arrow::Array> arr_out;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using CountScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

// Decimal
template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class AvgAction<DataType, CType, ResDataType, ResCType,
                precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  AvgAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
            std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
#ifdef DEBUG
    std::cout << "Construct AvgAction" << std::endl;
#endif
  }
  ~AvgAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_sum_[0] += typed_scalar->value;

    arrow::compute::CountOptions option(arrow::compute::CountOptions::ONLY_VALID);
    maybe_output = arrow::compute::Count(*in[0].get(), option, ctx_);
    output = *std::move(maybe_output);
    auto count_typed_scalar = std::dynamic_pointer_cast<CountScalarType>(output.scalar());
    cache_count_[0] += count_typed_scalar->value;

    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += 1;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();
    for (int i = 0; i < length; i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    std::shared_ptr<arrow::Array> arr_out;
    cache_sum_.resize(length);
    cache_validity_.resize(length);
    for (int i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_sum_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      cache_sum_[i + offset] /= cache_count_[i + offset];
    }
    std::shared_ptr<arrow::Array> arr_out;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using CountScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

//////////////// SumCountAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class SumCountAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumCountAction<DataType, CType, ResDataType, ResCType,
                     precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  SumCountAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
                 std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> sum_builder;
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::float64(), &sum_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::int64(), &count_builder);
    sum_builder_.reset(
        arrow::internal::checked_cast<arrow::DoubleBuilder*>(sum_builder.release()));
    count_builder_.reset(
        arrow::internal::checked_cast<arrow::Int64Builder*>(count_builder.release()));

#ifdef DEBUG
    std::cout << "Construct SumCountAction" << std::endl;
#endif
  }
  ~SumCountAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_validity_.resize(max_group_id + 1, false);
      length_ = cache_sum_.size();
    }

    in_ = in_list[0];
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        cache_validity_[dest_group_id] = true;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_sum_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_sum_.size() * 2;
    }
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_validity_.resize(max_group_id, false);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_sum_.empty()) {
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, true);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_sum_[0] += typed_scalar->value;

    arrow::compute::CountOptions option(arrow::compute::CountOptions::ONLY_VALID);
    maybe_output = arrow::compute::Count(*in[0].get(), option, ctx_);
    output = *std::move(maybe_output);
    auto count_typed_scalar = std::dynamic_pointer_cast<CountScalarType>(output.scalar());
    cache_count_[0] += count_typed_scalar->value;

    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += 1;
    cache_validity_[dest_group_id] = true;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();

    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    sum_builder_->Reset();
    count_builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using CountScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<arrow::DoubleBuilder> sum_builder_;
  std::unique_ptr<arrow::Int64Builder> count_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

/// Decimal ///
template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumCountAction<DataType, CType, ResDataType, ResCType,
                     precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  SumCountAction(arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
                 std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> sum_builder;
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &sum_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::int64(), &count_builder);
    sum_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(sum_builder.release()));
    count_builder_.reset(
        arrow::internal::checked_cast<arrow::Int64Builder*>(count_builder.release()));

#ifdef DEBUG
    std::cout << "Construct SumCountAction" << std::endl;
#endif
  }
  ~SumCountAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_validity_.resize(max_group_id + 1, false);
      length_ = cache_sum_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += in_->GetView(row_id);
        cache_count_[dest_group_id] += 1;
        cache_validity_[dest_group_id] = true;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_sum_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_sum_.size() * 2;
    }
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_validity_.resize(max_group_id, false);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in = std::make_shared<ArrayType>(in[0]);
    in_null_count_ = typed_in->null_count();
    for (int i = 0; i < typed_in->length(); i++) {
      if (in_null_count_ > 0 && typed_in->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp = typed_in->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp));
      }
    }

    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += 1;
    cache_validity_[dest_group_id] = true;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();

    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    sum_builder_->Reset();
    count_builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> sum_builder_;
  std::unique_ptr<arrow::Int64Builder> count_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

//////////////// SumCountMergeAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class SumCountMergeAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumCountMergeAction<DataType, CType, ResDataType, ResCType,
                          precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  SumCountMergeAction(arrow::compute::ExecContext* ctx,
                      std::shared_ptr<arrow::DataType> type,
                      std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> sum_builder;
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &sum_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::int64(), &count_builder);
    sum_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(sum_builder.release()));
    count_builder_.reset(
        arrow::internal::checked_cast<arrow::Int64Builder*>(count_builder.release()));
#ifdef DEBUG
    std::cout << "Construct SumCountMergeAction" << std::endl;
#endif
  }
  ~SumCountMergeAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountMergeAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_validity_.resize(max_group_id + 1, false);
      length_ = cache_sum_.size();
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    in_null_count_ = in_sum_->null_count();
    // prepare evaluate lambda
    data_sum_ = const_cast<CType*>(in_sum_->data()->GetValues<CType>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_sum_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
        cache_validity_[dest_group_id] = true;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_sum_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_sum_.size() * 2;
    }
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_validity_.resize(max_group_id, false);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_sum_.empty()) {
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, true);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_sum_[0] += typed_scalar->value;

    maybe_output = arrow::compute::Sum(*in[1].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto count_typed_scalar = std::dynamic_pointer_cast<CountScalarType>(output.scalar());
    cache_count_[0] += count_typed_scalar->value;

    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += *(int64_t*)data2;
    cache_validity_[dest_group_id] = true;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();

    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    sum_builder_->Reset();
    count_builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using CountScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> sum_builder_;
  std::unique_ptr<arrow::Int64Builder> count_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class SumCountMergeAction<DataType, CType, ResDataType, ResCType,
                          precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  SumCountMergeAction(arrow::compute::ExecContext* ctx,
                      std::shared_ptr<arrow::DataType> type,
                      std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> sum_builder;
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &sum_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::int64(), &count_builder);
    sum_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(sum_builder.release()));
    count_builder_.reset(
        arrow::internal::checked_cast<arrow::Int64Builder*>(count_builder.release()));
#ifdef DEBUG
    std::cout << "Construct SumCountMergeAction" << std::endl;
#endif
  }
  ~SumCountMergeAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountMergeAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_validity_.resize(max_group_id + 1, false);
      length_ = cache_sum_.size();
    }

    in_sum_ = std::make_shared<ArrayType>(in_list[0]);
    in_count_ = std::make_shared<precompile::Int64Array>(in_list[1]);
    in_null_count_ = in_sum_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_sum_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += in_sum_->GetView(row_id);
        cache_count_[dest_group_id] += in_count_->GetView(row_id);
        cache_validity_[dest_group_id] = true;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_sum_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_sum_.size() * 2;
    }
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_validity_.resize(max_group_id, false);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in_sum = std::make_shared<ArrayType>(in[0]);
    auto typed_in_count = std::make_shared<precompile::Int64Array>(in[1]);
    in_null_count_ = typed_in_sum->null_count();
    for (int i = 0; i < typed_in_sum->length(); i++) {
      if (in_null_count_ > 0 && typed_in_sum->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp_sum = typed_in_sum->GetView(i);
        auto tmp_count = typed_in_count->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp_sum, (void*)&tmp_count));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += *(int64_t*)data2;
    cache_validity_[dest_group_id] = true;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_sum_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    sum_builder_->Reset();
    count_builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(sum_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
      } else {
        RETURN_NOT_OK(sum_builder_->AppendNull());
        RETURN_NOT_OK(count_builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder_->Finish(&sum_array));
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> sum_builder_;
  std::unique_ptr<arrow::Int64Builder> count_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  int row_id;
  std::shared_ptr<ArrayType> in_sum_;
  std::shared_ptr<precompile::Int64Array> in_count_;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

//////////////// AvgByCountAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class AvgByCountAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class AvgByCountAction<DataType, CType, ResDataType, ResCType,
                       precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  AvgByCountAction(arrow::compute::ExecContext* ctx,
                   std::shared_ptr<arrow::DataType> type,
                   std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
#ifdef DEBUG
    std::cout << "Construct AvgByCountAction" << std::endl;
#endif
  }
  ~AvgByCountAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgByCountAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    // prepare evaluate lambda
    data_sum_ = const_cast<CType*>(in_sum_->data()->GetValues<CType>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    in_null_count_ = in_sum_->null_count();
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_sum_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }
    arrow::Datum output;
    auto maybe_output = arrow::compute::Sum(*in[0].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(output.scalar());
    cache_sum_[0] += typed_scalar->value;

    maybe_output = arrow::compute::Sum(*in[1].get(), arrow::compute::ScalarAggregateOptions::Defaults(), ctx_);
    output = *std::move(maybe_output);
    auto count_typed_scalar = std::dynamic_pointer_cast<CountScalarType>(output.scalar());
    cache_count_[0] += count_typed_scalar->value;

    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += *(int64_t*)data2;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < length_; i++) {
      if (cache_count_[i] == 0) {
        cache_sum_[i] = 0;
        cache_validity_[i] = false;
      } else {
        cache_validity_[i] = true;
        cache_sum_[i] /= cache_count_[i];
      }
    }
    cache_sum_.resize(length_);
    cache_validity_.resize(length_);
    RETURN_NOT_OK(builder_->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      if (cache_count_[i + offset] == 0) {
        cache_sum_[i + offset] = 0;
        cache_validity_[i] = false;
      } else {
        cache_validity_[i + offset] = true;
        cache_sum_[i + offset] /= cache_count_[i + offset];
      }
    }
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_arr;
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);
    return arrow::Status::OK();
  }

 private:
  using ScalarType = typename arrow::TypeTraits<ResDataType>::ScalarType;
  using CountScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  CType* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class AvgByCountAction<DataType, CType, ResDataType, ResCType,
                       precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  AvgByCountAction(arrow::compute::ExecContext* ctx,
                   std::shared_ptr<arrow::DataType> type,
                   std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    auto typed_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
    auto typed_res_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(res_type);
    scale_ = typed_type->scale();
    res_precision_ = typed_type->precision();
    res_scale_ = typed_res_type->scale();
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
#ifdef DEBUG
    std::cout << "Construct AvgByCountAction" << std::endl;
#endif
  }
  ~AvgByCountAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgByCountAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_sum_ = std::make_shared<ArrayType>(in_list[0]);
    in_count_ = std::make_shared<precompile::Int64Array>(in_list[1]);
    // prepare evaluate lambda
    row_id = 0;
    in_null_count_ = in_sum_->null_count();
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_sum_->IsNull(row_id);
      if (!is_null) {
        cache_sum_[dest_group_id] += in_sum_->GetView(row_id);
        cache_count_[dest_group_id] += in_count_->GetView(row_id);
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto typed_in_sum = std::make_shared<ArrayType>(in[0]);
    auto typed_in_count = std::make_shared<precompile::Int64Array>(in[1]);
    in_null_count_ = typed_in_sum->null_count();
    for (int i = 0; i < typed_in_sum->length(); i++) {
      if (in_null_count_ > 0 && typed_in_sum->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto tmp_sum = typed_in_sum->GetView(i);
        auto tmp_count = typed_in_count->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&tmp_sum, (void*)&tmp_count));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_sum_[dest_group_id] += *(CType*)data;
    cache_count_[dest_group_id] += *(int64_t*)data2;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < length_; i++) {
      if (cache_count_[i] == 0) {
        cache_sum_[i] = 0;
      } else {
        cache_validity_[i] = true;
        if (res_scale_ != scale_) {
          cache_sum_[i] = cache_sum_[i].Rescale(scale_, res_scale_).ValueOrDie();
        }
        cache_sum_[i] =
            divide(cache_sum_[i], res_precision_, res_scale_, cache_count_[i]);
      }
    }
    cache_sum_.resize(length_);
    cache_validity_.resize(length_);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        builder_->Append(cache_sum_[i]);
      } else {
        builder_->AppendNull();
      }
    }
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      if (cache_count_[i + offset] == 0) {
        cache_sum_[i + offset] = 0;
      } else {
        cache_validity_[i + offset] = true;
        if (res_scale_ != scale_) {
          cache_sum_[i + offset] =
              cache_sum_[i + offset].Rescale(scale_, res_scale_).ValueOrDie();
        }
        cache_sum_[i + offset] = divide(cache_sum_[i + offset], res_precision_,
                                        res_scale_, cache_count_[i + offset]);
      }
    }
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_arr;
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  int row_id;
  std::shared_ptr<ArrayType> in_sum_;
  std::shared_ptr<precompile::Int64Array> in_count_;
  int in_null_count_ = 0;
  // result
  int scale_;
  int res_precision_;
  int res_scale_;
  std::vector<ResCType> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

//////////////// StddevSampPartialAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class StddevSampPartialAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class StddevSampPartialAction<DataType, CType, ResDataType, ResCType,
                              precompile::enable_if_number<DataType>>
    : public ActionBase {
 public:
  StddevSampPartialAction(arrow::compute::ExecContext* ctx,
                          std::shared_ptr<arrow::DataType> type,
                          std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    std::unique_ptr<arrow::ArrayBuilder> avg_builder;
    std::unique_ptr<arrow::ArrayBuilder> m2_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &count_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &avg_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &m2_builder);
    count_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(count_builder.release()));
    avg_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(avg_builder.release()));
    m2_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(m2_builder.release()));
#ifdef DEBUG
    std::cout << "Construct StddevSampPartialAction" << std::endl;
#endif
  }
  ~StddevSampPartialAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampPartialAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetView(row_id);
        cache_validity_[dest_group_id] = true;
        auto pre_avg =
            cache_sum_[dest_group_id] * 1.0 /
            (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
        auto delta = data * 1.0 - pre_avg;
        auto deltaN = delta / (cache_count_[dest_group_id] + 1);
        cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
        cache_sum_[dest_group_id] += data * 1.0;
        cache_count_[dest_group_id] += 1.0;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_m2_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_m2_.resize(1, 0);
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }

    auto len = in[0]->length();
    auto typed_in_0 = std::make_shared<ArrayType>(in[0]);
    auto in_null_count = typed_in_0->null_count();
    for (int i = 0; i < len; i++) {
      if (in_null_count > 0 && typed_in_0->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto v = typed_in_0->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&v));
      }
    }

    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    auto pre_avg = cache_sum_[dest_group_id] * 1.0 /
                   (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
    auto delta = *(CType*)data * 1.0 - pre_avg;
    auto deltaN = delta / (cache_count_[dest_group_id] + 1);
    cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
    cache_sum_[dest_group_id] += *(CType*)data * 1.0;
    cache_count_[dest_group_id] += 1.0;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    cache_count_.resize(length_);
    cache_sum_.resize(length_);
    cache_m2_.resize(length_);
    cache_validity_.resize(length_);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        if (cache_count_[i] < 0.0001) {
          cache_sum_[i] = 0;
        } else {
          cache_sum_[i] /= cache_count_[i] * 1.0;
        }
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
        RETURN_NOT_OK(avg_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(m2_builder_->Append(cache_m2_[i]));
      } else {
        // append zero to count_builder if all values in this group are null
        auto zero = 0;
        RETURN_NOT_OK(count_builder_->Append(zero));
        RETURN_NOT_OK(avg_builder_->Append(zero));
        RETURN_NOT_OK(m2_builder_->Append(zero));
      }
    }
    // get count
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    // get avg
    std::shared_ptr<arrow::Array> avg_array;
    RETURN_NOT_OK(avg_builder_->Finish(&avg_array));
    // get m2
    std::shared_ptr<arrow::Array> m2_array;
    RETURN_NOT_OK(m2_builder_->Finish(&m2_array));

    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    count_builder_->Reset();
    avg_builder_->Reset();
    m2_builder_->Reset();

    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;

    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        if (cache_count_[i + offset] < 0.00001) {
          cache_sum_[i + offset] = 0;
        } else {
          cache_sum_[i + offset] /= cache_count_[i + offset];
        }
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
        RETURN_NOT_OK(avg_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(m2_builder_->Append(cache_m2_[offset + i]));
      } else {
        // append zero to count_builder if all values in this group are null
        auto zero = 0;
        RETURN_NOT_OK(count_builder_->Append(zero));
        RETURN_NOT_OK(avg_builder_->Append(zero));
        RETURN_NOT_OK(m2_builder_->Append(zero));
      }
    }

    std::shared_ptr<arrow::Array> count_array;
    std::shared_ptr<arrow::Array> avg_array;
    std::shared_ptr<arrow::Array> m2_array;
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    RETURN_NOT_OK(avg_builder_->Finish(&avg_array));
    RETURN_NOT_OK(m2_builder_->Finish(&m2_array));
    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> count_builder_;
  std::unique_ptr<ResBuilderType> avg_builder_;
  std::unique_ptr<ResBuilderType> m2_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_count_;
  std::vector<ResCType> cache_sum_;
  std::vector<ResCType> cache_m2_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class StddevSampPartialAction<DataType, CType, ResDataType, ResCType,
                              precompile::enable_if_decimal<DataType>>
    : public ActionBase {
 public:
  StddevSampPartialAction(arrow::compute::ExecContext* ctx,
                          std::shared_ptr<arrow::DataType> type,
                          std::shared_ptr<arrow::DataType> res_type)
      : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> count_builder;
    std::unique_ptr<arrow::ArrayBuilder> avg_builder;
    std::unique_ptr<arrow::ArrayBuilder> m2_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &count_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &avg_builder);
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &m2_builder);
    count_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(count_builder.release()));
    avg_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(avg_builder.release()));
    m2_builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(m2_builder.release()));

    auto typed_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
    scale = typed_type->scale();
#ifdef DEBUG
    std::cout << "Construct StddevSampPartialAction" << std::endl;
#endif
  }
  ~StddevSampPartialAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampPartialAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
      length_ = cache_validity_.size();
    }

    in_ = std::make_shared<ArrayType>(in_list[0]);
    in_null_count_ = in_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_->IsNull(row_id);
      if (!is_null) {
        auto data = in_->GetView(row_id).ToDouble(scale);
        cache_validity_[dest_group_id] = true;
        auto pre_avg =
            cache_sum_[dest_group_id] * 1.0 /
            (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
        auto delta = data - pre_avg;
        auto deltaN = delta / (cache_count_[dest_group_id] + 1);
        cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
        cache_sum_[dest_group_id] += data * 1.0;
        cache_count_[dest_group_id] += 1.0;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_sum_.resize(max_group_id, 0);
    cache_count_.resize(max_group_id, 0);
    cache_m2_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    if (cache_validity_.empty()) {
      cache_m2_.resize(1, 0);
      cache_sum_.resize(1, 0);
      cache_count_.resize(1, 0);
      cache_validity_.resize(1, false);
      length_ = 1;
    }

    auto len = in[0]->length();
    auto typed_in_0 = std::make_shared<ArrayType>(in[0]);
    auto in_null_count = typed_in_0->null_count();
    for (int i = 0; i < len; i++) {
      if (in_null_count > 0 && typed_in_0->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto v = typed_in_0->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&v));
      }
    }

    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    auto double_data = (*(CType*)data).ToDouble(scale);
    auto pre_avg = cache_sum_[dest_group_id] * 1.0 /
                   (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
    auto delta = double_data - pre_avg;
    auto deltaN = delta / (cache_count_[dest_group_id] + 1);
    cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
    cache_sum_[dest_group_id] += double_data * 1.0;
    cache_count_[dest_group_id] += 1.0;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    cache_count_.resize(length_);
    cache_sum_.resize(length_);
    cache_m2_.resize(length_);
    cache_validity_.resize(length_);
    for (int i = 0; i < length_; i++) {
      if (cache_validity_[i]) {
        if (cache_count_[i] < 0.00001) {
          cache_sum_[i] = 0;
        } else {
          cache_sum_[i] /= cache_count_[i] * 1.0;
        }
        RETURN_NOT_OK(count_builder_->Append(cache_count_[i]));
        RETURN_NOT_OK(avg_builder_->Append(cache_sum_[i]));
        RETURN_NOT_OK(m2_builder_->Append(cache_m2_[i]));
      } else {
        // append zero to count_builder if all values in this group are null
        RETURN_NOT_OK(count_builder_->Append(0));
        RETURN_NOT_OK(avg_builder_->Append(0));
        RETURN_NOT_OK(m2_builder_->Append(0));
      }
    }
    // get count
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    // get avg
    std::shared_ptr<arrow::Array> avg_array;
    RETURN_NOT_OK(avg_builder_->Finish(&avg_array));
    // get m2
    std::shared_ptr<arrow::Array> m2_array;
    RETURN_NOT_OK(m2_builder_->Finish(&m2_array));

    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    count_builder_->Reset();
    avg_builder_->Reset();
    m2_builder_->Reset();

    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;

    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_validity_[offset + i]) {
        if (cache_count_[i + offset] < 0.00001) {
          cache_sum_[i + offset] = 0;
        } else {
          cache_sum_[i + offset] /= cache_count_[i + offset];
        }
        RETURN_NOT_OK(count_builder_->Append(cache_count_[offset + i]));
        RETURN_NOT_OK(avg_builder_->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(m2_builder_->Append(cache_m2_[offset + i]));
      } else {
        // append zero to count_builder if all values in this group are null
        RETURN_NOT_OK(count_builder_->Append(0));
        RETURN_NOT_OK(avg_builder_->Append(0));
        RETURN_NOT_OK(m2_builder_->Append(0));
      }
    }

    std::shared_ptr<arrow::Array> count_array;
    std::shared_ptr<arrow::Array> avg_array;
    std::shared_ptr<arrow::Array> m2_array;
    RETURN_NOT_OK(count_builder_->Finish(&count_array));
    RETURN_NOT_OK(avg_builder_->Finish(&avg_array));
    RETURN_NOT_OK(m2_builder_->Finish(&m2_array));
    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> count_builder_;
  std::unique_ptr<ResBuilderType> avg_builder_;
  std::unique_ptr<ResBuilderType> m2_builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  int row_id;
  int in_null_count_ = 0;
  int scale;
  // result
  std::vector<ResCType> cache_count_;
  std::vector<ResCType> cache_sum_;
  std::vector<ResCType> cache_m2_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

//////////////// StddevSampFinalAction ///////////////
template <typename DataType, typename CType, typename ResDataType, typename ResCType,
          typename Enable = void>
class StddevSampFinalAction {};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class StddevSampFinalAction<DataType, CType, ResDataType, ResCType,
                            precompile::enable_if_number<DataType>> : public ActionBase {
 public:
  StddevSampFinalAction(arrow::compute::ExecContext* ctx,
                        std::shared_ptr<arrow::DataType> type,
                        std::shared_ptr<arrow::DataType> res_type,
                        bool null_on_divide_by_zero)
      : ctx_(ctx), null_on_divide_by_zero_(null_on_divide_by_zero) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
#ifdef DEBUG
    std::cout << "Construct StddevSampFinalAction" << std::endl;
#endif
  }
  ~StddevSampFinalAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampFinalAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_count_.resize(max_group_id + 1, 0);
      cache_avg_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
      length_ = cache_count_.size();
    }
    in_count_ = std::make_shared<ArrayType>(in_list[0]);
    in_avg_ = std::make_shared<ArrayType>(in_list[1]);
    in_m2_ = std::make_shared<ArrayType>(in_list[2]);
    in_null_count_ = in_m2_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_m2_->IsNull(row_id);
      auto data_count = in_count_->GetView(row_id);
      if (!is_null && data_count > 0) {
        cache_validity_[dest_group_id] = true;
        auto pre_avg = cache_avg_[dest_group_id];
        auto delta = in_avg_->GetView(row_id) - pre_avg;
        auto n1 = cache_count_[dest_group_id];
        auto n2 = data_count;
        auto deltaN = (n1 + n2) > 0 ? delta / (n1 + n2) : 0;
        cache_m2_[dest_group_id] += (in_m2_->GetView(row_id) + delta * deltaN * n1 * n2);
        cache_avg_[dest_group_id] += deltaN * n2;
        cache_count_[dest_group_id] += n2;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_count_.resize(max_group_id, 0);
    cache_avg_.resize(max_group_id, 0);
    cache_m2_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto len = in[0]->length();
    auto typed_in_0 = std::make_shared<ArrayType>(in[0]);
    auto typed_in_1 = std::make_shared<ArrayType>(in[1]);
    auto typed_in_2 = std::make_shared<ArrayType>(in[2]);
    auto in_null_count = typed_in_0->null_count();
    for (int i = 0; i < len; i++) {
      if (in_null_count > 0 && typed_in_0->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto v1 = typed_in_0->GetView(i);
        auto v2 = typed_in_1->GetView(i);
        auto v3 = typed_in_2->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&v1, (void*)&v2, (void*)&v3));
      }
    }
    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2, void* data3) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    auto pre_avg = cache_avg_[dest_group_id];
    auto delta = *(CType*)data2 - pre_avg;
    auto n1 = cache_count_[dest_group_id];
    auto n2 = *(CType*)data;
    auto deltaN = (n1 + n2) > 0 ? delta / (n1 + n2) : 0;
    cache_m2_[dest_group_id] += (*(CType*)data3 + delta * deltaN * n1 * n2);
    cache_avg_[dest_group_id] += deltaN * n2;
    cache_count_[dest_group_id] += n2;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < length_; i++) {
      auto tmp = cache_m2_[i] / (cache_count_[i] > 1 ? (cache_count_[i] - 1) : 1);
      cache_m2_[i] = sqrt(tmp);
    }
    cache_m2_.resize(length_);
    cache_validity_.resize(length_);
    for (int i = 0; i < length_; i++) {
      if (cache_count_[i] - 1 < 0.00001) {
        if (null_on_divide_by_zero_) {
          RETURN_NOT_OK(builder_->AppendNull());
        } else {
          // append NaN if only one non-null value exists
          RETURN_NOT_OK(builder_->Append(std::numeric_limits<ResCType>::quiet_NaN()));
        }
      } else if (cache_validity_[i]) {
        RETURN_NOT_OK(builder_->Append(cache_m2_[i]));
      } else {
        // append null if all values are null
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      auto tmp = cache_m2_[i + offset] /
                 (cache_count_[i + offset] > 1 ? (cache_count_[i + offset] - 1) : 1);
      cache_m2_[i + offset] = sqrt(tmp);
    }
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_count_[offset + i] - 1 < 0.00001) {
        if (null_on_divide_by_zero_) {
          RETURN_NOT_OK(builder_->AppendNull());
        } else {
          // append NaN if only one non-null value exists
          RETURN_NOT_OK(builder_->Append(std::numeric_limits<ResCType>::quiet_NaN()));
        }
      } else if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_m2_[offset + i]));
      } else {
        // append null if all values are null
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_array;
    RETURN_NOT_OK(builder_->Finish(&out_array));
    out->push_back(out_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  // input
  arrow::compute::ExecContext* ctx_;
  bool null_on_divide_by_zero_;
  int row_id;
  std::shared_ptr<ArrayType> in_count_;
  std::shared_ptr<ArrayType> in_avg_;
  std::shared_ptr<ArrayType> in_m2_;
  int in_null_count_ = 0;
  // result
  std::vector<ResCType> cache_count_;
  std::vector<ResCType> cache_avg_;
  std::vector<ResCType> cache_m2_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

template <typename DataType, typename CType, typename ResDataType, typename ResCType>
class StddevSampFinalAction<DataType, CType, ResDataType, ResCType,
                            precompile::enable_if_decimal<DataType>> : public ActionBase {
 public:
  StddevSampFinalAction(arrow::compute::ExecContext* ctx,
                        std::shared_ptr<arrow::DataType> type,
                        std::shared_ptr<arrow::DataType> res_type,
                        bool null_on_divide_by_zero)
      : ctx_(ctx), null_on_divide_by_zero_(null_on_divide_by_zero) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), res_type, &builder);
    builder_.reset(arrow::internal::checked_cast<ResBuilderType*>(builder.release()));
    auto typed_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
    zero = castDECIMAL(double(0.0), typed_type->precision(), typed_type->scale());
    one = castDECIMAL(double(1.0), typed_type->precision(), typed_type->scale());
    scale = typed_type->scale();

#ifdef DEBUG
    std::cout << "Construct StddevSampFinalAction" << std::endl;
#endif
  }
  ~StddevSampFinalAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampFinalAction" << std::endl;
#endif
  }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_count_.resize(max_group_id + 1, 0);
      cache_avg_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
      length_ = cache_count_.size();
    }

    in_count_ = std::make_shared<CountArrayType>(in_list[0]);
    in_avg_ = std::make_shared<ArrayType>(in_list[1]);
    in_m2_ = std::make_shared<ArrayType>(in_list[2]);
    in_null_count_ = in_m2_->null_count();
    // prepare evaluate lambda
    row_id = 0;
    *on_valid = [this](int dest_group_id) {
      const bool is_null = in_null_count_ > 0 && in_m2_->IsNull(row_id);
      auto data_count = in_count_->GetView(row_id);
      if (!is_null && data_count > 0) {
        cache_validity_[dest_group_id] = true;
        auto pre_avg = cache_avg_[dest_group_id];
        auto delta = in_avg_->GetView(row_id) - pre_avg;
        auto n1 = cache_count_[dest_group_id];
        auto n2 = data_count;
        arrow::Decimal128 deltaN;
        if ((n1 + n2) > 0) {
          deltaN = delta / (n1 + n2);
        } else {
          deltaN = zero;
        }
        cache_m2_[dest_group_id] += (in_m2_->GetView(row_id) + delta * deltaN * n1 * n2);
        cache_avg_[dest_group_id] += deltaN * n2;
        cache_count_[dest_group_id] += n2;
      }
      row_id++;
      return arrow::Status::OK();
    };

    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status GrowByFactor(int dest_group_id) {
    int max_group_id;
    if (cache_validity_.size() < 128) {
      max_group_id = 128;
    } else {
      max_group_id = cache_validity_.size() * 2;
    }
    cache_validity_.resize(max_group_id, false);
    cache_count_.resize(max_group_id, 0);
    cache_avg_.resize(max_group_id, 0);
    cache_m2_.resize(max_group_id, 0);
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const arrow::ArrayVector& in) {
    auto len = in[0]->length();
    auto typed_in_0 = std::make_shared<CountArrayType>(in[0]);
    auto typed_in_1 = std::make_shared<ArrayType>(in[1]);
    auto typed_in_2 = std::make_shared<ArrayType>(in[2]);
    auto in_null_count = typed_in_0->null_count();
    for (int i = 0; i < len; i++) {
      if (in_null_count > 0 && typed_in_0->IsNull(i)) {
        RETURN_NOT_OK(EvaluateNull(0));
      } else {
        auto v1 = typed_in_0->GetView(i);
        auto v2 = typed_in_1->GetView(i);
        auto v3 = typed_in_2->GetView(i);
        RETURN_NOT_OK(Evaluate(0, (void*)&v1, (void*)&v2, (void*)&v3));
      }
    }
    if (!cache_validity_[0]) cache_validity_[0] = true;
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(int dest_group_id, void* data, void* data2, void* data3) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    cache_validity_[dest_group_id] = true;
    auto pre_avg = cache_avg_[dest_group_id];
    auto delta = *(CType*)data2 - pre_avg;
    auto n1 = cache_count_[dest_group_id];
    auto n2 = *(CountCType*)data;
    arrow::Decimal128 deltaN;
    if ((n1 + n2) > 0) {
      deltaN = delta / (n1 + n2);
    } else {
      deltaN = zero;
    }
    cache_m2_[dest_group_id] += (*(CType*)data3 + delta * deltaN * n1 * n2);
    cache_avg_[dest_group_id] += deltaN * n2;
    cache_count_[dest_group_id] += n2;
    return arrow::Status::OK();
  }

  arrow::Status EvaluateNull(int dest_group_id) {
    auto target_group_size = dest_group_id + 1;
    if (cache_validity_.size() <= target_group_size) GrowByFactor(target_group_size);
    if (length_ < target_group_size) length_ = target_group_size;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < length_; i++) {
      arrow::Decimal128 tmp;
      if (cache_count_[i] > 1) {
        tmp = cache_m2_[i] / (cache_count_[i] - 1);
      } else {
        tmp = cache_m2_[i];
      }
      cache_m2_[i] = tmp * tmp;
    }
    cache_m2_.resize(length_);
    cache_validity_.resize(length_);
    for (int i = 0; i < length_; i++) {
      if (cache_count_[i] - 1 < 0) {
        if (null_on_divide_by_zero_) {
          RETURN_NOT_OK(builder_->AppendNull());
        } else {
          // append NaN if only one non-null value exists
          RETURN_NOT_OK(builder_->Append(std::numeric_limits<ResCType>::quiet_NaN()));
        }
      } else if (cache_validity_[i]) {
        RETURN_NOT_OK(builder_->Append(cache_m2_[i]));
      } else {
        // append null if all values are null
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }
    RETURN_NOT_OK(builder_->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return length_; }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    builder_->Reset();
    auto res_length = (offset + length) > length_ ? (length_ - offset) : length;
    for (int i = 0; i < res_length; i++) {
      arrow::Decimal128 tmp;
      if (cache_count_[i + offset] > 1) {
        tmp = cache_m2_[i + offset] / (cache_count_[i + offset] - 1);
      } else {
        tmp = cache_m2_[i + offset];
      }

      cache_m2_[i + offset] = tmp * tmp;
    }
    for (uint64_t i = 0; i < res_length; i++) {
      if (cache_count_[offset + i] - 1 < 0) {
        if (null_on_divide_by_zero_) {
          RETURN_NOT_OK(builder_->AppendNull());
        } else {
          // append NaN if only one non-null value exists
          RETURN_NOT_OK(builder_->Append(std::numeric_limits<ResCType>::quiet_NaN()));
        }
      } else if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder_->Append(cache_m2_[offset + i]));
      } else {
        // append null if all values are null
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_array;
    RETURN_NOT_OK(builder_->Finish(&out_array));
    out->push_back(out_array);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename precompile::TypeTraits<DataType>::ArrayType;
  using CountArrayType = typename precompile::TypeTraits<arrow::Int64Type>::ArrayType;
  using CountCType = typename precompile::TypeTraits<arrow::Int64Type>::CType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  std::unique_ptr<ResBuilderType> builder_;
  arrow::Decimal128 zero;
  arrow::Decimal128 one;
  // input
  arrow::compute::ExecContext* ctx_;
  bool null_on_divide_by_zero_;
  int row_id;
  std::shared_ptr<CountArrayType> in_count_;
  std::shared_ptr<ArrayType> in_avg_;
  std::shared_ptr<ArrayType> in_m2_;
  int in_null_count_ = 0;
  int scale;
  // result
  std::vector<CountCType> cache_count_;
  std::vector<ResCType> cache_avg_;
  std::vector<ResCType> cache_m2_;
  std::vector<bool> cache_validity_;
  uint64_t length_ = 0;
};

///////////////////// Public Functions //////////////////
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

arrow::Status MakeUniqueAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    using CType = typename arrow::TypeTraits<InType>::CType;                    \
    auto action_ptr = std::make_shared<UniqueAction<InType, CType>>(ctx, type); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::StringType::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::StringType, std::string>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::Date32Type, int32_t>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::TimestampType::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::TimestampType, int64_t>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::Decimal128Type, arrow::Decimal128>>(ctx,
                                                                                   type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::BooleanType, bool>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    default: {
      std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                << std::endl;
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::ExecContext* ctx,
                              std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                              std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountAction<arrow::Int64Type>>(ctx);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeCountLiteralAction(
    arrow::compute::ExecContext* ctx, int arg,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountLiteralAction<arrow::Int64Type>>(ctx, arg);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeMinAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out, bool NaN_check /*false*/) {
  switch (type->id()) {
#define PROCESS(InType)                                                                 \
  case InType::type_id: {                                                               \
    using CType = typename arrow::TypeTraits<InType>::CType;                            \
    auto action_ptr = std::make_shared<MinAction<InType, CType>>(ctx, type, NaN_check); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                           \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Date32Type::type_id: {
      using CType = typename arrow::TypeTraits<arrow::Date32Type>::CType;
      auto action_ptr = std::make_shared<MinAction<arrow::Date32Type, CType>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<MinAction<arrow::Decimal128Type, arrow::Decimal128>>(ctx,
                                                                                type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::StringType::type_id: {
      auto action_ptr =
          std::make_shared<MinAction<arrow::StringType, std::string>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto action_ptr = std::make_shared<MinAction<arrow::BooleanType, bool>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeMaxAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out, bool NaN_check /*false*/) {
  switch (type->id()) {
#define PROCESS(InType)                                                                 \
  case InType::type_id: {                                                               \
    using CType = typename arrow::TypeTraits<InType>::CType;                            \
    auto action_ptr = std::make_shared<MaxAction<InType, CType>>(ctx, type, NaN_check); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                           \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Date32Type::type_id: {
      using CType = typename arrow::TypeTraits<arrow::Date32Type>::CType;
      auto action_ptr = std::make_shared<MaxAction<arrow::Date32Type, CType>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<MaxAction<arrow::Decimal128Type, arrow::Decimal128>>(ctx,
                                                                                type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::StringType::type_id: {
      auto action_ptr =
          std::make_shared<MaxAction<arrow::StringType, std::string>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto action_ptr = std::make_shared<MaxAction<arrow::BooleanType, bool>>(ctx, type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id: {                                                                \
    using CType = typename arrow::TypeTraits<InType>::CType;                             \
    using ResDataType = typename FindAccumulatorType<InType>::Type;                      \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;                     \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();                    \
    auto action_ptr = std::make_shared<SumAction<InType, CType, ResDataType, ResCType>>( \
        ctx, type, res_type);                                                            \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                            \
  } break;

    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<SumAction<arrow::Decimal128Type, arrow::Decimal128,
                                     arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumAction<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(ctx, type,
                                                                             res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(ctx, type,
                                                                          res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumActionPartial(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                           \
  case InType::type_id: {                                                         \
    using CType = typename arrow::TypeTraits<InType>::CType;                      \
    using ResDataType = typename FindAccumulatorType<InType>::Type;               \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;              \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();             \
    auto action_ptr =                                                             \
        std::make_shared<SumActionPartial<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type);                                                 \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                     \
  } break;

    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<SumActionPartial<arrow::Decimal128Type, arrow::Decimal128,
                                            arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumActionPartial<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumActionPartial<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;

    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id: {                                                                \
    using CType = typename arrow::TypeTraits<InType>::CType;                             \
    using ResDataType = typename FindAccumulatorType<InType>::Type;                      \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;                     \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();                    \
    auto action_ptr = std::make_shared<AvgAction<InType, CType, ResDataType, ResCType>>( \
        ctx, type, res_type);                                                            \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                            \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<AvgAction<arrow::Decimal128Type, arrow::Decimal128,
                                     arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          AvgAction<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(ctx, type,
                                                                             res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          AvgAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(ctx, type,
                                                                          res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    using CType = typename arrow::TypeTraits<InType>::CType;                    \
    using ResDataType = typename FindAccumulatorType<InType>::Type;             \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;            \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();           \
    auto action_ptr =                                                           \
        std::make_shared<SumCountAction<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type);                                               \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<SumCountAction<arrow::Decimal128Type, arrow::Decimal128,
                                          arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumCountAction<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumCountAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(ctx, type,
                                                                               res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountMergeAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                              \
  case InType::type_id: {                                                            \
    using CType = typename arrow::TypeTraits<InType>::CType;                         \
    using ResDataType = typename FindAccumulatorType<InType>::Type;                  \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;                 \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();                \
    auto action_ptr =                                                                \
        std::make_shared<SumCountMergeAction<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type);                                                    \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<SumCountMergeAction<arrow::Decimal128Type, arrow::Decimal128,
                                               arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumCountMergeAction<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          SumCountMergeAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgByCountAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                           \
  case InType::type_id: {                                                         \
    using CType = typename arrow::TypeTraits<InType>::CType;                      \
    using ResDataType = typename FindAccumulatorType<InType>::Type;               \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;              \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();             \
    auto action_ptr =                                                             \
        std::make_shared<AvgByCountAction<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type);                                                 \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                     \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr =
          std::make_shared<AvgByCountAction<arrow::Decimal128Type, arrow::Decimal128,
                                            arrow::Decimal128Type, arrow::Decimal128>>(
              ctx, type, res_type_list[0]);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Date64Type>::type_singleton();
      auto action_ptr = std::make_shared<
          AvgByCountAction<arrow::Date32Type, int32_t, arrow::Date64Type, int64_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          AvgByCountAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeStddevSampPartialAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id: {                                                                \
    using CType = typename arrow::TypeTraits<InType>::CType;                             \
    using ResDataType = typename FindAccumulatorType<InType>::Type;                      \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;                     \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();                    \
    auto action_ptr =                                                                    \
        std::make_shared<StddevSampPartialAction<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type);                                                        \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                            \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    case arrow::Decimal128Type::type_id: {
      auto action_ptr = std::make_shared<StddevSampPartialAction<
          arrow::Decimal128Type, arrow::Decimal128, arrow::DoubleType, double>>(
          ctx, type, arrow::float64());
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::BooleanType::type_id: {
      auto res_type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
      auto action_ptr = std::make_shared<
          AvgByCountAction<arrow::BooleanType, bool, arrow::Int32Type, int32_t>>(
          ctx, type, res_type);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeStddevSampFinalAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    bool null_on_divide_by_zero, std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using CType = typename arrow::TypeTraits<InType>::CType;                           \
    using ResDataType = typename FindAccumulatorType<InType>::Type;                    \
    using ResCType = typename arrow::TypeTraits<ResDataType>::CType;                   \
    auto res_type = arrow::TypeTraits<ResDataType>::type_singleton();                  \
    auto action_ptr =                                                                  \
        std::make_shared<StddevSampFinalAction<InType, CType, ResDataType, ResCType>>( \
            ctx, type, res_type, null_on_divide_by_zero);                              \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
    /*case arrow::Decimal128Type::type_id: {
      auto action_ptr = std::make_shared<
          StddevSampFinalAction<arrow::Decimal128Type, arrow::Decimal128,
                                arrow::Decimal128Type, arrow::Decimal128>>(ctx,
    type, type); *out = std::dynamic_pointer_cast<ActionBase>(action_ptr); }
    break;*/
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
