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

arrow::Status ActionBase::Submit(ArrayList in,
                                 int max_group_id,
                                 std::function<arrow::Status(int)> *on_valid,
                                 std::function<arrow::Status()> *on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                                 std::function<arrow::Status(uint64_t, uint64_t)> *on_valid,
                                 std::function<arrow::Status()> *on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(const std::shared_ptr<arrow::Array> &in,
                                 std::stringstream *ss,
                                 std::function<arrow::Status(int)> *out) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Submit(const std::shared_ptr<arrow::Array> &in,
                                 std::function<arrow::Status(uint32_t)> *on_valid,
                                 std::function<arrow::Status()> *on_null) {
  return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
}

arrow::Status ActionBase::Finish(ArrayList *out) {
  return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
}

arrow::Status ActionBase::Finish(uint64_t offset, uint64_t length, ArrayList *out) {
  return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
}

arrow::Status ActionBase::FinishAndReset(ArrayList *out) {
  return arrow::Status::NotImplemented("ActionBase FinishAndReset is abstract.");
}

uint64_t ActionBase::GetResultLength() { return 0; }

//////////////// UniqueAction ///////////////
template <typename DataType, typename CType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct UniqueAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      null_flag_.resize(max_group_id + 1, false);
    }

    in_ = std::dynamic_pointer_cast<ArrayType>(in_list[0]);
    row_id_ = 0;
    // prepare evaluate lambda
    auto type = arrow::TypeTraits<DataType>::type_singleton();
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id_);
        if (cache_validity_[dest_group_id] == false) {
          if (!is_null) {
            cache_validity_[dest_group_id] = true;
            cache_.emplace(cache_.begin() + dest_group_id, in_->GetView(row_id_));
          } else {
            cache_validity_[dest_group_id] = true;
            null_flag_[dest_group_id] = true;
            CType num;
            cache_.emplace(cache_.begin() + dest_group_id, num);
          }
        }
        row_id_++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (cache_validity_[dest_group_id] == false) {
          cache_validity_[dest_group_id] = true;
          cache_.emplace(cache_.begin() + dest_group_id, in_->GetView(row_id_));
        }
        row_id_++;
        return arrow::Status::OK();
      };
    }

    *on_null = [this]() {
      row_id_++;
      return arrow::Status::OK();
    };
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

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    // appendValues to builder_
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[i]) {
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
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  int row_id_ = 0;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  // output
  std::unique_ptr<BuilderType> builder_;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::vector<bool> null_flag_;
};

//////////////// CountAction ///////////////
template <typename DataType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct CountAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
  }
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_.size() <= max_group_id) {
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    row_id = 0;
    // prepare evaluate lambda
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
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

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
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
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  int32_t row_id;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// CountLiteralAction ///////////////
template <typename DataType>
class CountLiteralAction : public ActionBase {
 public:
  CountLiteralAction(arrow::compute::FunctionContext* ctx, int arg)
      : ctx_(ctx), arg_(arg) {
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

  int RequiredColNum() { return 0; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_.size() <= max_group_id) {
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    *on_valid = [this](int dest_group_id) {
      cache_[dest_group_id] += arg_;
      return arrow::Status::OK();
    };

    *on_null = [this]() { return arrow::Status::OK(); };
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

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
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
  arrow::compute::FunctionContext* ctx_;
  int arg_;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// MinAction ///////////////
template <typename DataType>
class MinAction : public ActionBase {
 public:
  MinAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<DataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
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
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          if (data_[row_id] < cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] < cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
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

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
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
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
};

//////////////// MaxAction ///////////////
template <typename DataType>
class MaxAction : public ActionBase {
 public:
  MaxAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<DataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
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
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          if (data_[row_id] > cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] > cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
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

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
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
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<BuilderType> builder_;
};

//////////////// SumAction ///////////////
template <typename DataType>
class SumAction : public ActionBase {
 public:
  SumAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);
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
          cache_[dest_group_id] += data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data_[row_id];
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

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    builder_->Reset();
    for (uint64_t i = 0; i < length; i++) {
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
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::unique_ptr<ResBuilderType> builder_;
};

//////////////// AvgAction ///////////////
template <typename DataType>
class AvgAction : public ActionBase {
 public:
  AvgAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgAction" << std::endl;
#endif
  }
  ~AvgAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
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

  arrow::Status Finish(ArrayList* out) override {
    for (int i = 0; i < cache_sum_.size(); i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    std::shared_ptr<arrow::Array> arr_out;
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (int i = 0; i < length; i++) {
      cache_sum_[i + offset] /= cache_count_[i + offset];
    }
    std::shared_ptr<arrow::Array> arr_out;
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }

    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<uint64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// SumCountAction ///////////////
template <typename DataType>
class SumCountAction : public ActionBase {
 public:
  SumCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumCountAction" << std::endl;
#endif
  }
  ~SumCountAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
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

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      RETURN_NOT_OK(sum_builder->Append(cache_sum_[i]));
      RETURN_NOT_OK(count_builder->Append(cache_count_[i]));
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      RETURN_NOT_OK(sum_builder->Append(cache_sum_[offset + i]));
      RETURN_NOT_OK(count_builder->Append(cache_count_[offset + i]));
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
};

//////////////// SumCountMergeAction ///////////////
template <typename DataType>
class SumCountMergeAction : public ActionBase {
 public:
  SumCountMergeAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumCountMergeAction" << std::endl;
#endif
  }
  ~SumCountMergeAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountMergeAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 2; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_sum_.size() <= max_group_id) {
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    // prepare evaluate lambda
    data_sum_ = const_cast<double*>(in_sum_->data()->GetValues<double>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    if (in_sum_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_sum_->IsNull(row_id);
        if (!is_null) {
          cache_sum_[dest_group_id] += data_sum_[row_id];
          cache_count_[dest_group_id] += data_count_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
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

  arrow::Status Finish(ArrayList* out) override {
    auto length = GetResultLength();
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      RETURN_NOT_OK(sum_builder->Append(cache_sum_[i]));
      RETURN_NOT_OK(count_builder->Append(cache_count_[i]));
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      RETURN_NOT_OK(sum_builder->Append(cache_sum_[offset + i]));
      RETURN_NOT_OK(count_builder->Append(cache_count_[offset + i]));
    }

    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> count_array;
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  double* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
};

//////////////// AvgByCountAction ///////////////
template <typename DataType>
class AvgByCountAction : public ActionBase {
 public:
  AvgByCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgByCountAction" << std::endl;
#endif
  }
  ~AvgByCountAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgByCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 2; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    // prepare evaluate lambda
    data_sum_ = const_cast<double*>(in_sum_->data()->GetValues<double>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    if (in_sum_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_sum_->IsNull(row_id);
        if (!is_null) {
          cache_sum_[dest_group_id] += data_sum_[row_id];
          cache_count_[dest_group_id] += data_count_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
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

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < cache_sum_.size(); i++) {
      if (cache_count_[i] == 0) {
        cache_sum_[i] = 0;
      } else {
        cache_validity_[i] = true;
        cache_sum_[i] /= cache_count_[i];
      }
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (int i = 0; i < length; i++) {
      if (cache_count_[i + offset] == 0) {
        cache_sum_[i + offset] = 0;
      } else {
        cache_validity_[i + offset] = true;
        cache_sum_[i + offset] /= cache_count_[i + offset];
      }
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder->Append(cache_sum_[offset + i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_arr;
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  double* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// StddevSampPartialAction ///////////////
template <typename DataType>
class StddevSampPartialAction : public ActionBase {
 public:
  StddevSampPartialAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct StddevSampPartialAction" << std::endl;
#endif
  }
  ~StddevSampPartialAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampPartialAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
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
          double pre_avg = cache_sum_[dest_group_id] * 1.0 / 
            (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
          double delta = data_[row_id] * 1.0 - pre_avg;
          double deltaN = delta / (cache_count_[dest_group_id] + 1);
          cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
          cache_sum_[dest_group_id] += data_[row_id] * 1.0;
          cache_count_[dest_group_id] += 1.0;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        double pre_avg = cache_sum_[dest_group_id] * 1.0 / 
            (cache_count_[dest_group_id] > 0 ? cache_count_[dest_group_id] : 1);
        double delta = data_[row_id] * 1.0 - pre_avg;
        double deltaN = delta / (cache_count_[dest_group_id] + 1);
        cache_m2_[dest_group_id] += delta * deltaN * cache_count_[dest_group_id];
        cache_sum_[dest_group_id] += data_[row_id] * 1.0;
        cache_count_[dest_group_id] += 1.0;
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

  arrow::Status Finish(ArrayList* out) override {
    // get count
    std::shared_ptr<arrow::Array> count_array;
    auto count_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(count_builder->AppendValues(cache_count_, cache_validity_));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    // get avg
    std::shared_ptr<arrow::Array> avg_array;
    for (uint64_t i = 0; i < cache_sum_.size(); i++) {
      if (cache_count_[i] < 0.0001) {
        cache_sum_[i] = 0;
      } else {
        cache_sum_[i] /= cache_count_[i] * 1.0;
      }  
    }
    auto avg_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(avg_builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(avg_builder->Finish(&avg_array));
    //get m2
    std::shared_ptr<arrow::Array> m2_array;
    auto m2_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(m2_builder->AppendValues(cache_m2_, cache_validity_));
    RETURN_NOT_OK(m2_builder->Finish(&m2_array));

    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_sum_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (uint64_t i = 0; i < length; i++) {
      if (cache_count_[i + offset] < 0.00001) {
        cache_sum_[i + offset] = 0;
      } else {
        cache_sum_[i + offset] /= cache_count_[i + offset] * 1.0;
      }
    }
    auto count_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto avg_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    auto m2_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(count_builder->Append(cache_count_[offset + i]));
        RETURN_NOT_OK(avg_builder->Append(cache_sum_[offset + i]));
        RETURN_NOT_OK(m2_builder->Append(cache_m2_[offset + i]));
      } else {
        // append zero to count_builder if all values in this group are null
        double zero = 0;
        RETURN_NOT_OK(count_builder->Append(zero));
        RETURN_NOT_OK(avg_builder->Append(zero));
        RETURN_NOT_OK(m2_builder->Append(zero));
      }
    }

    std::shared_ptr<arrow::Array> count_array;
    std::shared_ptr<arrow::Array> avg_array;
    std::shared_ptr<arrow::Array> m2_array;
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    RETURN_NOT_OK(avg_builder->Finish(&avg_array));
    RETURN_NOT_OK(m2_builder->Finish(&m2_array));
    out->push_back(count_array);
    out->push_back(avg_array);
    out->push_back(m2_array);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<double> cache_count_;
  std::vector<double> cache_m2_;
  std::vector<bool> cache_validity_;
};

//////////////// StddevSampFinalAction ///////////////
template <typename DataType>
class StddevSampFinalAction : public ActionBase {
 public:
  StddevSampFinalAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct StddevSampFinalAction" << std::endl;
#endif
  }
  ~StddevSampFinalAction() {
#ifdef DEBUG
    std::cout << "Destruct StddevSampFinalAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 3; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_count_.resize(max_group_id + 1, 0);
      cache_avg_.resize(max_group_id + 1, 0);
      cache_m2_.resize(max_group_id + 1, 0);
    }
    in_count_ = in_list[0];
    in_avg_ = in_list[1];
    in_m2_ = in_list[2];
    // prepare evaluate lambda
    data_count_ = const_cast<double*>(in_count_->data()->GetValues<double>(1));
    data_avg_ = const_cast<double*>(in_avg_->data()->GetValues<double>(1));
    data_m2_ = const_cast<double*>(in_m2_->data()->GetValues<double>(1));
    row_id = 0;
    if (in_m2_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_m2_->IsNull(row_id);
        if (!is_null && data_count_[row_id] > 0) {
          cache_validity_[dest_group_id] = true;
          double pre_avg = cache_avg_[dest_group_id];
          double delta = data_avg_[row_id] - pre_avg;
          double n1 = cache_count_[dest_group_id];
          double n2 = data_count_[row_id];
          double deltaN = (n1 + n2) > 0 ? delta / (n1 + n2) : 0;
          cache_m2_[dest_group_id] += (data_m2_[row_id] + delta * deltaN * n1 * n2);
          cache_avg_[dest_group_id] += deltaN * n2;
          cache_count_[dest_group_id] += n2;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (data_count_[row_id] > 0) {
           cache_validity_[dest_group_id] = true;
           double pre_avg = cache_avg_[dest_group_id];
           double delta = data_avg_[row_id] - pre_avg;
           double n1 = cache_count_[dest_group_id];
           double n2 = data_count_[row_id];
           double deltaN = (n1 + n2) > 0 ? delta / (n1 + n2) : 0;
           cache_m2_[dest_group_id] += (data_m2_[row_id] + delta * deltaN * n1 * n2);
           cache_avg_[dest_group_id] += deltaN * n2;
           cache_count_[dest_group_id] += n2;
        }
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

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < cache_count_.size(); i++) {
      cache_m2_[i] = sqrt(cache_m2_[i] / (cache_count_[i] > 1 ? 
        (cache_count_[i] - 1) : 1));
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_m2_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

  uint64_t GetResultLength() { return cache_count_.size(); }

  arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out) override {
    for (int i = 0; i < length; i++) {
      cache_m2_[i + offset] = sqrt(cache_m2_[i + offset] / 
        (cache_count_[i + offset] > 1 ? (cache_count_[i + offset] - 1) : 1));
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    for (uint64_t i = 0; i < length; i++) {
      if (cache_count_[offset + i] - 1 < 0.00001) {
        // append Infinity if only one non-null value exists
        // RETURN_NOT_OK(builder->Append(std::numeric_limits<double>::quiet_NaN()));
        RETURN_NOT_OK(builder->Append(std::numeric_limits<double>::infinity()));
      } else if (cache_validity_[offset + i]) {
        RETURN_NOT_OK(builder->Append(cache_m2_[offset + i]));
      } else {
        // append null if all values are null 
        RETURN_NOT_OK(builder->AppendNull());
      }
    }

    std::shared_ptr<arrow::Array> out_array;
    RETURN_NOT_OK(builder->Finish(&out_array));
    out->push_back(out_array);
    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  double* data_count_;
  double* data_avg_;
  double* data_m2_;
  int row_id;
  std::shared_ptr<arrow::Array> in_count_;
  std::shared_ptr<arrow::Array> in_avg_;
  std::shared_ptr<arrow::Array> in_m2_;
  // result
  std::vector<double> cache_count_;
  std::vector<double> cache_avg_;
  std::vector<double> cache_m2_;
  std::vector<bool> cache_validity_;
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

arrow::Status MakeUniqueAction(arrow::compute::FunctionContext *ctx,
                               std::shared_ptr<arrow::DataType> type,
                               std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                                   \
  case InType::type_id: {                                                 \
    using CType = typename arrow::TypeTraits<InType>::CType;              \
    auto action_ptr = std::make_shared<UniqueAction<InType, CType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);             \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::StringType::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::StringType, std::string>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    case arrow::Date32Type::type_id: {
      auto action_ptr =
          std::make_shared<UniqueAction<arrow::Date32Type, int32_t>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;

    default: {
      std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                << std::endl;
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::FunctionContext *ctx, std::shared_ptr<ActionBase> *out) {
  auto action_ptr = std::make_shared<CountAction<arrow::UInt64Type>>(ctx);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeCountLiteralAction(arrow::compute::FunctionContext *ctx, int arg, std::shared_ptr<ActionBase> *out) {
  auto action_ptr = std::make_shared<CountLiteralAction<arrow::UInt64Type>>(ctx, arg);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<SumAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgAction(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<AvgAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeMinAction(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<MinAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeMaxAction(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<MaxAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountAction(arrow::compute::FunctionContext *ctx,
                                 std::shared_ptr<arrow::DataType> type,
                                 std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                              \
  case InType::type_id: {                                            \
    auto action_ptr = std::make_shared<SumCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountMergeAction(arrow::compute::FunctionContext *ctx,
                                      std::shared_ptr<arrow::DataType> type,
                                      std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                              \
  case InType::type_id: {                                            \
    auto action_ptr = std::make_shared<SumCountMergeAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgByCountAction(arrow::compute::FunctionContext *ctx,
                                   std::shared_ptr<arrow::DataType> type,
                                   std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                                \
  case InType::type_id: {                                              \
    auto action_ptr = std::make_shared<AvgByCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeStddevSampPartialAction(arrow::compute::FunctionContext *ctx,
                                          std::shared_ptr<arrow::DataType> type,
                                          std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                                \
  case InType::type_id: {                                              \
    auto action_ptr = std::make_shared<StddevSampPartialAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeStddevSampFinalAction(arrow::compute::FunctionContext *ctx,
                                        std::shared_ptr<arrow::DataType> type,
                                        std::shared_ptr<ActionBase> *out) {
  switch (type->id()) {
#define PROCESS(InType)                                                \
  case InType::type_id: {                                              \
    auto action_ptr = std::make_shared<StddevSampFinalAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
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
