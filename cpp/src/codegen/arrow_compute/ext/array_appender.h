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

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

class AppenderBase {
 public:
  virtual ~AppenderBase() {}

  enum AppenderType { left, right, exist };
  virtual AppenderType GetType() { return left; }

  virtual arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    return arrow::Status::NotImplemented("AppenderBase AddArray is abstract.");
  }

  virtual arrow::Status PopArray() {
    return arrow::Status::NotImplemented("AppenderBase PopArray is abstract.");
  }

  virtual arrow::Status Append(const uint16_t& array_id, const uint16_t& item_id) {
    return arrow::Status::NotImplemented("AppenderBase Append is abstract.");
  }

  virtual arrow::Status AppendNull() {
    return arrow::Status::NotImplemented("AppenderBase AppendNull is abstract.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    return arrow::Status::NotImplemented("AppenderBase Finish is abstract.");
  }

  virtual arrow::Status Reset() {
    return arrow::Status::NotImplemented("AppenderBase Reset is abstract.");
  }

  virtual arrow::Status AppendExistence(bool is_exist) {
    return arrow::Status::NotImplemented("AppenderBase AppendExistence is abstract.");
  }
};

template <typename DataType, typename Enable = void>
class ArrayAppender {};

template <typename T>
using enable_if_not_boolean = std::enable_if_t<!arrow::is_boolean_type<T>::value>;

template <typename DataType>
class ArrayAppender<DataType, enable_if_not_boolean<DataType>> : public AppenderBase {
 public:
  ArrayAppender(arrow::compute::FunctionContext* ctx, AppenderType type = left)
      : ctx_(ctx), type_(type) {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType_*>(array_builder.release()));
  }
  ~ArrayAppender() {}

  AppenderType GetType() override { return type_; }
  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.emplace_back(typed_arr_);
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    return arrow::Status::OK();
  }

  arrow::Status Append(const uint16_t& array_id, const uint16_t& item_id) override {
    if (!cached_arr_[array_id]->IsNull(item_id)) {
      auto val = cached_arr_[array_id]->GetView(item_id);
      return builder_->Append(cached_arr_[array_id]->GetView(item_id));
    } else {
      return builder_->AppendNull();
    }
  }

  arrow::Status AppendNull() override { return builder_->AppendNull(); }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) override {
    return builder_->Finish(out_);
  }

  arrow::Status Reset() override {
    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using BuilderType_ = typename arrow::TypeTraits<DataType>::BuilderType;
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::unique_ptr<BuilderType_> builder_;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::FunctionContext* ctx_;
  AppenderType type_;
};

template <typename DataType>
class ArrayAppender<DataType, arrow::enable_if_boolean<DataType>> : public AppenderBase {
 public:
  ArrayAppender(arrow::compute::FunctionContext* ctx, AppenderType type = left)
      : ctx_(ctx), type_(type) {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType_*>(array_builder.release()));
  }
  ~ArrayAppender() {}

  AppenderType GetType() override { return type_; }
  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) override {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.emplace_back(typed_arr_);
    return arrow::Status::OK();
  }

  arrow::Status PopArray() override {
    cached_arr_.pop_back();
    return arrow::Status::OK();
  }

  arrow::Status Append(const uint16_t& array_id, const uint16_t& item_id) override {
    if (!cached_arr_[array_id]->IsNull(item_id)) {
      auto val = cached_arr_[array_id]->GetView(item_id);
      return builder_->Append(cached_arr_[array_id]->GetView(item_id));
    } else {
      return builder_->AppendNull();
    }
  }

  arrow::Status AppendNull() override { return builder_->AppendNull(); }

  arrow::Status AppendExistence(bool is_exist) { return builder_->Append(is_exist); }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) override {
    return builder_->Finish(out_);
  }

  arrow::Status Reset() override {
    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using BuilderType_ = typename arrow::TypeTraits<DataType>::BuilderType;
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::unique_ptr<BuilderType_> builder_;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::FunctionContext* ctx_;
  AppenderType type_;
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
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::StringType)
static arrow::Status MakeAppender(arrow::compute::FunctionContext* ctx,
                                  std::shared_ptr<arrow::DataType> type,
                                  AppenderBase::AppenderType appender_type,
                                  std::shared_ptr<AppenderBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    auto app_ptr = std::make_shared<ArrayAppender<InType>>(ctx, appender_type); \
    *out = std::dynamic_pointer_cast<AppenderBase>(app_ptr);                    \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default: {
      std::cout << "MakeAppender type not supported, type is " << type << std::endl;
    } break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
