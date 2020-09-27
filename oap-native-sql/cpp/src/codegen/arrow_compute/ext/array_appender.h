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

#include <cstdint>
#include <vector>
#include <arrow/status.h>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

class AppenderBase {
  public:
  virtual ~AppenderBase() {}

  virtual arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr)  {
    return arrow::Status::NotImplemented("AppenderBase AddArray is abstract.");
  }

  virtual arrow::Status Append(uint16_t& array_id, uint16_t& item_id) {
    return arrow::Status::NotImplemented("AppenderBase Append is abstract.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    return arrow::Status::NotImplemented("AppenderBase Finish is abstract.");
  }

  virtual arrow::Status Reset() {
    return arrow::Status::NotImplemented("AppenderBase Reset is abstract.");
  }
};

template <class DataType>
class ArrayAppender : public AppenderBase {
  public:
  ArrayAppender(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(
        ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<BuilderType_*>(array_builder.release()));
  }
  ~ArrayAppender() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr)  {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.emplace_back(typed_arr_);
    return arrow::Status::OK();
  }

  arrow::Status Append(uint16_t& array_id, uint16_t& item_id) {
    if (!cached_arr_[array_id]->IsNull(item_id)) {
      auto val = cached_arr_[array_id]->GetView(item_id);
      builder_->Append(cached_arr_[array_id]->GetView(item_id));
    } else {
      builder_->AppendNull();
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    builder_->Finish(out_);
    return arrow::Status::OK();
  }

  arrow::Status Reset() {
    builder_->Reset();
    return arrow::Status::OK();
  }
                                                           
  private:
  using BuilderType_ = typename arrow::TypeTraits<DataType>::BuilderType;
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::unique_ptr<BuilderType_> builder_;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::FunctionContext* ctx_;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
