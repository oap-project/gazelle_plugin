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
#include "precompile/builder.h"

#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/util/decimal.h>

#include <iostream>

namespace sparkcolumnarplugin {
namespace precompile {

#define TYPED_BUILDER_IMPL(TYPENAME, TYPE, CTYPE)                              \
  class TYPENAME::Impl : public arrow::TYPENAME {                              \
   public:                                                                     \
    Impl(arrow::MemoryPool* pool) : arrow::TYPENAME(TYPE, pool) {}             \
  };                                                                           \
                                                                               \
  TYPENAME::TYPENAME(arrow::MemoryPool* pool) {                                \
    impl_ = std::make_shared<Impl>(pool);                                      \
  }                                                                            \
  arrow::Status TYPENAME::Append(CTYPE value) { return impl_->Append(value); } \
  arrow::Status TYPENAME::AppendNull() { return impl_->AppendNull(); }         \
  arrow::Status TYPENAME::Reserve(int64_t length) {                            \
    return impl_->Reserve(length);                                             \
  }                                                                            \
  arrow::Status TYPENAME::AppendNulls(int64_t length) {                        \
    return impl_->AppendNulls(length);                                         \
  }                                                                            \
  arrow::Status TYPENAME::Finish(std::shared_ptr<arrow::Array>* out) {         \
    return impl_->Finish(out);                                                 \
  }                                                                            \
  arrow::Status TYPENAME::Reset() {                                            \
    impl_->Reset();                                                            \
    return arrow::Status::OK();                                                \
  }

TYPED_BUILDER_IMPL(BooleanBuilder, arrow::boolean(), bool)
TYPED_BUILDER_IMPL(Int8Builder, arrow::int8(), int8_t)
TYPED_BUILDER_IMPL(Int16Builder, arrow::int16(), int16_t)
TYPED_BUILDER_IMPL(UInt8Builder, arrow::uint8(), uint8_t)
TYPED_BUILDER_IMPL(UInt16Builder, arrow::uint16(), uint16_t)
TYPED_BUILDER_IMPL(Int32Builder, arrow::int32(), int32_t)
TYPED_BUILDER_IMPL(Int64Builder, arrow::int64(), int64_t)
TYPED_BUILDER_IMPL(UInt32Builder, arrow::uint32(), uint32_t)
TYPED_BUILDER_IMPL(UInt64Builder, arrow::uint64(), uint64_t)
TYPED_BUILDER_IMPL(FloatBuilder, arrow::float32(), float)
TYPED_BUILDER_IMPL(DoubleBuilder, arrow::float64(), double)
TYPED_BUILDER_IMPL(Date32Builder, arrow::date32(), int32_t)
TYPED_BUILDER_IMPL(Date64Builder, arrow::date64(), int64_t)
#undef TYPED_BUILDER_IMPL

class StringBuilder::Impl : public arrow::StringBuilder {
 public:
  Impl(arrow::MemoryPool* pool) : arrow::StringBuilder(arrow::utf8(), pool) {}
};

StringBuilder::StringBuilder(arrow::MemoryPool* pool) {
  impl_ = std::make_shared<Impl>(pool);
}
arrow::Status StringBuilder::Append(arrow::util::string_view value) {
  return impl_->Append(value);
}
arrow::Status StringBuilder::AppendString(std::string value) {
  return impl_->Append(arrow::util::string_view(value));
}
arrow::Status StringBuilder::AppendNull() { return impl_->AppendNull(); }
arrow::Status StringBuilder::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}
arrow::Status StringBuilder::Reset() {
  impl_->Reset();
  return arrow::Status::OK();
}

class Decimal128Builder::Impl : public arrow::Decimal128Builder {
 public:
  Impl(std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool)
      : arrow::Decimal128Builder(type, pool) {}
};

Decimal128Builder::Decimal128Builder(std::shared_ptr<arrow::DataType> type,
                                     arrow::MemoryPool* pool) {
  impl_ = std::make_shared<Impl>(type, pool);
}
arrow::Status Decimal128Builder::Append(arrow::Decimal128 value) {
  return impl_->Append(value);
}
arrow::Status Decimal128Builder::AppendNull() { return impl_->AppendNull(); }
arrow::Status Decimal128Builder::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}
arrow::Status Decimal128Builder::Reset() {
  impl_->Reset();
  return arrow::Status::OK();
}

}  // namespace precompile
}  // namespace sparkcolumnarplugin
