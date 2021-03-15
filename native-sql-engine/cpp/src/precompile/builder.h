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
#include <arrow/type_fwd.h>

#include "arrow/util/string_view.h"  // IWYU pragma: export
namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_BUILDER_DEFINE(TYPENAME, TYPE)                  \
  class TYPENAME {                                            \
   public:                                                    \
    TYPENAME(arrow::MemoryPool* pool);                        \
    arrow::Status Append(TYPE val);                           \
    arrow::Status AppendNull();                               \
    arrow::Status Reserve(int64_t);                           \
    arrow::Status AppendNulls(int64_t);                       \
    arrow::Status Finish(std::shared_ptr<arrow::Array>* out); \
    arrow::Status Reset();                                    \
                                                              \
   private:                                                   \
    class Impl;                                               \
    std::shared_ptr<Impl> impl_;                              \
  };

TYPED_BUILDER_DEFINE(BooleanBuilder, bool)
TYPED_BUILDER_DEFINE(Int8Builder, int8_t)
TYPED_BUILDER_DEFINE(Int16Builder, int16_t)
TYPED_BUILDER_DEFINE(UInt8Builder, uint8_t)
TYPED_BUILDER_DEFINE(UInt16Builder, uint16_t)
TYPED_BUILDER_DEFINE(Int32Builder, int32_t)
TYPED_BUILDER_DEFINE(Int64Builder, int64_t)
TYPED_BUILDER_DEFINE(UInt32Builder, uint32_t)
TYPED_BUILDER_DEFINE(UInt64Builder, uint64_t)
TYPED_BUILDER_DEFINE(FloatBuilder, float)
TYPED_BUILDER_DEFINE(DoubleBuilder, double)
TYPED_BUILDER_DEFINE(Date32Builder, int32_t)
TYPED_BUILDER_DEFINE(Date64Builder, int64_t)

class StringBuilder {
 public:
  StringBuilder(arrow::MemoryPool* pool);
  arrow::Status Append(arrow::util::string_view val);
  arrow::Status AppendString(std::string val);
  arrow::Status AppendNull();
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out);
  arrow::Status Reset();

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

class Decimal128Builder {
 public:
  Decimal128Builder(std::shared_ptr<arrow::DataType> type,
                    arrow::MemoryPool* pool);
  arrow::Status Append(arrow::Decimal128 val);
  arrow::Status AppendNull();
  arrow::Status Reserve(int64_t);
  arrow::Status AppendNulls(int64_t);
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out);
  arrow::Status Reset();

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
}  // namespace precompile
}  // namespace sparkcolumnarplugin
