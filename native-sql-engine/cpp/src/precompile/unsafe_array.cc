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

#include "precompile/unsafe_array.h"

#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <iostream>

namespace sparkcolumnarplugin {
namespace precompile {

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
arrow::Status MakeUnsafeArray(std::shared_ptr<arrow::DataType> type, int idx,
                              const std::shared_ptr<arrow::Array>& in,
                              std::shared_ptr<UnsafeArray>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                            \
  case InType::type_id: {                                                          \
    auto typed_unsafe_array = std::make_shared<TypedUnsafeArray<InType>>(idx, in); \
    *out = std::dynamic_pointer_cast<UnsafeArray>(typed_unsafe_array);             \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default: {
      std::cout << "MakeUnsafeArray type not supported, type is " << type << std::endl;
    } break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES
}  // namespace precompile
}  // namespace sparkcolumnarplugin
