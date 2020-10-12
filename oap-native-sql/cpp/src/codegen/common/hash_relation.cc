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

#include "codegen/common/hash_relation_number.h"
#include "codegen/common/hash_relation_string.h"

///////////////////////////////////////////////////////////////////////////////////
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
arrow::Status MakeHashRelationColumn(uint32_t data_type_id,
                                     std::shared_ptr<HashRelationColumn>* out) {
  switch (data_type_id) {
#define PROCESS(InType)                                                      \
  case TypeTraits<InType>::type_id: {                                        \
    auto typed_column = std::make_shared<TypedHashRelationColumn<InType>>(); \
    *out = std::dynamic_pointer_cast<HashRelationColumn>(typed_column);      \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default: {
      return arrow::Status::NotImplemented("MakeHashRelationColumn doesn't suppoty type ",
                                           data_type_id);
    } break;
  }

  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

/////////////////////////////////////////////////////////////////////////

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
arrow::Status MakeHashRelation(
    uint32_t key_type_id, arrow::compute::FunctionContext* ctx,
    const std::vector<std::shared_ptr<HashRelationColumn>>& hash_relation_column,
    std::shared_ptr<HashRelation>* out) {
  switch (key_type_id) {
#define PROCESS(InType)                                                         \
  case TypeTraits<InType>::type_id: {                                           \
    auto typed_hash_relation =                                                  \
        std::make_shared<TypedHashRelation<InType>>(ctx, hash_relation_column); \
    *out = std::dynamic_pointer_cast<HashRelation>(typed_hash_relation);        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default: {
      return arrow::Status::NotImplemented("MakeHashRelation doesn't suppoty type ",
                                           key_type_id);
    } break;
  }

  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES