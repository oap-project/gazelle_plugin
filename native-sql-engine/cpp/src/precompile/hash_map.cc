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
#include "precompile/hash_map.h"

#include <arrow/compute/api.h>
#include <arrow/status.h>

#include <iostream>

#include "third_party/arrow/utils/hashing.h"
#include "third_party/sparsehash/sparse_hash_map.h"

namespace sparkcolumnarplugin {
namespace precompile {

#define TYPED_ARROW_HASH_MAP_IMPL(HASHMAPNAME, TYPENAME, TYPE, MEMOTABLETYPE)          \
  using MEMOTABLETYPE =                                                                \
      typename arrow::internal::HashTraits<arrow::TYPENAME>::MemoTableType;            \
  class HASHMAPNAME::Impl : public MEMOTABLETYPE {                                     \
   public:                                                                             \
    Impl(arrow::MemoryPool* pool) : MEMOTABLETYPE(pool, 128) {}                        \
  };                                                                                   \
                                                                                       \
  HASHMAPNAME::HASHMAPNAME(arrow::MemoryPool* pool) {                                  \
    impl_ = std::make_shared<Impl>(pool);                                              \
  }                                                                                    \
  arrow::Status HASHMAPNAME::GetOrInsert(const TYPE& value, void (*on_found)(int32_t), \
                                         void (*on_not_found)(int32_t),                \
                                         int32_t* out_memo_index) {                    \
    return impl_->GetOrInsert(value, on_found, on_not_found, out_memo_index);          \
  }                                                                                    \
  int32_t HASHMAPNAME::GetOrInsertNull(void (*on_found)(int32_t),                      \
                                       void (*on_not_found)(int32_t)) {                \
    return impl_->GetOrInsertNull(on_found, on_not_found);                             \
  }                                                                                    \
  int32_t HASHMAPNAME::Size() { return impl_->size(); }                                \
  int32_t HASHMAPNAME::Get(const TYPE& value) { return impl_->Get(value); }            \
  int32_t HASHMAPNAME::GetNull() { return impl_->GetNull(); }

#define TYPED_ARROW_HASH_MAP_DECIMAL_IMPL(HASHMAPNAME, TYPENAME, TYPE, MEMOTABLETYPE)  \
  using MEMOTABLETYPE =                                                                \
      typename arrow::internal::HashTraits<arrow::TYPENAME>::MemoTableType;            \
  class HASHMAPNAME::Impl : public MEMOTABLETYPE {                                     \
   public:                                                                             \
    Impl(arrow::MemoryPool* pool) : MEMOTABLETYPE(pool) {}                             \
  };                                                                                   \
                                                                                       \
  HASHMAPNAME::HASHMAPNAME(arrow::MemoryPool* pool) {                                  \
    impl_ = std::make_shared<Impl>(pool);                                              \
  }                                                                                    \
  arrow::Status HASHMAPNAME::GetOrInsert(const TYPE& value, void (*on_found)(int32_t), \
                                         void (*on_not_found)(int32_t),                \
                                         int32_t* out_memo_index) {                    \
    return impl_->GetOrInsert(&value, 16, on_found, on_not_found, out_memo_index);     \
  }                                                                                    \
  int32_t HASHMAPNAME::GetOrInsertNull(void (*on_found)(int32_t),                      \
                                       void (*on_not_found)(int32_t)) {                \
    return impl_->GetOrInsertNull(on_found, on_not_found);                             \
  }                                                                                    \
  int32_t HASHMAPNAME::Get(const TYPE& value) { return impl_->Get(&value, 16); }       \
  int32_t HASHMAPNAME::GetNull() { return impl_->GetNull(); }

TYPED_ARROW_HASH_MAP_IMPL(Int32HashMap, Int32Type, int32_t, Int32MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(Int64HashMap, Int64Type, int64_t, Int64MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(UInt32HashMap, UInt32Type, uint32_t, UInt32MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(UInt64HashMap, UInt64Type, uint64_t, UInt64MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(FloatHashMap, FloatType, float, FloatMemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(DoubleHashMap, DoubleType, double, DoubleMemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(Date32HashMap, Date32Type, int32_t, Date32MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(Date64HashMap, Date64Type, int64_t, Date64MemoTableType)
TYPED_ARROW_HASH_MAP_IMPL(StringHashMap, StringType, arrow::util::string_view,
                          StringMemoTableType)
TYPED_ARROW_HASH_MAP_DECIMAL_IMPL(Decimal128HashMap, Decimal128Type, arrow::Decimal128,
                                  DecimalMemoTableType)
#undef TYPED_ARROW_HASH_MAP_IMPL
#undef TYPED_ARROW_HASH_MAP_DECIMAL_IMPL
}  // namespace precompile
}  // namespace sparkcolumnarplugin
