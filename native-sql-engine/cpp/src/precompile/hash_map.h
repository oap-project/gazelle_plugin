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
#include <arrow/util/string_view.h>

namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_HASH_MAP_DEFINE(TYPENAME, TYPE)                                          \
  class TYPENAME {                                                                     \
   public:                                                                             \
    TYPENAME(arrow::MemoryPool* pool);                                                 \
    arrow::Status GetOrInsert(const TYPE& value, void (*on_found)(int32_t),            \
                              void (*on_not_found)(int32_t), int32_t* out_memo_index); \
    int32_t GetOrInsertNull(void (*on_found)(int32_t), void (*on_not_found)(int32_t)); \
    int32_t Get(const TYPE& value);                                                    \
    int32_t GetNull();                                                                 \
                                                                                       \
   private:                                                                            \
    class Impl;                                                                        \
    std::shared_ptr<Impl> impl_;                                                       \
  };

TYPED_HASH_MAP_DEFINE(Int32HashMap, int32_t)
TYPED_HASH_MAP_DEFINE(Int64HashMap, int64_t)
TYPED_HASH_MAP_DEFINE(UInt32HashMap, uint32_t)
TYPED_HASH_MAP_DEFINE(UInt64HashMap, uint64_t)
TYPED_HASH_MAP_DEFINE(FloatHashMap, float)
TYPED_HASH_MAP_DEFINE(DoubleHashMap, double)
TYPED_HASH_MAP_DEFINE(Date32HashMap, int32_t)
TYPED_HASH_MAP_DEFINE(Date64HashMap, int64_t)
TYPED_HASH_MAP_DEFINE(StringHashMap, arrow::util::string_view)
TYPED_HASH_MAP_DEFINE(Decimal128HashMap, arrow::Decimal128)
TYPED_HASH_MAP_DEFINE(TimestampHashMap, int64_t)
#undef TYPED_HASH_MAP_DEFINE
}  // namespace precompile
}  // namespace sparkcolumnarplugin
