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
#include <memory>

namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_VECTOR_DEFINE(TYPENAME, TYPE) \
  class TYPENAME {                          \
   public:                                  \
    TYPENAME();                             \
    void push_back(const TYPE);             \
    TYPE& operator[](uint32_t);             \
    uint64_t size();                        \
                                            \
   private:                                 \
    class Impl;                             \
    std::shared_ptr<Impl> impl_;            \
  };

TYPED_VECTOR_DEFINE(Int32Vector, int32_t)
TYPED_VECTOR_DEFINE(Int64Vector, int64_t)
TYPED_VECTOR_DEFINE(UInt32Vector, uint32_t)
TYPED_VECTOR_DEFINE(UInt64Vector, uint64_t)
TYPED_VECTOR_DEFINE(FloatVector, float)
TYPED_VECTOR_DEFINE(DoubleVector, double)
TYPED_VECTOR_DEFINE(StringVector, std::string)
#undef TYPED_VECTOR_DEFINE
}  // namespace precompile
}  // namespace sparkcolumnarplugin
