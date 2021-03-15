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
#include "precompile/vector.h"

#include <cstdint>
#include <vector>

namespace sparkcolumnarplugin {
namespace precompile {

#define TYPED_VECTOR_IMPL(TYPENAME, TYPE)                                 \
  class TYPENAME::Impl : public std::vector<TYPE> {                       \
   public:                                                                \
    using std::vector<TYPE>::vector;                                      \
    using std::vector<TYPE>::operator=;                                   \
    Impl(Impl const&) = default;                                          \
    Impl(Impl&&) = default;                                               \
  };                                                                      \
                                                                          \
  TYPENAME::TYPENAME() { impl_ = std::make_shared<Impl>(); }              \
  void TYPENAME::push_back(const TYPE value) { impl_->push_back(value); } \
  TYPE& TYPENAME::operator[](uint32_t i) { return impl_->operator[](i); } \
  uint64_t TYPENAME::size() { return impl_->size(); }

TYPED_VECTOR_IMPL(Int32Vector, int32_t)
TYPED_VECTOR_IMPL(Int64Vector, int64_t)
TYPED_VECTOR_IMPL(UInt32Vector, uint32_t)
TYPED_VECTOR_IMPL(UInt64Vector, uint64_t)
TYPED_VECTOR_IMPL(FloatVector, float)
TYPED_VECTOR_IMPL(DoubleVector, double)
TYPED_VECTOR_IMPL(StringVector, std::string)
#undef TYPED_VECTOR_IMPL

}  // namespace precompile
}  // namespace sparkcolumnarplugin
