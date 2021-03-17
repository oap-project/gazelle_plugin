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
#include "precompile/sort.h"

#include "third_party/ska_sort.hpp"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_ASC_SORT_IMPL(CTYPE)                                  \
  void sort_asc(ArrayItemIndex* begin, ArrayItemIndex* end,         \
                std::function<CTYPE(ArrayItemIndex)> extract_key) { \
    ska_sort(begin, end, extract_key);                              \
  }

TYPED_ASC_SORT_IMPL(int32_t)
TYPED_ASC_SORT_IMPL(uint32_t)
TYPED_ASC_SORT_IMPL(int64_t)
TYPED_ASC_SORT_IMPL(uint64_t)
TYPED_ASC_SORT_IMPL(float)
TYPED_ASC_SORT_IMPL(double)
TYPED_ASC_SORT_IMPL(std::string)

void sort_desc(ArrayItemIndex* begin, ArrayItemIndex* end,
               std::function<bool(ArrayItemIndex, ArrayItemIndex)> comp) {
  // std::sort(begin, end, *comp.target<bool (*)(ArrayItemIndex,
  // ArrayItemIndex)>());
  std::sort(begin, end, comp);
}
}  // namespace precompile
}  // namespace sparkcolumnarplugin
