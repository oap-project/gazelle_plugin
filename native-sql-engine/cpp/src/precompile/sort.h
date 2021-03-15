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
#include <cstdint>
#include <functional>
#include <string>

#include "codegen/arrow_compute/ext/array_item_index.h"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_ASC_SORT_DEFINE(CTYPE) \
  void sort_asc(ArrayItemIndex*, ArrayItemIndex*, std::function<CTYPE(ArrayItemIndex)>);

TYPED_ASC_SORT_DEFINE(int32_t)
TYPED_ASC_SORT_DEFINE(uint32_t)
TYPED_ASC_SORT_DEFINE(int64_t)
TYPED_ASC_SORT_DEFINE(uint64_t)
TYPED_ASC_SORT_DEFINE(float)
TYPED_ASC_SORT_DEFINE(double)
TYPED_ASC_SORT_DEFINE(std::string)

void sort_desc(ArrayItemIndex*, ArrayItemIndex*,
               std::function<bool(ArrayItemIndex, ArrayItemIndex)>);

}  // namespace precompile
}  // namespace sparkcolumnarplugin
