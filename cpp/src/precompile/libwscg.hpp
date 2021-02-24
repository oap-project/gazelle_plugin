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

#include <arrow/compute/context.h>
#include <arrow/record_batch.h>

#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "precompile/array.h"
using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;

#include "precompile/builder.h"
#include "utils/macros.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/hash_relation.h"
#include "codegen/arrow_compute/ext/actions_impl.h"
#include "precompile/hash_map.h"
#include "precompile/sparse_hash_map.h"
#include "codegen/common/sort_relation.h"
#include "third_party/row_wise_memory/unsafe_row.h"

#include "precompile/builder.h"
#include "precompile/type.h"
#include "third_party/ska_sort.hpp"
#include "third_party/timsort.hpp"


#include <arrow/buffer.h>

#include <algorithm>
#include <cmath>
