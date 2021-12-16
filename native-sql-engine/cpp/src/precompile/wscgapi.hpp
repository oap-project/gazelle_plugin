#pragma once

#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "precompile/array.h"

#include "precompile/builder.h"
#include "utils/macros.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/hash_relation.h"
#include "codegen/arrow_compute/ext/actions_impl.h"
#include "precompile/hash_map.h"
#include "precompile/sparse_hash_map.h"
#include "codegen/common/sort_relation.h"
#include "third_party/row_wise_memory/unsafe_row.h"

#include "precompile/type.h"
#include "third_party/ska_sort.hpp"
#include "third_party/timsort.hpp"
#include "precompile/gandiva.h"

#include <arrow/buffer.h>

#include <algorithm>
#include <cmath>

#include <tuple>
#include <numeric>
using namespace sparkcolumnarplugin::precompile;

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
