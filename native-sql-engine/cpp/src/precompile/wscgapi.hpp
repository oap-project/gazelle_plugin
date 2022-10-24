#pragma once

#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include <algorithm>
#include <cmath>
#include <charconv>
#include <numeric>
#include <tuple>

#include "codegen/arrow_compute/ext/actions_impl.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/common/hash_relation.h"
#include "codegen/common/sort_relation.h"
#include "precompile/array.h"
#include "precompile/builder.h"
#include "precompile/gandiva.h"
#include "precompile/gandiva_projector.h"
#include "precompile/hash_map.h"
#include "precompile/sparse_hash_map.h"
#include "precompile/type.h"
#include "third_party/row_wise_memory/unsafe_row.h"
#include "third_party/ska_sort.hpp"
#include "third_party/timsort.hpp"
#include "utils/macros.h"
using namespace sparkcolumnarplugin::precompile;

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
