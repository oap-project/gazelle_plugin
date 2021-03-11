#pragma once
#include "third_party/sparsehash/sparse_hash_map.h"

extern template class SparseHashMap<int32_t>;
extern template class SparseHashMap<int64_t>;
extern template class SparseHashMap<uint32_t>;
extern template class SparseHashMap<uint64_t>;
extern template class SparseHashMap<float>;
extern template class SparseHashMap<double>;