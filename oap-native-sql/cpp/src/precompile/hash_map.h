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
TYPED_HASH_MAP_DEFINE(StringHashMap, arrow::util::string_view)
#undef TYPED_HASH_MAP_DEFINE
}  // namespace precompile
}  // namespace sparkcolumnarplugin