#pragma once
#include <arrow/type_fwd.h>

#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_BUILDER_DEFINE(TYPENAME, TYPE)                  \
  class TYPENAME {                                            \
   public:                                                    \
    TYPENAME(arrow::MemoryPool* pool);                        \
    arrow::Status Append(TYPE val);                           \
    arrow::Status AppendNull();                               \
    arrow::Status Finish(std::shared_ptr<arrow::Array>* out); \
    arrow::Status Reset();                                    \
                                                              \
   private:                                                   \
    class Impl;                                               \
    std::shared_ptr<Impl> impl_;                              \
  };

TYPED_BUILDER_DEFINE(BooleanBuilder, bool)
TYPED_BUILDER_DEFINE(Int8Builder, int8_t)
TYPED_BUILDER_DEFINE(Int16Builder, int16_t)
TYPED_BUILDER_DEFINE(UInt8Builder, uint8_t)
TYPED_BUILDER_DEFINE(UInt16Builder, uint16_t)
TYPED_BUILDER_DEFINE(Int32Builder, int32_t)
TYPED_BUILDER_DEFINE(Int64Builder, int64_t)
TYPED_BUILDER_DEFINE(UInt32Builder, uint32_t)
TYPED_BUILDER_DEFINE(UInt64Builder, uint64_t)
TYPED_BUILDER_DEFINE(FloatBuilder, float)
TYPED_BUILDER_DEFINE(DoubleBuilder, double)
TYPED_BUILDER_DEFINE(Date32Builder, int32_t)
TYPED_BUILDER_DEFINE(StringBuilder, arrow::util::string_view)
#undef TYPED_BUILDER_DEFINE
}  // namespace precompile
}  // namespace sparkcolumnarplugin