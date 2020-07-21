#include "precompile/builder.h"

#include <arrow/builder.h>
#include <arrow/compute/context.h>

#include <iostream>

namespace sparkcolumnarplugin {
namespace precompile {

#define TYPED_BUILDER_IMPL(TYPENAME, TYPE, CTYPE)                                       \
  class TYPENAME::Impl : public arrow::TYPENAME {                                       \
   public:                                                                              \
    Impl(arrow::MemoryPool* pool) : arrow::TYPENAME(TYPE, pool) {}                      \
  };                                                                                    \
                                                                                        \
  TYPENAME::TYPENAME(arrow::MemoryPool* pool) { impl_ = std::make_shared<Impl>(pool); } \
  arrow::Status TYPENAME::Append(CTYPE value) { return impl_->Append(value); }          \
  arrow::Status TYPENAME::AppendNull() { return impl_->AppendNull(); }                  \
  arrow::Status TYPENAME::Finish(std::shared_ptr<arrow::Array>* out) {                  \
    return impl_->Finish(out);                                                          \
  }                                                                                     \
  arrow::Status TYPENAME::Reset() {                                                     \
    impl_->Reset();                                                                     \
    return arrow::Status::OK();                                                         \
  }

TYPED_BUILDER_IMPL(BooleanBuilder, arrow::boolean(), bool)
TYPED_BUILDER_IMPL(Int8Builder, arrow::int8(), int8_t)
TYPED_BUILDER_IMPL(Int16Builder, arrow::int16(), int16_t)
TYPED_BUILDER_IMPL(UInt8Builder, arrow::uint8(), uint8_t)
TYPED_BUILDER_IMPL(UInt16Builder, arrow::uint16(), uint16_t)
TYPED_BUILDER_IMPL(Int32Builder, arrow::int32(), int32_t)
TYPED_BUILDER_IMPL(Int64Builder, arrow::int64(), int64_t)
TYPED_BUILDER_IMPL(UInt32Builder, arrow::uint32(), uint32_t)
TYPED_BUILDER_IMPL(UInt64Builder, arrow::uint64(), uint64_t)
TYPED_BUILDER_IMPL(FloatBuilder, arrow::float32(), float)
TYPED_BUILDER_IMPL(DoubleBuilder, arrow::float64(), double)
TYPED_BUILDER_IMPL(Date32Builder, arrow::date32(), int32_t)
TYPED_BUILDER_IMPL(StringBuilder, arrow::utf8(), arrow::util::string_view)
#undef TYPED_BUILDER_IMPL

}  // namespace precompile
}  // namespace sparkcolumnarplugin
