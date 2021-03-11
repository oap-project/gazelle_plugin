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
