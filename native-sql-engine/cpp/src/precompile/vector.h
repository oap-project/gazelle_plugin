#pragma once
#include <memory>

namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_VECTOR_DEFINE(TYPENAME, TYPE) \
  class TYPENAME {                          \
   public:                                  \
    TYPENAME();                             \
    void push_back(const TYPE);             \
    TYPE& operator[](uint32_t);             \
    uint64_t size();                        \
                                            \
   private:                                 \
    class Impl;                             \
    std::shared_ptr<Impl> impl_;            \
  };

TYPED_VECTOR_DEFINE(Int32Vector, int32_t)
TYPED_VECTOR_DEFINE(Int64Vector, int64_t)
TYPED_VECTOR_DEFINE(UInt32Vector, uint32_t)
TYPED_VECTOR_DEFINE(UInt64Vector, uint64_t)
TYPED_VECTOR_DEFINE(FloatVector, float)
TYPED_VECTOR_DEFINE(DoubleVector, double)
TYPED_VECTOR_DEFINE(StringVector, std::string)
#undef TYPED_VECTOR_DEFINE
}  // namespace precompile
}  // namespace sparkcolumnarplugin