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
    arrow::Status Reserve(int64_t);                           \
    arrow::Status AppendNulls(int64_t);                       \
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
TYPED_BUILDER_DEFINE(Date64Builder, int64_t)

class StringBuilder {
 public:
  StringBuilder(arrow::MemoryPool* pool);
  arrow::Status Append(arrow::util::string_view val);
  arrow::Status AppendString(std::string val);
  arrow::Status AppendNull();
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out);
  arrow::Status Reset();

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

class Decimal128Builder {
 public:
  Decimal128Builder(std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool);
  arrow::Status Append(arrow::Decimal128 val);
  arrow::Status AppendNull();
  arrow::Status Reserve(int64_t);
  arrow::Status AppendNulls(int64_t);
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out);
  arrow::Status Reset();

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
}  // namespace precompile
}  // namespace sparkcolumnarplugin
