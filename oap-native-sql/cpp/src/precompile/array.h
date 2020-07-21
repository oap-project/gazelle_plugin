#pragma once

#include <arrow/type_fwd.h>

#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace sparkcolumnarplugin {
namespace precompile {

class Array {
 public:
  Array(const std::shared_ptr<arrow::Array>&);
  bool IsNull(int64_t i) const {
    i += offset_;
    return null_bitmap_data_ != NULLPTR &&
           !((null_bitmap_data_[i >> 3] >> (i & 0x07)) & 1);
  }
  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  const uint8_t* value_data() const { return raw_value_; }

 private:
  std::shared_ptr<arrow::Array> cache_;
  const uint8_t* raw_value_;
  const uint8_t* null_bitmap_data_;
  uint64_t offset_;
  uint64_t length_;
  uint64_t null_count_;
};

class BooleanArray {
 public:
  BooleanArray(const std::shared_ptr<arrow::Array>&);
  bool GetView(int64_t i) const {
    auto bits = reinterpret_cast<const uint8_t*>(raw_value_);
    i += offset_;
    return (bits[i >> 3] >> (i & 0x07)) & 1;
  }
  bool IsNull(int64_t i) const {
    i += offset_;
    return null_bitmap_data_ != NULLPTR &&
           !((null_bitmap_data_[i >> 3] >> (i & 0x07)) & 1);
  }
  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }

 private:
  std::shared_ptr<arrow::Array> cache_;
  const uint8_t* raw_value_;
  const uint8_t* null_bitmap_data_;
  uint64_t offset_;
  uint64_t length_;
  uint64_t null_count_;
};

#define TYPED_ARRAY_DEFINE(TYPENAME, TYPE)                     \
  class TYPENAME {                                             \
   public:                                                     \
    TYPENAME(const std::shared_ptr<arrow::Array>&);            \
    TYPE GetView(int64_t i) const { return raw_value_[i]; }    \
    bool IsNull(int64_t i) const {                             \
      i += offset_;                                            \
      return null_bitmap_data_ != NULLPTR &&                   \
             !((null_bitmap_data_[i >> 3] >> (i & 0x07)) & 1); \
    }                                                          \
    int64_t length() const { return length_; }                 \
    int64_t null_count() const { return null_count_; }         \
    const TYPE* value_data() const { return raw_value_; }      \
                                                               \
   private:                                                    \
    std::shared_ptr<arrow::Array> cache_;                      \
    const TYPE* raw_value_;                                    \
    const uint8_t* null_bitmap_data_;                          \
    uint64_t offset_;                                          \
    uint64_t length_;                                          \
    uint64_t null_count_;                                      \
  };

TYPED_ARRAY_DEFINE(Int32Array, int32_t)
TYPED_ARRAY_DEFINE(Int64Array, int64_t)
TYPED_ARRAY_DEFINE(UInt32Array, uint32_t)
TYPED_ARRAY_DEFINE(UInt64Array, uint64_t)
TYPED_ARRAY_DEFINE(FloatArray, float)
TYPED_ARRAY_DEFINE(DoubleArray, double)
TYPED_ARRAY_DEFINE(Date32Array, int32_t)
TYPED_ARRAY_DEFINE(FixedSizeBinaryArray, uint8_t)
#undef TYPED_ARRAY_DEFINE

#define TYPED_BINARY_ARRAY_DEFINE(TYPENAME, TYPE)                                      \
  class TYPENAME {                                                                     \
   public:                                                                             \
    TYPENAME(const std::shared_ptr<arrow::Array>&);                                    \
    TYPE GetString(int64_t i) const { return std::string(GetView(i)); }                \
    arrow::util::string_view GetView(int64_t i) const {                                \
      i += offset_;                                                                    \
      const offset_type pos = raw_value_offsets_[i];                                   \
      return arrow::util::string_view(reinterpret_cast<const char*>(raw_value_ + pos), \
                                      raw_value_offsets_[i + 1] - pos);                \
    }                                                                                  \
    bool IsNull(int64_t i) const {                                                     \
      i += offset_;                                                                    \
      return null_bitmap_data_ != NULLPTR &&                                           \
             !((null_bitmap_data_[i >> 3] >> (i & 0x07)) & 1);                         \
    }                                                                                  \
    int64_t length() const { return length_; }                                         \
    int64_t null_count() const { return null_count_; }                                 \
                                                                                       \
   private:                                                                            \
    using offset_type = int32_t;                                                       \
    std::shared_ptr<arrow::Array> cache_;                                              \
    const uint8_t* raw_value_;                                                         \
    const int32_t* raw_value_offsets_;                                                 \
    const uint8_t* null_bitmap_data_;                                                  \
    int64_t offset_;                                                                   \
    uint64_t length_;                                                                  \
    uint64_t null_count_;                                                              \
  };
TYPED_BINARY_ARRAY_DEFINE(StringArray, std::string)
#undef TYPED_BINARY_ARRAY_DEFINE

arrow::Status MakeFixedSizeBinaryArray(const std::shared_ptr<arrow::FixedSizeBinaryType>&,
                                       int64_t, const std::shared_ptr<arrow::Buffer>&,
                                       std::shared_ptr<FixedSizeBinaryArray>*);

}  // namespace precompile
}  // namespace sparkcolumnarplugin