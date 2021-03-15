/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <arrow/type_fwd.h>
#include <arrow/util/decimal.h>
#include <arrow/util/string_view.h>  // IWYU pragma: export

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

  std::shared_ptr<arrow::Array> cache_;

 private:
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

  std::shared_ptr<arrow::Array> cache_;

 private:
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
    std::shared_ptr<arrow::Array> cache_;                      \
                                                               \
   private:                                                    \
    const TYPE* raw_value_;                                    \
    const uint8_t* null_bitmap_data_;                          \
    uint64_t offset_;                                          \
    uint64_t length_;                                          \
    uint64_t null_count_;                                      \
  };

TYPED_ARRAY_DEFINE(Int8Array, int8_t)
TYPED_ARRAY_DEFINE(Int16Array, int16_t)
TYPED_ARRAY_DEFINE(Int32Array, int32_t)
TYPED_ARRAY_DEFINE(Int64Array, int64_t)
TYPED_ARRAY_DEFINE(UInt8Array, uint8_t)
TYPED_ARRAY_DEFINE(UInt16Array, uint16_t)
TYPED_ARRAY_DEFINE(UInt32Array, uint32_t)
TYPED_ARRAY_DEFINE(UInt64Array, uint64_t)
TYPED_ARRAY_DEFINE(FloatArray, float)
TYPED_ARRAY_DEFINE(DoubleArray, double)
TYPED_ARRAY_DEFINE(Date32Array, int32_t)
TYPED_ARRAY_DEFINE(Date64Array, int64_t)
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
    std::shared_ptr<arrow::Array> cache_;                                              \
                                                                                       \
   private:                                                                            \
    using offset_type = int32_t;                                                       \
    const uint8_t* raw_value_;                                                         \
    const int32_t* raw_value_offsets_;                                                 \
    const uint8_t* null_bitmap_data_;                                                  \
    int64_t offset_;                                                                   \
    uint64_t length_;                                                                  \
    uint64_t null_count_;                                                              \
  };
TYPED_BINARY_ARRAY_DEFINE(StringArray, std::string)
#undef TYPED_BINARY_ARRAY_DEFINE

class FixedSizeBinaryArray {
 public:
  FixedSizeBinaryArray(const std::shared_ptr<arrow::Array>&);
  arrow::util::string_view GetView(int64_t i) const {
    return arrow::util::string_view(reinterpret_cast<const char*>(GetValue(i)),
                                    byte_width_);
  }
  const uint8_t* GetValue(int64_t i) const {
    return raw_value_ + (i + offset_) * byte_width_;
  }
  bool IsNull(int64_t i) const {
    i += offset_;
    return null_bitmap_data_ != NULLPTR &&
           !((null_bitmap_data_[i >> 3] >> (i & 0x07)) & 1);
  }
  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  const uint8_t* value_data() const { return raw_value_; }

  std::shared_ptr<arrow::Array> cache_;

 private:
  const uint8_t* raw_value_;
  const uint8_t* null_bitmap_data_;
  uint64_t offset_;
  uint64_t length_;
  uint64_t null_count_;
  int32_t byte_width_;
};

class Decimal128Array : public FixedSizeBinaryArray {
 public:
  Decimal128Array(const std::shared_ptr<arrow::Array>& in) : FixedSizeBinaryArray(in) {}
  arrow::Decimal128 GetView(int64_t i) const {
    const arrow::Decimal128 value(GetValue(i));
    return value;
  }
};

arrow::Status MakeFixedSizeBinaryArray(const std::shared_ptr<arrow::FixedSizeBinaryType>&,
                                       int64_t, const std::shared_ptr<arrow::Buffer>&,
                                       std::shared_ptr<FixedSizeBinaryArray>*);

/*template <typename T, typename Enable = void>
struct TypeTraits {};

template <typename T>
struct TypeTraits<T, std::enable_if_t<std::is_same<T, uint32_t>::value>> {
  using ArrayType = UInt32Array;
};*/

}  // namespace precompile
}  // namespace sparkcolumnarplugin
