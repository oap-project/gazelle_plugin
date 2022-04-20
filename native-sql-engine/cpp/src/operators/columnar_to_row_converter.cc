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

#include "operators/columnar_to_row_converter.h"

#include <iostream>

namespace sparkcolumnarplugin {
namespace columnartorow {

int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

int64_t RoundNumberOfBytesToNearestWord(int64_t numBytes) {
  int64_t remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }
}

int64_t CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema,
                                 int64_t num_cols) {
  std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
  // Calculate the decimal col num when the precision >18
  int32_t count = 0;
  for (auto i = 0; i < num_cols; i++) {
    auto type = fields[i]->type();
    if (type->id() == arrow::Decimal128Type::type_id) {
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(type.get());
      int32_t precision = dtype->precision();
      if (precision > 18) count++;
    }
  }

  int64_t fixed_size = CalculateBitSetWidthInBytes(num_cols) + num_cols * 8;
  int64_t decimal_cols_size = count * 16;
  return fixed_size + decimal_cols_size;
}

int32_t CalculateHeaderPortionInBytes(int32_t num_elements) {
  return 8 + ((num_elements + 63) / 64) * 8;
}

arrow::Status CalculatedElementSize(arrow::Type::type type_id, int32_t* length) {
  switch (type_id) {
    case arrow::BooleanType::type_id:
    case arrow::Int8Type::type_id: {
      *length = 1;
      break;
    }

    case arrow::Int16Type::type_id: {
      *length = 2;
      break;
    }

    case arrow::Int32Type::type_id:
    case arrow::Date32Type::type_id:
    case arrow::FloatType::type_id: {
      *length = 4;
      break;
    }

    case arrow::Int64Type::type_id:
    case arrow::DoubleType::type_id:
    case arrow::TimestampType::type_id:
    // The following type is variable type.
    // BinaryType & StringType store the offset & size
    // The Decimal128Type store the long value when the precision <=18
    // and store the offset & size when the precision > 18.
    case arrow::BinaryType::type_id:
    case arrow::StringType::type_id:
    case arrow::Decimal128Type::type_id: {
      *length = 8;
      break;
    }
    default:
      return arrow::Status::Invalid("Unsupported data type in ListArray: " + type_id);
  }
  return arrow::Status::OK();
}

arrow::Status ColumnarToRowConverter::Init() {
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);

  int64_t fixed_size_per_row = CalculatedFixeSizePerRow(rb_->schema(), num_cols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  for (auto i = 0; i < num_rows_; i++) {
    lengths_.push_back(fixed_size_per_row);
    offsets_.push_back(0);
    buffer_cursor_.push_back(nullBitsetWidthInBytes_ + 8 * num_cols_);
  }
  // Calculated the lengths_
  for (auto i = 0; i < num_cols_; i++) {
    auto array = rb_->column(i);
    if (arrow::is_binary_like(array->type_id())) {
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;
      offset_type length;
      for (auto j = 0; j < num_rows_; j++) {
        auto value = binary_array->GetValue(j, &length);
        lengths_[j] += RoundNumberOfBytesToNearestWord(length);
      }
    }

    // Each array has four parts in Spark UnsafeArrayData class:
    // [numElements][null bits][values or offset&length][variable length portion]
    // The array type is considered to be the variable col in the Spark UnsafeRow.
    if (array->type_id() == arrow::ListType::type_id) {
      auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
      int32_t element_size_in_bytes = -1;
      // header_in_bytes:  [numElements][null bits]
      int32_t num_elements = 0, header_in_bytes = 0, fixed_part_in_bytes = 0,
              variable_part_in_bytes = 0;
      for (auto j = 0; j < num_rows_; j++) {
        // Calculated the size of per row in list array
        auto row_array = list_array->value_slice(j);
        num_elements = row_array->length();
        header_in_bytes = CalculateHeaderPortionInBytes(num_elements);

        if (element_size_in_bytes == -1) {
          // only calculated once
          CalculatedElementSize(row_array->type_id(), &element_size_in_bytes);
        }

        fixed_part_in_bytes =
            RoundNumberOfBytesToNearestWord(num_elements * element_size_in_bytes);

        // If the type is binary like or decimal precision > 18, need to calculated the
        // variable part size
        if (arrow::is_binary_like(row_array->type_id())) {
          auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(row_array);
          using offset_type = typename arrow::BinaryType::offset_type;
          offset_type length;
          for (auto k = 0; k < num_elements; k++) {
            if (!binary_array->IsNull(k)) {
              auto value = binary_array->GetValue(k, &length);
              variable_part_in_bytes += RoundNumberOfBytesToNearestWord(length);
            }
          }
        }

        if (row_array->type_id() == arrow::Decimal128Type::type_id) {
          auto dtype = dynamic_cast<arrow::Decimal128Type*>(row_array->type().get());
          int32_t precision = dtype->precision();
          int32_t null_count = row_array->null_count();
          // TODO: the size in UnsafeArrayData is not 16 and is the
          // RoundNumberOfBytesToNearestWord(real size)
          if (precision > 18) variable_part_in_bytes += 16 * (num_elements - null_count);
        }

        lengths_[j] += (header_in_bytes + fixed_part_in_bytes + variable_part_in_bytes);
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    total_memory_size += lengths_[i];
  }

  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size, memory_pool_));

  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);

  buffer_address_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

void BitSet(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t word;
  memcpy(&word, buffer_address + wordOffset, sizeof(int64_t));
  int64_t value = word | mask;
  memcpy(buffer_address + wordOffset, &value, sizeof(int64_t));
}

int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

void SetNullAt(uint8_t* buffer_address, int64_t row_offset, int64_t field_offset,
               int32_t col_index) {
  BitSet(buffer_address + row_offset, col_index);
  // set the value to 0
  memset(buffer_address + row_offset + field_offset, 0, sizeof(int64_t));
  return;
}

int32_t FirstNonzeroLongNum(std::vector<int32_t> mag, int32_t length) {
  int32_t fn = 0;
  int32_t i;
  for (i = length - 1; i >= 0 && mag[i] == 0; i--)
    ;
  fn = length - i - 1;
  return fn;
}

int32_t GetInt(int32_t n, int32_t sig, std::vector<int32_t> mag, int32_t length) {
  if (n < 0) return 0;
  if (n >= length) return sig < 0 ? -1 : 0;

  int32_t magInt = mag[length - n - 1];
  return (sig >= 0 ? magInt
                   : (n <= FirstNonzeroLongNum(mag, length) ? -magInt : ~magInt));
}

int32_t GetNumberOfLeadingZeros(uint32_t i) {
  // HD, Figure 5-6
  if (i == 0) return 32;
  int32_t n = 1;
  if (i >> 16 == 0) {
    n += 16;
    i <<= 16;
  }
  if (i >> 24 == 0) {
    n += 8;
    i <<= 8;
  }
  if (i >> 28 == 0) {
    n += 4;
    i <<= 4;
  }
  if (i >> 30 == 0) {
    n += 2;
    i <<= 2;
  }
  n -= i >> 31;
  return n;
}

int32_t GetBitLengthForInt(uint32_t n) { return 32 - GetNumberOfLeadingZeros(n); }

int32_t GetBitCount(uint32_t i) {
  // HD, Figure 5-2
  i = i - ((i >> 1) & 0x55555555);
  i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
  i = (i + (i >> 4)) & 0x0f0f0f0f;
  i = i + (i >> 8);
  i = i + (i >> 16);
  return i & 0x3f;
}

int32_t GetBitLength(int32_t sig, std::vector<int32_t> mag, int32_t len) {
  int32_t n = -1;
  if (len == 0) {
    n = 0;
  } else {
    // Calculate the bit length of the magnitude
    int32_t mag_bit_length = ((len - 1) << 5) + GetBitLengthForInt((uint32_t)mag[0]);
    if (sig < 0) {
      // Check if magnitude is a power of two
      bool pow2 = (GetBitCount((uint32_t)mag[0]) == 1);
      for (int i = 1; i < len && pow2; i++) pow2 = (mag[i] == 0);

      n = (pow2 ? mag_bit_length - 1 : mag_bit_length);
    } else {
      n = mag_bit_length;
    }
  }
  return n;
}

std::vector<uint32_t> ConvertMagArray(int64_t new_high, uint64_t new_low, int32_t* size) {
  std::vector<uint32_t> mag;
  int64_t orignal_low = new_low;
  int64_t orignal_high = new_high;
  mag.push_back(new_high >>= 32);
  mag.push_back((uint32_t)orignal_high);
  mag.push_back(new_low >>= 32);
  mag.push_back((uint32_t)orignal_low);

  int32_t start = 0;
  // remove the front 0
  for (int32_t i = 0; i < 4; i++) {
    if (mag[i] == 0) start++;
    if (mag[i] != 0) break;
  }

  int32_t length = 4 - start;
  std::vector<uint32_t> new_mag;
  // get the mag after remove the high 0
  for (int32_t i = start; i < 4; i++) {
    new_mag.push_back(mag[i]);
  }

  *size = length;
  return new_mag;
}

/*
 *  This method refer to the BigInterger#toByteArray() method in Java side.
 */
std::array<uint8_t, 16> ToByteArray(arrow::Decimal128 value, int32_t* length) {
  int64_t high = value.high_bits();
  uint64_t low = value.low_bits();
  arrow::Decimal128 new_value;
  int32_t sig;
  if (value > 0) {
    new_value = value;
    sig = 1;
  } else if (value < 0) {
    new_value = value.Abs();
    sig = -1;
  } else {
    new_value = value;
    sig = 0;
  }

  int64_t new_high = new_value.high_bits();
  uint64_t new_low = new_value.low_bits();

  std::vector<uint32_t> mag;
  int32_t size;
  mag = ConvertMagArray(new_high, new_low, &size);

  std::vector<int32_t> final_mag;
  for (auto i = 0; i < size; i++) {
    final_mag.push_back(mag[i]);
  }

  int32_t byte_length = GetBitLength(sig, final_mag, size) / 8 + 1;

  std::array<uint8_t, 16> out{{0}};
  uint32_t next_int = 0;
  for (int32_t i = byte_length - 1, bytes_copied = 4, int_index = 0; i >= 0; i--) {
    if (bytes_copied == 4) {
      next_int = GetInt(int_index++, sig, final_mag, size);
      bytes_copied = 1;
    } else {
      next_int >>= 8;
      bytes_copied++;
    }

    out[i] = (uint8_t)next_int;
  }
  *length = byte_length;
  return out;
}

arrow::Status WriteValue(uint8_t* buffer_address, int64_t field_offset,
                         std::shared_ptr<arrow::Array> array, int32_t col_index,
                         int64_t row_index, std::vector<int64_t>& offsets,
                         std::vector<int64_t>& buffer_cursor) {
  switch (array->type_id()) {
    case arrow::BooleanType::type_id: {
      // Boolean type
      auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
        bool is_null = array->IsNull(row_index);
        if (is_null) {
          SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);
        } else {
          auto value = bool_array->Value(row_index);
          memcpy(buffer_address + offsets[row_index] + field_offset, &value, sizeof(bool));
        }
      break;
    }
    case arrow::BinaryType::type_id: {
      // Binary type
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;

        bool is_null = array->IsNull(row_index);
        if (is_null) {
          SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);
        } else {
          offset_type length;
          auto value = binary_array->GetValue(row_index, &length);
          // write the variable value
          memcpy(buffer_address + offsets[row_index] + buffer_cursor[row_index], value, length);
          // write the offset and size
          int64_t offsetAndSize = (buffer_cursor[row_index] << 32) | length;
          memcpy(buffer_address + offsets[row_index] + field_offset, &offsetAndSize,
                 sizeof(int64_t));
          buffer_cursor[row_index] += length;
        }
      break;
    }
    case arrow::StringType::type_id: {
      // String type
      auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
      using offset_type = typename arrow::StringType::offset_type;
      offset_type length;
        bool is_null = array->IsNull(row_index);
        if (is_null) {
          SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);
        } else {
          offset_type length;
          auto value = string_array->GetValue(row_index, &length);
          // write the variable value
          memcpy(buffer_address + offsets[row_index] + buffer_cursor[row_index], value, length);
          // write the offset and size
          int64_t offsetAndSize = (buffer_cursor[row_index] << 32) | length;
          memcpy(buffer_address + offsets[row_index] + field_offset, &offsetAndSize,
                 sizeof(int64_t));
          buffer_cursor[row_index] += length;
        }
      break;
    }
    case arrow::Decimal128Type::type_id: {
      auto out_array = dynamic_cast<arrow::Decimal128Array*>(array.get());
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());

      int32_t precision = dtype->precision();
      int32_t scale = dtype->scale();

        const arrow::Decimal128 out_value(out_array->GetValue(row_index));
        bool flag = out_array->IsNull(row_index);

        if (precision <= 18) {
          if (!flag) {
            // Get the long value and write the long value
            // Refer to the int64_t() method of Decimal128
            int64_t long_value = static_cast<int64_t>(out_value.low_bits());
            memcpy(buffer_address + offsets[row_index] + field_offset, &long_value, sizeof(long));
          } else {
            SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);
          }
        } else {
          if (flag) {
            SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);
          } else {
            int32_t size;
            auto out = ToByteArray(out_value, &size);
            assert(size <= 16);

            // write the variable value
            memcpy(buffer_address + buffer_cursor[row_index] + offsets[row_index], &out[0], size);
            // write the offset and size
            int64_t offsetAndSize = (buffer_cursor[row_index] << 32) | size;
            memcpy(buffer_address + offsets[row_index] + field_offset, &offsetAndSize,
                   sizeof(int64_t));
          }

          // Update the cursor of the buffer.
          int64_t new_cursor = buffer_cursor[row_index] + 16;
          buffer_cursor[row_index] = new_cursor;
        }
      break;
    }
    default: {
      switch (arrow::bit_width(array->type_id())) {
#define PROCESS(ARRAYTYPE, BYTES) {                                                     \
      /* default Numeric type */                                                              \
      auto numeric_array = std::static_pointer_cast<arrow::ARRAYTYPE>(array);               \
        bool is_null = array->IsNull(row_index);                                  \
        if (is_null) {                                                      \
          SetNullAt(buffer_address, offsets[row_index], field_offset, col_index);                \
        } else {                                                                          \
          auto value = numeric_array->Value(row_index);                                      \
          memcpy(buffer_address + offsets[row_index] + field_offset, &value, BYTES);           \
        }                                                                           \
      break;                                                                  \
    }
      case 8:
        PROCESS(Int8Array, 1)
      case 16:
        PROCESS(Int16Array, 2)
      case 32:
        PROCESS(Int32Array, 4)
      case 64:
        PROCESS(Int64Array, 8)
#undef PROCESS
      default:
        return arrow::Status::Invalid("Unsupported data type: " + array->type_id());
    }
   }
  }
  return arrow::Status::OK();
}

arrow::Status ColumnarToRowConverter::Write() {
  // for (auto i = 0; i < num_cols_; i++) {
  //   auto array = rb_->column(i);
  //   int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, i);
  //   WriteValue(buffer_address_, field_offset, array, i, num_rows_, offsets_,
  //              buffer_cursor_);
  // }


  std::vector<int64_t> field_offset_vec;
  for (auto i = 0; i < num_cols_; i++) {
    field_offset_vec.push_back(GetFieldOffset(nullBitsetWidthInBytes_, i));
  }

  for (auto i = 0; i < num_rows_; i++) {
    for (auto j = 0; j < num_cols_; j++) {
      auto array = rb_->column(j);
      WriteValue(buffer_address_, field_offset_vec[j], array, j, i, offsets_,
               buffer_cursor_);
    }
  }


  return arrow::Status::OK();
}

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin
