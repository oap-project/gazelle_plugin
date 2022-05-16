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
#include <immintrin.h>

namespace sparkcolumnarplugin {
namespace columnartorow {

uint32_t x_7[8] __attribute__ ((aligned (32))) = {0x7,0x7,0x7,0x7,0x7,0x7,0x7,0x7};
uint32_t x_8[8] __attribute__ ((aligned (32))) = {0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8};

inline int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) >> 6) << 3;
}

inline int32_t RoundNumberOfBytesToNearestWord(int32_t numBytes) {
  int32_t remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  
  return numBytes + ((8 - remainder) & 0x7);
  /*if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }*/
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

inline int32_t CalculateHeaderPortionInBytes(int32_t num_elements) {
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
/*  std::vector<int32_t> buffer_cursor_;
  std::shared_ptr<arrow::RecordBatch> rb_;
  std::shared_ptr<arrow::Buffer> buffer_;
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  int32_t nullBitsetWidthInBytes_;
  int32_t num_cols_;
  int32_t num_rows_;
  uint8_t* buffer_address_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>> lengths_;
*/
arrow::Status ColumnarToRowConverter::Init(const std::shared_ptr<arrow::RecordBatch>& rb) {
  rb_ = rb;
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);

  int32_t fixed_size_per_row = CalculatedFixeSizePerRow(rb_->schema(), num_cols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  lengths_.resize(num_rows_,fixed_size_per_row);
  std::fill(lengths_.begin(),lengths_.end(),fixed_size_per_row);

  offsets_.resize(num_rows_ + 1);
  buffer_cursor_.resize(num_rows_, nullBitsetWidthInBytes_ + 8 * num_cols_);
  
  // Calculated the lengths_
  for (auto i = 0; i < num_cols_; i++) {
    auto array = rb_->column(i);
//    if (arrow::is_binary_like(array->type_id())) {
//      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
//      using offset_type = typename arrow::BinaryType::offset_type;
//      offset_type length;
//      const offset_type* offsetarray = binary_array->raw_value_offsets();
//      __m256i x7_8x = _mm256_load_si256((__m256i*)x_7);
//      __m256i x8_8x = _mm256_load_si256((__m256i*)x_8);
//      int32_t j=0;
//      int32_t* length_data = lengths_.data();
//
//      __m256i offsetarray_1_8x;
//      if (j + 16 < num_rows_)
//      {
//        offsetarray_1_8x = _mm256_load_si256((__m256i*)&offsetarray[j]);
//      }
//      for (j; j + 16 < num_rows_; j += 8) {
//        __m256i offsetarray_8x = offsetarray_1_8x;
//        offsetarray_1_8x = _mm256_load_si256((__m256i*)&offsetarray[j+8]);
//
//        __m256i length_8x = _mm256_alignr_epi32(offsetarray_8x,offsetarray_1_8x,0x1);
//        length_8x = _mm256_sub_epi32(length_8x, offsetarray_8x);
//
//        __m256i reminder_8x = _mm256_and_si256(length_8x, x7_8x);
//        reminder_8x = _mm256_sub_epi32(x8_8x,reminder_8x);
//        reminder_8x = _mm256_and_si256(reminder_8x,x7_8x);
//        __m256i dst_length_8x = _mm256_loadu_si256((__m256i*)length_data);
//        dst_length_8x = _mm256_add_epi32(dst_length_8x, reminder_8x);
//        _mm256_storeu_si256((__m256i*)length_data,dst_length_8x);
//        length_data+=8;
//        _mm_prefetch(&offsetarray[j+(128+128)/sizeof(offset_type)],_MM_HINT_T0);
//      }
//      for (j; j < num_rows_; j++) {
//
//        offset_type length = offsetarray[j+1] - offsetarray[j];
//        *length_data += RoundNumberOfBytesToNearestWord(length);
//        length_data++;
//      }
//    }

        if (arrow::is_binary_like(array->type_id())) {
              auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
              using offset_type = typename arrow::BinaryType::offset_type;
              offset_type length;
              for (auto j = 0; j < num_rows_; j++) {
                auto value = binary_array->GetValue(j, &length);
                lengths_[j] += RoundNumberOfBytesToNearestWord(length);
              }
         }



  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  offsets_[0] = 0;
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = total_memory_size;
    total_memory_size += lengths_[i];
  }
  offsets_[num_rows_] = total_memory_size;

  
  // allocate one more cache line to ease avx operations
//  if(buffer_==nullptr || buffer_->capacity() < total_memory_size+64){
//    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size+64, memory_pool_));
//    memset(buffer_->mutable_data()+total_memory_size,0,buffer_->capacity()-total_memory_size);
//  }

  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size, memory_pool_));
  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);

//  std::cout << std::hex << "buffer addr = " << buffer_->address() << std::dec  << " size = " << total_memory_size << std::endl;

//  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);

  buffer_address_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

inline void BitSet(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t word;
  word = *(int64_t*)(buffer_address + wordOffset);
  int64_t value = word | mask;
  *(int64_t*)(buffer_address + wordOffset) = value;
}

inline void SetNullAt(uint8_t* buffer_address, int64_t row_offset, int64_t field_offset,
               int32_t col_index) {
  BitSet(buffer_address + row_offset, col_index);
  // set the value to 0
  *(int64_t*)(buffer_address + row_offset + field_offset) = 0;

  return;
}

inline int32_t FirstNonzeroLongNum(std::vector<int32_t> mag, int32_t length) {
  int32_t fn = 0;
  int32_t i;
  for (i = length - 1; i >= 0 && mag[i] == 0; i--)
    ;
  fn = length - i - 1;
  return fn;
}

inline int32_t GetInt(int32_t n, int32_t sig, std::vector<int32_t> mag, int32_t length) {
  if (n < 0) return 0;
  if (n >= length) return sig < 0 ? -1 : 0;

  int32_t magInt = mag[length - n - 1];
  return (sig >= 0 ? magInt
                   : (n <= FirstNonzeroLongNum(mag, length) ? -magInt : ~magInt));
}

inline int32_t GetNumberOfLeadingZeros(uint32_t i) {
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

inline int32_t GetBitLengthForInt(uint32_t n) { return 32 - GetNumberOfLeadingZeros(n); }

inline int32_t GetBitCount(uint32_t i) {
  // HD, Figure 5-2
  i = i - ((i >> 1) & 0x55555555);
  i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
  i = (i + (i >> 4)) & 0x0f0f0f0f;
  i = i + (i >> 8);
  i = i + (i >> 16);
  return i & 0x3f;
}

inline int32_t GetBitLength(int32_t sig, std::vector<int32_t> mag, int32_t len) {
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

arrow::Status ColumnarToRowConverter::Write() {
               
    auto buffer_address = buffer_address_;
    auto num_rows = num_rows_;
    auto offsets = offsets_;
    auto buffer_cursor = buffer_cursor_;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::vector<const uint8_t*>> dataptrs;
    std::vector<int64_t> col_arrdata_offsets;
    dataptrs.resize(num_cols_);
    col_arrdata_offsets.resize(num_cols_);
    std::vector<uint8_t> nullvec;
    nullvec.resize(num_cols_,0);

    std::vector<arrow::Type::type> typevec;
    std::vector<uint8_t> typewidth;

    typevec.resize(num_cols_);
    // Store bytes for different fixed width types
    typewidth.resize(num_cols_);

  
  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    auto array = rb_->column(col_index);
    arrays.push_back(array);
    auto arraydata = array->data();
    auto bufs = arraydata->buffers;
    col_arrdata_offsets[col_index] = arraydata->offset;

    // If nullvec[col_index]  equals 1, means no null value in this column
    nullvec[col_index] = (array->null_count()==0);
    typevec[col_index] = array->type_id();
    // calculate bytes from bit_num
    typewidth[col_index] = arrow::bit_width(typevec[col_index]) >> 3;

    if (arrow::bit_width(array->type_id()) > 1)
    {
      if(bufs[0]){
        dataptrs[col_index].push_back(bufs[0]->data());
      }else{
        dataptrs[col_index].push_back(nullptr);
      }
      dataptrs[col_index].push_back( bufs[1]->data() + arraydata->offset * (arrow::bit_width(array->type_id())>>3));
    }else if(array->type_id()==arrow::StringType::type_id || array->type_id()==arrow::BinaryType::type_id)
    {
      if(bufs[0]){
        dataptrs[col_index].push_back(bufs[0]->data());
      }else{
        dataptrs[col_index].push_back(nullptr);
      }

      auto binary_array = (arrow::BinaryArray*)(array.get());
      dataptrs[col_index].push_back((uint8_t*)(binary_array->raw_value_offsets()));
      dataptrs[col_index].push_back((uint8_t*)(binary_array->raw_data()));
    }
  }

int32_t i=0;
#define BATCH_ROW_NUM 16
for (i; i + BATCH_ROW_NUM < num_rows_; i+=BATCH_ROW_NUM) {
  __m256i fill_0_8x;
  fill_0_8x = _mm256_xor_si256(fill_0_8x, fill_0_8x);
  //_mm_prefetch(&buffer_address+offsets[i],_MM_HINT_T2);

  for (auto j = i; j< i+BATCH_ROW_NUM; j++)
  {
    auto rowlength = offsets[j+1] - offsets[j];
    for (auto p = 0; p<rowlength+32;p+=32)
    {
      _mm256_storeu_si256((__m256i*)(buffer_address+offsets[j]),fill_0_8x);
      _mm_prefetch(buffer_address+offsets[j]+128,_MM_HINT_T0);
    }
  }
  //auto x = i + ((i==(num_rows_-1))-1) & 1;
  //_mm_prefetch(buffer_address+offsets[x],_MM_HINT_T0);
  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    auto& array = arrays[col_index];
    int64_t field_offset = nullBitsetWidthInBytes_ + (col_index << 3L);

    switch (typevec[col_index]) {
      case arrow::BooleanType::type_id: {
        // Boolean type
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);

        for(auto j=i;j<i+BATCH_ROW_NUM;j++) {
          bool is_null = array->IsNull(j);
          if (nullvec[col_index] || (!array->IsNull(j))) {
            auto value = bool_array->Value(j);
            memcpy(buffer_address + offsets[j] + field_offset, &value, sizeof(bool));
          } else {
            SetNullAt(buffer_address, offsets[j], field_offset, col_index);
          }
        }
        break;
      }
      case arrow::StringType::type_id:
      case arrow::BinaryType::type_id: {
        // Binary type
        auto binary_array = (arrow::BinaryArray*)(array.get());
        using offset_type = typename arrow::BinaryType::offset_type;
        offset_type* BinaryOffsets = (offset_type*)(dataptrs[col_index][1]);
        for(auto j=i;j<i+BATCH_ROW_NUM;j++)
        {
          if (nullvec[col_index] || (!array->IsNull(j)))
          {
//            offset_type length = BinaryOffsets[j+1]-BinaryOffsets[j];
//            auto value = &dataptrs[col_index][2][BinaryOffsets[j]];
//            // write the variable value
//            offset_type k;
//            for(k=0;k+32<length;k+=32)
//            {
//              __m256i v = _mm256_loadu_si256((const __m256i*)value+k);
//              _mm256_storeu_si256((__m256i*)(buffer_address + offsets[j] + buffer_cursor[j]+k),v);
//            }
//            // create some bits of "1", num equals length
//            auto mask=(1L << (length-k))-1;
//            __m256i v = _mm256_maskz_loadu_epi8(mask, value+k);
//             _mm256_mask_storeu_epi8(buffer_address + offsets[j] + buffer_cursor[j]+k, mask, v);
//
//          // write the offset and size
//          int64_t offsetAndSize = ((int64_t)buffer_cursor[j] << 32) | length;
//          *(int64_t*)(buffer_address + offsets[j] + field_offset) = offsetAndSize;
//          buffer_cursor[i] += (length);


            offset_type length;
            auto value = binary_array->GetValue(j, &length);
            // write the variable value
            memcpy(buffer_address + offsets[j] + buffer_cursor[j], value, length);
            // write the offset and size
            int64_t offsetAndSize = ((int64_t)buffer_cursor[j] << 32) | length;
            memcpy(buffer_address + offsets[j] + field_offset, &offsetAndSize,
                   sizeof(int64_t));
            buffer_cursor[j] += RoundNumberOfBytesToNearestWord(length);


          }else{
              SetNullAt(buffer_address, offsets[j], field_offset, col_index);
          }
        }
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto out_array = dynamic_cast<arrow::Decimal128Array*>(array.get());
        auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());

        int32_t precision = dtype->precision();
        int32_t scale = dtype->scale();

        for(auto j=i;j<i+BATCH_ROW_NUM;j++) {
          const arrow::Decimal128 out_value(out_array->GetValue(j));

          if (nullvec[col_index] || (!array->IsNull(j))) {
            if (precision <= 18) {
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t long_value = static_cast<int64_t>(out_value.low_bits());
              memcpy(buffer_address + offsets[j] + field_offset, &long_value, sizeof(long));
            } else {
              int32_t size;
              auto out = ToByteArray(out_value, &size);
              assert(size <= 16);

              // write the variable value
              memcpy(buffer_address + buffer_cursor[j] + offsets[j], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)buffer_cursor[j] << 32) | size;
              memcpy(buffer_address + offsets[j] + field_offset, &offsetAndSize,
                    sizeof(int64_t));
            }
          } else {
            SetNullAt(buffer_address, offsets[j], field_offset, col_index);
          }
        }
        break;
      }
      default: {
        if (typewidth[col_index]>0)
        {
          auto dataptr = dataptrs[col_index][1];
          auto mask=(1L << (typewidth[col_index]))-1;
          auto shift = _tzcnt_u32(typewidth[col_index]);
          auto buffer_address_tmp = buffer_address + field_offset;
          for(auto j=i;j<i+BATCH_ROW_NUM;j++)
          {
            if (nullvec[col_index] || (!array->IsNull(j)))
            {
              const uint8_t* srcptr = dataptr + (j << shift);
              __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
              _mm256_mask_storeu_epi8(buffer_address_tmp + offsets[j], mask, v);
              _mm_prefetch(srcptr+64, _MM_HINT_T0);
            }else
            {
              SetNullAt(buffer_address, offsets[j], field_offset, col_index);
            }
          }
          break;
        }else
        {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[col_index]);
        }
      }
    }
  }
}

for (i; i < num_rows_; i++) {
  __m256i fill_0_8x;
  fill_0_8x = _mm256_xor_si256(fill_0_8x, fill_0_8x);
  auto rowlength = offsets[i+1] - offsets[i];
  for (auto p = 0; p<rowlength+32;p+=32)
  {
    _mm256_storeu_si256((__m256i*)(buffer_address+offsets[i]),fill_0_8x);
  }
  _mm_prefetch(buffer_address+offsets[i],_MM_HINT_T1);
  //auto x = i + ((i==(num_rows_-1))-1) & 1;
  //_mm_prefetch(buffer_address+offsets[x],_MM_HINT_T0);
  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    auto& array = arrays[col_index];

    int64_t field_offset = nullBitsetWidthInBytes_ + (col_index << 3L);

    switch (typevec[col_index]) {
      case arrow::BooleanType::type_id: {
        // Boolean type
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);

       //        for (auto i = 0; i < num_rows_; i++) {
          bool is_null = array->IsNull(i);
          if (is_null) {
            SetNullAt(buffer_address, offsets[i], field_offset, col_index);
          } else {
            auto value = bool_array->Value(i);
            memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
          }
      //        }
        break;
      }
      case arrow::StringType::type_id:
      case arrow::BinaryType::type_id: {
        // Binary type
        auto binary_array = (arrow::BinaryArray*)(array.get());
        using offset_type = typename arrow::BinaryType::offset_type;
        offset_type* BinaryOffsets = (offset_type*)(dataptrs[col_index][1]);
        if (nullvec[col_index] || (!array->IsNull(i)))
        {
//          offset_type length = BinaryOffsets[i+1]-BinaryOffsets[i];
//          auto value = &dataptrs[col_index][2][BinaryOffsets[i]];
//          // write the variable value
//          offset_type k;
//          auto j=i;
//            for(k=0;k+32<length;k+=32)
//            {
//              __m256i v = _mm256_loadu_si256((const __m256i*)value+k);
//              _mm256_storeu_si256((__m256i*)(buffer_address + offsets[j] + buffer_cursor[j]+k),v);
//            }
//            auto mask=(1L << (length-k))-1;
//            __m256i v = _mm256_maskz_loadu_epi8(mask, value+k);
//
//          _mm256_mask_storeu_epi8(buffer_address + offsets[j] + buffer_cursor[j]+k, mask, v);
//          // write the offset and size
//          int64_t offsetAndSize = ((int64_t)buffer_cursor[i] << 32) | length;
//          *(int64_t*)(buffer_address + offsets[i] + field_offset) = offsetAndSize;
//
//          auto bufferAddr = buffer_address + offsets[i] + field_offset;
//
//
//
//          buffer_cursor[i] += (length);


          offset_type length;
          auto value = binary_array->GetValue(i, &length);
          // write the variable value
          memcpy(buffer_address + offsets[i] + buffer_cursor[i], value, length);
          // write the offset and size
          int64_t offsetAndSize = ((int64_t)buffer_cursor[i] << 32) | length;
          memcpy(buffer_address + offsets[i] + field_offset, &offsetAndSize,
                 sizeof(int64_t));
          buffer_cursor[i] += RoundNumberOfBytesToNearestWord(length);


        }else{
            SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        }
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto out_array = dynamic_cast<arrow::Decimal128Array*>(array.get());
        auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());

        int32_t precision = dtype->precision();
        int32_t scale = dtype->scale();

        //        for (auto i = 0; i < num_rows; i++) {
          const arrow::Decimal128 out_value(out_array->GetValue(i));
          bool flag = out_array->IsNull(i);

          if (precision <= 18) {
            if (!flag) {
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t long_value = static_cast<int64_t>(out_value.low_bits());
              memcpy(buffer_address + offsets[i] + field_offset, &long_value, sizeof(long));
            } else {
              SetNullAt(buffer_address, offsets[i], field_offset, col_index);
            }
          } else {
            if (flag) {
              SetNullAt(buffer_address, offsets[i], field_offset, col_index);
            } else {
              int32_t size;
              auto out = ToByteArray(out_value, &size);
              assert(size <= 16);

              // write the variable value
              memcpy(buffer_address + buffer_cursor[i] + offsets[i], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)buffer_cursor[i] << 32) | size;
              memcpy(buffer_address + offsets[i] + field_offset, &offsetAndSize,
                    sizeof(int64_t));
            }

            // Update the cursor of the buffer.
            int64_t new_cursor = buffer_cursor[i] + 16;
            buffer_cursor[i] = new_cursor;
          }
        //        }
        break;
      }
      default: {
        if (typewidth[col_index]>0)
        {
          auto dataptr = dataptrs[col_index][1];
          auto mask=(1L << (typewidth[col_index]))-1;
          auto shift = _tzcnt_u32(typewidth[col_index]);
          
          if (nullvec[col_index] || (!array->IsNull(i)))
          {
            const uint8_t* srcptr = dataptr + (i << shift);
            __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
            _mm256_mask_storeu_epi8(buffer_address + offsets[i] + field_offset, mask, v);
            _mm_prefetch(srcptr+64, _MM_HINT_T0);
          } else
          {
            SetNullAt(buffer_address, offsets[i], field_offset, col_index);
          }
          break;
        }else
        {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[col_index]);
        }
      }
    }
  }
}

  return arrow::Status::OK();
}

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin
