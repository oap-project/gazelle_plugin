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

#include "operators/row_to_columnar_converter.h"

#include <immintrin.h>
#include <x86intrin.h>

#include <algorithm>
#include <iostream>

namespace sparkcolumnarplugin {
namespace rowtocolumnar {

int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

inline bool IsNull(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t value = *((int64_t*)(buffer_address + wordOffset));
  return (value & mask) != 0;
}

int32_t CalculateHeaderPortionInBytes(int32_t num_elements) {
  return 8 + ((num_elements + 63) / 64) * 8;
}

arrow::Status CreateArrayData(int32_t row_start, std::shared_ptr<arrow::Schema> schema, int32_t  batch_rows,
                              int32_t columnar_id, int64_t fieldOffset,
                              std::vector<int64_t>& offsets, uint8_t* memory_address_,
                              std::shared_ptr<arrow::ArrayData>* array,
                              arrow::MemoryPool* pool, bool support_avx512,
                              std::vector<arrow::Type::type>& typevec,
                              std::vector<uint8_t>& typewidth) {
  // auto field = schema->field(columnar_id);
  // auto type = field->type();

  switch (typevec[columnar_id]) {
    case arrow::BooleanType::type_id: {
      // arrow::ArrayData out_data;
      // (*array)->length = num_rows;
      // (*array)->buffers.resize(2);
      // (*array)->type = arrow::TypeTraits<arrow::BooleanType>::type_singleton();

      // ARROW_ASSIGN_OR_RAISE((*array)->buffers[1], AllocateBitmap(num_rows, pool));
      // ARROW_ASSIGN_OR_RAISE((*array)->buffers[0], AllocateBitmap(num_rows, pool));
      auto array_data = (*array)->buffers[1]->mutable_data();
      int64_t position = row_start;
      int64_t null_count = 0;

      auto out_is_valid = (*array)->buffers[0]->mutable_data();
      while (position < row_start + batch_rows) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          null_count++;
          arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        } else {
          bool value = *(bool*)(memory_address_ + offsets[position] + fieldOffset);
          arrow::BitUtil::SetBitTo(array_data, position, value);
          arrow::BitUtil::SetBitTo(out_is_valid, position, true);
        }
        position++;
      }
      (*array)->null_count += null_count;
      // *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      return arrow::Status::OK();
      break;
    }
    case arrow::BinaryType::type_id:
    case arrow::StringType::type_id: {
      // arrow::ArrayData out_data;
      // (*array)->length = num_rows;
      // (*array)->buffers.resize(3);
      // (*array)->type = field->type();
      using offset_type = typename arrow::StringType::offset_type;
      // ARROW_ASSIGN_OR_RAISE((*array)->buffers[0], AllocateBitmap(num_rows, pool));
      // ARROW_ASSIGN_OR_RAISE((*array)->buffers[1],
      //                       AllocateBuffer(sizeof(offset_type) * (num_rows + 1), pool));
      // ARROW_ASSIGN_OR_RAISE((*array)->buffers[2],
      //                       AllocateResizableBuffer(20 * num_rows, pool));
      auto validity_buffer = (*array)->buffers[0]->mutable_data();
      // // initialize all true once allocated
      // memset(validity_buffer, 0xff, (*array)->buffers[0]->capacity());
      auto array_offset = (*array)->GetMutableValues<offset_type>(1);
      auto array_data = (*array)->buffers[2]->mutable_data();
      int64_t null_count = 0;

      array_offset[0] = 0;
      for (int64_t position = row_start; position < row_start+batch_rows; position++) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          arrow::BitUtil::SetBitTo(validity_buffer, position, false);
          array_offset[position + 1] = array_offset[position];
          null_count++;
        } else {
          int64_t offsetAndSize =
              *(int64_t*)(memory_address_ + offsets[position] + fieldOffset);
          offset_type length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          auto value_offset = array_offset[position + 1] =
              array_offset[position] + length;
          uint64_t capacity = (*array)->buffers[2]->capacity();

          if (ARROW_PREDICT_FALSE(value_offset >= capacity)) {
            // allocate value buffer again
            // enlarge the buffer by 1.5x
            capacity = capacity + std::max((capacity >> 1), (uint64_t)length);
            auto value_buffer =
                std::static_pointer_cast<arrow::ResizableBuffer>((*array)->buffers[2]);
            value_buffer->Reserve(capacity);
            array_data = value_buffer->mutable_data();
          }

          auto dst_value_base = array_data + array_offset[position];
          auto value_src_ptr = memory_address_ + offsets[position] + wordoffset;
#ifdef __AVX512BW__
          if (ARROW_PREDICT_TRUE(support_avx512)) {
            // write the variable value
            uint32_t k;
            for (k = 0; k + 32 < length; k += 32) {
              __m256i v = _mm256_loadu_si256((const __m256i*)(value_src_ptr + k));
              _mm256_storeu_si256((__m256i*)(dst_value_base + k), v);
            }
            auto mask = (1L << (length - k)) - 1;
            __m256i v = _mm256_maskz_loadu_epi8(mask, value_src_ptr + k);
            _mm256_mask_storeu_epi8(dst_value_base + k, mask, v);
          } else
#endif
          {
            memcpy(dst_value_base, value_src_ptr, length);
          }
        }
      }
      (*array)->null_count += null_count;
      if (null_count == 0) {
        (*array)->buffers[0] == nullptr;
      }
      // *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      break;
    }
    default: {
        // arrow::ArrayData out_data;
        // (*array)->length = num_rows;
        // (*array)->buffers.resize(2);
        // (*array)->type = field->type();
        // ARROW_ASSIGN_OR_RAISE(
        //     (*array)->buffers[1],
        //     AllocateBuffer(typewidth[columnar_id] * num_rows,
        //                   pool));
        // ARROW_ASSIGN_OR_RAISE((*array)->buffers[0], AllocateBitmap(num_rows, pool));
        auto array_data = (*array)->buffers[1]->mutable_data();
        // auto array_data =
        //     (*array)->GetMutableValues<arrow::TypeTraits<arrow::Int64Type>::CType>(1);
        int64_t position = row_start;
        int64_t null_count = 0;
        auto out_is_valid = (*array)->buffers[0]->mutable_data();



        if (typewidth[columnar_id] > 0) {
          while (position < row_start+batch_rows) {
          const uint8_t* srcptr = (memory_address_ + offsets[position] + fieldOffset);
          bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);

          // auto dataptr = dataptrs[col_index][1];
          auto mask = (1L << (typewidth[columnar_id])) - 1;
          auto shift = _tzcnt_u32(typewidth[columnar_id]);
          // auto buffer_address_tmp = buffer_address + field_offset;
          // for (auto j = row_start; j < row_start + batch_rows; j++) {
            // if (nullvec[col_index] || (!array->IsNull(j))) {
              uint8_t* destptr = array_data + (position << shift);
              if(!is_null) {
              // const uint8_t* srcptr = dataptr + (j << shift);
              
              
#ifdef __AVX512BW__
              if (ARROW_PREDICT_TRUE(support_avx512)) {
                __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
                _mm256_mask_storeu_epi8(destptr, mask, v);
                // _mm_prefetch(srcptr + 64, _MM_HINT_T0);
              } else
#endif
              {
                memcpy(destptr, srcptr, typewidth[columnar_id]);
              }
            } else {
              null_count++;
              arrow::BitUtil::SetBitTo(out_is_valid, position, false);
              // SetNullAt(buffer_address, offsets[j], field_offset, col_index);
              memset(destptr, 0, typewidth[columnar_id]);
            }
            position++;
          }
          (*array)->null_count += null_count;
          // *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
          break;
        } else {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[columnar_id]);
        }
      }
  }
  return arrow::Status::OK();
}

arrow::Status RowToColumnarConverter::Init(std::shared_ptr<arrow::RecordBatch>* batch) {
  support_avx512_ = __builtin_cpu_supports("avx512bw");
  int64_t nullBitsetWidthInBytes = CalculateBitSetWidthInBytes(num_cols_);
  for (auto i = 0; i < num_rows_; i++) {
    offsets_.push_back(0);
  }
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + row_length_[i - 1];
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  auto num_fields = schema_->num_fields();

  std::vector<arrow::Type::type> typevec;
  std::vector<uint8_t> typewidth;
  std::vector<int64_t> field_offset_vec;
  typevec.resize(num_fields);
  // Store bytes for different fixed width types
  typewidth.resize(num_fields);
  field_offset_vec.resize(num_fields);

 
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  columns.resize(num_fields);

  for (auto i = 0; i < num_fields; i++) {
    auto field = schema_->field(i);
    typevec[i] = field->type()->id();
    typewidth[i] = arrow::bit_width(typevec[i]) >> 3;
    field_offset_vec[i] = GetFieldOffset(nullBitsetWidthInBytes, i);

    switch (typevec[i]) {
    case arrow::BooleanType::type_id: {
      arrow::ArrayData out_data;
      out_data.length = num_rows_;
      out_data.buffers.resize(2);
      out_data.type = arrow::TypeTraits<arrow::BooleanType>::type_singleton();
      out_data.null_count = 0;

      ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBitmap(num_rows_, m_pool_));
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
      columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
      break;
    }
    case arrow::BinaryType::type_id:
    case arrow::StringType::type_id: {
      arrow::ArrayData out_data;
      out_data.length = num_rows_;
      out_data.buffers.resize(3);
      out_data.type = field->type();
      out_data.null_count = 0;
      using offset_type = typename arrow::StringType::offset_type;
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[1],
                            AllocateBuffer(sizeof(offset_type) * (num_rows_ + 1), m_pool_));
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[2],
                            AllocateResizableBuffer(20 * num_rows_, m_pool_));
      auto validity_buffer = out_data.buffers[0]->mutable_data();
      // initialize all true once allocated
      memset(validity_buffer, 0xff, out_data.buffers[0]->capacity());
      columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
      break;
    }
    case arrow::Decimal128Type::type_id: {
      auto dtype = std::dynamic_pointer_cast<arrow::Decimal128Type>(field->type());
      int32_t precision = dtype->precision();
      int32_t scale = dtype->scale();

      arrow::ArrayData out_data;
      out_data.length = num_rows_;
      out_data.buffers.resize(2);
      out_data.type = arrow::decimal128(precision, scale);
      out_data.null_count = 0;
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(16 * num_rows_, m_pool_));
      ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
      columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
      break;
    }
    default: {
        arrow::ArrayData out_data;
        out_data.length = num_rows_;
        out_data.buffers.resize(2);
        out_data.type = field->type();
        out_data.null_count = 0;
        ARROW_ASSIGN_OR_RAISE(
            out_data.buffers[1],
            AllocateBuffer(typewidth[i] * num_rows_,
                          m_pool_));
        ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
        columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
        break;
    }
    }

  }

  
  #define BATCH_ROW_NUM 16
  int row = 0;
  for (row; row + BATCH_ROW_NUM < num_rows_; row += BATCH_ROW_NUM) {
    for (auto i = 0; i < num_fields; i++) {
    // auto field = schema_->field(i);
    // int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes, i);
    RETURN_NOT_OK(CreateArrayData(row, schema_, BATCH_ROW_NUM, i, field_offset_vec[i], offsets_,
                                  memory_address_, &(columns[i]), m_pool_,
                                  support_avx512_, typevec, typewidth));
    }
  }
  for (row; row < num_rows_; row++) {
    for (auto i = 0; i < num_fields; i++) {
    // auto field = schema_->field(i);
    // int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes, i);
    RETURN_NOT_OK(CreateArrayData(row, schema_, 1, i, field_offset_vec[i], offsets_,
                                  memory_address_, &(columns[i]), m_pool_,
                                  support_avx512_, typevec, typewidth));
    }
  }

  // for (auto i = 0; i < num_fields; i++) {
  //   auto field = schema_->field(i);
    
    



  //   std::shared_ptr<arrow::Array> array_data;
  //   int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes, i);
  //   RETURN_NOT_OK(CreateArrayData(schema_, num_rows_, i, field_offset, offsets_,
  //                                 memory_address_, &array_data, m_pool_,
  //                                 support_avx512_, typevec, typewidth));
  //   arrays.push_back(array_data);
  // }

  for (auto i = 0; i < num_fields; i++) {
    auto array = MakeArray(columns[i]);
    arrays.push_back(array);
  }

  *batch = arrow::RecordBatch::Make(schema_, num_rows_, arrays);
  return arrow::Status::OK();
}


}  // namespace rowtocolumnar
}  // namespace sparkcolumnarplugin
