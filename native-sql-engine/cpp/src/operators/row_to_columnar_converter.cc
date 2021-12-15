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
#include "codegen/arrow_compute/ext/array_taker.h"
#include <iostream>

namespace sparkcolumnarplugin {
namespace rowtocolumnar {

int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

bool IsNull(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t word;
  memcpy(&word, buffer_address + wordOffset, sizeof(int64_t));
  int64_t value = (word & mask);
  int64_t thebit = value >> (index & 0x3f);
  if (thebit == 1){
    return true;
  } else {
    return false;
  }
}

int32_t CalculateHeaderPortionInBytes(int32_t num_elements) {
  return 8 + ((num_elements + 63) / 64) * 8;
}

int32_t WordOffset(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t word;
  memcpy(&word, buffer_address + wordOffset, sizeof(int64_t));
  int64_t value = (word & mask);
  int64_t thebit = value >> (index & 0x3f);

}

arrow::Status CreateArrayData2(std::shared_ptr<arrow::Schema> schema, int64_t num_rows, int32_t columnar_id, 
    int64_t fieldOffset, std::vector<int64_t>& offsets, int64_t* row_length_, uint8_t* memory_address_, std::shared_ptr<arrow::Array>* array, arrow::MemoryPool* pool) {
  
  auto field = schema->field(columnar_id);
  auto type = field->type();
  
  

  if (type->id() == arrow::BooleanType::type_id) {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::BooleanType>::type_singleton();
  
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBitmap(num_rows, pool));
    // if (has_null_) {
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // }
    auto array_data = out_data.buffers[1]->mutable_data(); 
    int64_t position = 0;
    int64_t null_count = 0;

    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
      } else {
        bool value = *(bool *)(memory_address_ + offsets[position] + fieldOffset);
        arrow::BitUtil::SetBitTo(array_data, position, value);
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::Int8Type::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::Int8Type>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::Int8Type>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::Int8Type>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::Int8Type>::CType{};
      } else {
        auto value = *(int8_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();

  }
  else if (type->id() == arrow::Int16Type::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::Int16Type>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::Int16Type>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::Int16Type>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::Int16Type>::CType{};
      } else {
        auto value = *(int16_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::Int32Type::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::Int32Type>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::Int32Type>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::Int16Type>::CType{};
      } else {
        auto value = *(int32_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::Int64Type::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::Int64Type>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::Int64Type>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::Int64Type>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::Int64Type>::CType{};
      } else {
        auto value = *(int64_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::FloatType::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::FloatType>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::FloatType>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::FloatType>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::FloatType>::CType{};
      } else {
        auto value = *(float *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::DoubleType::type_id)
  {
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::DoubleType>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::DoubleType>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::DoubleType>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::DoubleType>::CType{};
      } else {
        auto value = *(double *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();
  }
  else if (type->id() == arrow::BinaryType::type_id)
  {
    std::unique_ptr<arrow::TypeTraits<arrow::BinaryType>::BuilderType> builder_;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(pool, arrow::TypeTraits<arrow::BinaryType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<arrow::TypeTraits<arrow::BinaryType>::BuilderType *>(array_builder.release()));

    using offset_type = typename arrow::BinaryType::offset_type;
    for (int64_t position = 0; position < num_rows; position++) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        RETURN_NOT_OK(builder_->AppendNull());
      } else {        
        int64_t offsetAndSize;
        memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));
        offset_type length = int32_t(offsetAndSize);
        int32_t wordoffset = int32_t(offsetAndSize >> 32);
        RETURN_NOT_OK(builder_->Append(memory_address_ + wordoffset, length));
      }
    }
    auto status = builder_->Finish(array);  
    return arrow::Status::OK();   
  }
  else if (type->id() == arrow::StringType::type_id)
  { 
    std::unique_ptr<arrow::TypeTraits<arrow::StringType>::BuilderType> builder_;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(pool, arrow::TypeTraits<arrow::StringType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<arrow::TypeTraits<arrow::StringType>::BuilderType *>(array_builder.release()));

    using offset_type = typename arrow::StringType::offset_type;
    for (int64_t position = 0; position < num_rows; position++) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        RETURN_NOT_OK(builder_->AppendNull());
      } else {        
        int64_t offsetAndSize;
        memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));
        offset_type length = int32_t(offsetAndSize);
        int32_t wordoffset = int32_t(offsetAndSize >> 32);
        RETURN_NOT_OK(builder_->Append(memory_address_ + wordoffset, length));
      }
    }
    auto status = builder_->Finish(array);  
    return arrow::Status::OK();   
  }
  else if (type->id() == arrow::Decimal128Type::type_id)
  {
    auto dtype = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
    int32_t precision = dtype->precision();
    int32_t scale = dtype->scale();

    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::decimal128(precision, scale);
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(16 * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    auto array_data = out_data.GetMutableValues<arrow::Decimal128>(1);

    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::Decimal128{};
      } else {
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
        if (precision < 18){
          uint8_t bytesValue[8] = {0};
          memcpy(&bytesValue, memory_address_ + offsets[position] + fieldOffset, 8);
          arrow::Decimal128 value = arrow::Decimal128(arrow::BasicDecimal128(bytesValue));
          array_data[position] = value;
        }
        else{
          int64_t offsetAndSize;
          memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));
          int32_t length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          uint8_t bytesValue[16] = {0};
          // std::array<uint8_t, 16> bytesValue{{0}};
          memcpy(&bytesValue, memory_address_ + offsets[position] + wordoffset, length);
          arrow::Decimal128 value = arrow::Decimal128(arrow::BasicDecimal128(bytesValue));
          array_data[position] = value;
        }
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();   
  }
  else if (type->id() == arrow::Date32Type::type_id)
  { 
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::TypeTraits<arrow::Date32Type>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::Date32Type>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::Date32Type>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::Date32Type>::CType{};
      } else {
        auto value = *(int32_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();   
  }
  else if (type->id() == arrow::TimestampType::type_id)
  {   
    arrow::ArrayData out_data;
    out_data.length = num_rows;
    out_data.buffers.resize(2);
    out_data.type = arrow::int64();
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[1], AllocateBuffer(sizeof(arrow::TypeTraits<arrow::TimestampType>::CType) * num_rows, pool));
    ARROW_ASSIGN_OR_RAISE(out_data.buffers[0], AllocateBitmap(num_rows, pool));
    // auto array_data = out_data.buffers[1]->mutable_data();
    auto array_data = out_data.GetMutableValues<arrow::TypeTraits<arrow::TimestampType>::CType>(1);
    int64_t position = 0;
    int64_t null_count = 0;
    auto out_is_valid = out_data.buffers[0]->mutable_data();
    while (position < num_rows) {
      bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
      if (is_null) {
        null_count++;
        arrow::BitUtil::SetBitTo(out_is_valid, position, false);
        array_data[position] = arrow::TypeTraits<arrow::TimestampType>::CType{};
      } else {
        auto value = *(int64_t *)(memory_address_ + offsets[position] + fieldOffset);
        array_data[position] = value;
        // arrow::BitUtil::SetBitTo(array_data, position, false);
        arrow::BitUtil::SetBitTo(out_is_valid, position, true);
      }
      position++;
    }
    out_data.null_count = null_count;
    *array = MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
    return arrow::Status::OK();   
  }
  else if (type->id() == arrow::ListType::type_id)
  {

    // int64_t header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(type);
    auto child_type = list_type->value_type();
    if (child_type->id() == arrow::Int8Type::type_id){
      // ListBuilder components_builder(pool, std::make_shared<DoubleBuilder>(pool)); 
      // std::unique_ptr<arrow::TypeTraits<arrow::ListType>::BuilderType> builder_;
      // std::unique_ptr<arrow::ArrayBuilder> array_builder;
      // arrow::MakeBuilder(pool, std::make_shared<arrow::ListType>(arrow::BooleanType()), &array_builder);
      // builder_.reset(arrow::internal::checked_cast<arrow::TypeTraits<arrow::ListType>::BuilderType *>(array_builder.release()));
      
      // arrow::BooleanBuilder& cost_components_builder = *(static_cast<arrow::BooleanBuilder *>(builder_.value_builder()));

      arrow::ListBuilder parent_builder(pool, std::make_shared<arrow::Int8Builder>(pool));
      // The following builder is owned by components_builder.
      arrow::Int8Builder& child_builder = *(static_cast<arrow::Int8Builder*>(parent_builder.value_builder()));
      for (int64_t position = 0; position < num_rows; position++) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          RETURN_NOT_OK(parent_builder.AppendNull());
        } else { 
          RETURN_NOT_OK(parent_builder.Append());      
          int64_t offsetAndSize;
          memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));          
          int32_t length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          int64_t num_elements = *(int64_t *)(memory_address_ + offsets[position] + wordoffset); 
          int64_t header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
          for (auto j = 0; j < num_elements; j++) {
            bool is_null = IsNull(memory_address_ + offsets[position] + wordoffset + 8, j);
            if (is_null) {
              child_builder.AppendNull();
            } else {
                bool value = *(int8_t *)(memory_address_ + offsets[position] + wordoffset + header_in_bytes +
                           j * sizeof(int8_t));
                RETURN_NOT_OK(child_builder.Append(value));
            }
          }
          ARROW_RETURN_NOT_OK(parent_builder.Finish(array));
        }
      }
    }
    else if (child_type->id() == arrow::Int16Type::type_id)
    {
      arrow::ListBuilder parent_builder(pool, std::make_shared<arrow::Int16Builder>(pool));
      // The following builder is owned by components_builder.
      arrow::Int16Builder& child_builder = *(static_cast<arrow::Int16Builder*>(parent_builder.value_builder()));
      for (int64_t position = 0; position < num_rows; position++) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          RETURN_NOT_OK(parent_builder.AppendNull());
        } else { 
          RETURN_NOT_OK(parent_builder.Append());      
          int64_t offsetAndSize;
          memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));          
          int32_t length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          int64_t num_elements = *(int64_t *)(memory_address_ + offsets[position] + wordoffset); 
          int64_t header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
          for (auto j = 0; j < num_elements; j++) {
            bool is_null = IsNull(memory_address_ + offsets[position] + wordoffset + 8, j);
            if (is_null) {
              child_builder.AppendNull();
            } else {
                bool value = *(int16_t *)(memory_address_ + offsets[position] + wordoffset + header_in_bytes +
                           j * sizeof(int16_t));
                RETURN_NOT_OK(child_builder.Append(value));
            }
          }
          ARROW_RETURN_NOT_OK(parent_builder.Finish(array));
        }
      }
    }
    else if (child_type->id() == arrow::Int32Type::type_id)
    {
      arrow::ListBuilder parent_builder(pool, std::make_shared<arrow::Int32Builder>(pool));
      // The following builder is owned by components_builder.
      arrow::Int32Builder& child_builder = *(static_cast<arrow::Int32Builder*>(parent_builder.value_builder()));
      for (int64_t position = 0; position < num_rows; position++) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          RETURN_NOT_OK(parent_builder.AppendNull());
        } else { 
          RETURN_NOT_OK(parent_builder.Append());      
          int64_t offsetAndSize;
          memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));          
          int32_t length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          int64_t num_elements = *(int64_t *)(memory_address_ + offsets[position] + wordoffset); 
          int64_t header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
          for (auto j = 0; j < num_elements; j++) {
            bool is_null = IsNull(memory_address_ + offsets[position] + wordoffset + 8, j);
            if (is_null) {
              child_builder.AppendNull();
            } else {
                bool value = *(int32_t *)(memory_address_ + offsets[position] + wordoffset + header_in_bytes +
                           j * sizeof(int32_t));
                RETURN_NOT_OK(child_builder.Append(value));
            }
          }
          ARROW_RETURN_NOT_OK(parent_builder.Finish(array));
        }
      }
    }
    else if (child_type->id() == arrow::Int64Type::type_id)
    {
      arrow::ListBuilder parent_builder(pool, std::make_shared<arrow::Int64Builder>(pool));
      // The following builder is owned by components_builder.
      arrow::Int64Builder& child_builder = *(static_cast<arrow::Int64Builder*>(parent_builder.value_builder()));
      for (int64_t position = 0; position < num_rows; position++) {
        bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
        if (is_null) {
          RETURN_NOT_OK(parent_builder.AppendNull());
        } else { 
          RETURN_NOT_OK(parent_builder.Append());      
          int64_t offsetAndSize;
          memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));          
          int32_t length = int32_t(offsetAndSize);
          int32_t wordoffset = int32_t(offsetAndSize >> 32);
          int64_t num_elements = *(int64_t *)(memory_address_ + offsets[position] + wordoffset); 
          int64_t header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
          for (auto j = 0; j < num_elements; j++) {
            bool is_null = IsNull(memory_address_ + offsets[position] + wordoffset + 8, j);
            if (is_null) {
              child_builder.AppendNull();
            } else {
                bool value = *(int64_t *)(memory_address_ + offsets[position] + wordoffset + header_in_bytes +
                           j * sizeof(int32_t));
                RETURN_NOT_OK(child_builder.Append(value));
            }
          }
          ARROW_RETURN_NOT_OK(parent_builder.Finish(array));
        }
      }
    }


    // std::unique_ptr<arrow::TypeTraits<arrow::ListType>::BuilderType> builder_;
    // std::unique_ptr<arrow::ArrayBuilder> array_builder;
    // arrow::MakeBuilder(pool, arrow::ListType{}, &array_builder);
    // builder_.reset(arrow::internal::checked_cast<arrow::TypeTraits<arrow::ListType>::BuilderType *>(array_builder.release()));

    return arrow::Status::OK();   
  }
  
}


arrow::Status CreateArrayData(std::shared_ptr<arrow::Schema> schema, int64_t num_rows, int32_t columnar_id, 
    int64_t fieldOffset, std::vector<int64_t>& offsets, int64_t* row_length_, uint8_t* memory_address_, std::shared_ptr<arrow::Array>* array_data) {
  
  


  auto field = schema->field(columnar_id);
  auto type = field->type();
  
  if (type->id() == arrow::BooleanType::type_id) 
  {
    arrow::BooleanBuilder builder;
    builder.Resize(num_rows);   
    bool value;
    for (auto i = 0; i < num_rows; i++) {
      bool is_null = IsNull(memory_address_ + offsets[i], columnar_id);
      if (is_null) {
        builder.UnsafeAppendNull();
        // SetNullAt(buffer_address, offsets[i], field_offset, col_index);
      } else {
        
        memcpy(&value, memory_address_ + offsets[i] + fieldOffset, sizeof(bool));
        // auto value = bool_array->Value(i);
        // memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
        builder.UnsafeAppend(value);
      }
    }
    return builder.Finish(array_data);
  }
  else if (type->id() == arrow::Int8Type::type_id)
  {    
    arrow::Int8Builder builder;
    builder.Resize(num_rows);   
    int8_t value;
    for (auto i = 0; i < num_rows; i++) {
      bool is_null = IsNull(memory_address_ + offsets[i], columnar_id);
      if (is_null) {
        builder.UnsafeAppendNull();
        // SetNullAt(buffer_address, offsets[i], field_offset, col_index);
      } else {   
        memcpy(&value, memory_address_ + offsets[i] + CalculateBitSetWidthInBytes(schema->num_fields()), sizeof(int8_t));
        // auto value = bool_array->Value(i);
        // memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
        builder.UnsafeAppend(value);
      }
    }
    return builder.Finish(array_data); 
  }
  else if (type->id() == arrow::Int16Type::type_id)
  {    
    arrow::Int16Builder builder;
    builder.Resize(num_rows);   
    int16_t value;
    for (auto i = 0; i < num_rows; i++) {
      bool is_null = IsNull(memory_address_ + offsets[i], columnar_id);
      if (is_null) {
        builder.UnsafeAppendNull();
        // SetNullAt(buffer_address, offsets[i], field_offset, col_index);
      } else {   
        memcpy(&value, memory_address_ + offsets[i] + CalculateBitSetWidthInBytes(schema->num_fields()), sizeof(int16_t));
        // auto value = bool_array->Value(i);
        // memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
        builder.UnsafeAppend(value);
      }
    }
    return builder.Finish(array_data); 
  }
  else if (type->id() == arrow::Int32Type::type_id)
  {    
    arrow::Int32Builder builder;
    builder.Resize(num_rows);   
    int8_t value;
    for (auto i = 0; i < num_rows; i++) {
      bool is_null = IsNull(memory_address_ + offsets[i], columnar_id);
      if (is_null) {
        builder.UnsafeAppendNull();
        // SetNullAt(buffer_address, offsets[i], field_offset, col_index);
      } else {   
        memcpy(&value, memory_address_ + offsets[i] + CalculateBitSetWidthInBytes(schema->num_fields()), sizeof(int32_t));
        // auto value = bool_array->Value(i);
        // memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
        builder.UnsafeAppend(value);
      }
    }
    return builder.Finish(array_data); 
  }


  // switch (type->id) {
  //   case arrow::Int16Type::type_id:
  //     auto dtype = dynamic_cast<arrow::Decimal128Type*>(type.get());
  //     int32_t precision = dtype->precision();
  //     break;
  //   }

  //   default:
  //     return arrow::Status::Invalid("Unsupported data type in ListArray: ");
  // }
   

  return arrow::Status::OK();
}



arrow::Status RowToColumnarConverter::Init() {

  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);
  for (auto i = 0; i < num_rows_; i++) {
    offsets_.push_back(0);
  }
  for (auto i = 0; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + row_length_[i - 1];
  }
  

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  auto num_fields = schema_->num_fields();

  for (auto i = 0; i < num_fields; i++) {
    auto field = schema_->field(i);
    std::shared_ptr<arrow::Array> array_data;
    int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, i);
    // RETURN_NOT_OK(CreateArrayData(schema_, num_rows_, i, field_offset, offsets_, row_length_, memory_address_,
    //                             &array_data));

    RETURN_NOT_OK(CreateArrayData2(schema_, num_rows_, i, field_offset, offsets_, row_length_, memory_address_,
                                &array_data, m_pool_));
    // arrow::ArrayData out_data;
    // out_data.length = num_rows_;
    // out_data.buffers.resize(2);

    // out_data.type = arrow::TypeTraits<arrow::BooleanType>::type_singleton();


    // RETURN_NOT_OK();
    arrays.push_back(array_data);
  }
  std::shared_ptr<arrow::RecordBatch> rb;
  rb = arrow::RecordBatch::Make(schema_, num_rows_, arrays);

  return arrow::Status::OK();

//  num_rows_ = rb_->num_rows();
//  num_cols_ = rb_->num_columns();
//  // Calculate the initial size
//  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);
//
//  int64_t fixed_size_per_row = CalculatedFixeSizePerRow(rb_->schema(), num_cols_);
//
//  // Initialize the offsets_ , lengths_, buffer_cursor_
//  for (auto i = 0; i < num_rows_; i++) {
//    lengths_.push_back(fixed_size_per_row);
//    offsets_.push_back(0);
//    buffer_cursor_.push_back(nullBitsetWidthInBytes_ + 8 * num_cols_);
//  }
//  // Calculated the lengths_
//  for (auto i = 0; i < num_cols_; i++) {
//    auto array = rb_->column(i);
//    if (arrow::is_binary_like(array->type_id())) {
//      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
//      using offset_type = typename arrow::BinaryType::offset_type;
//      offset_type length;
//      for (auto j = 0; j < num_rows_; j++) {
//        auto value = binary_array->GetValue(j, &length);
//        lengths_[j] += RoundNumberOfBytesToNearestWord(length);
//      }
//    }
//
//    // Each array has four parts in Spark UnsafeArrayData class:
//    // [numElements][null bits][values or offset&length][variable length portion]
//    // The array type is considered to be the variable col in the Spark UnsafeRow.
//    if (array->type_id() == arrow::ListType::type_id) {
//      auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
//      int32_t element_size_in_bytes = -1;
//      // header_in_bytes:  [numElements][null bits]
//      int32_t num_elements = 0, header_in_bytes = 0, fixed_part_in_bytes = 0,
//              variable_part_in_bytes = 0;
//      for (auto j = 0; j < num_rows_; j++) {
//        // Calculated the size of per row in list array
//        auto row_array = list_array->value_slice(j);
//        num_elements = row_array->length();
//        header_in_bytes = CalculateHeaderPortionInBytes(num_elements);
//
//        if (element_size_in_bytes == -1) {
//          // only calculated once
//          CalculatedElementSize(row_array->type_id(), &element_size_in_bytes);
//        }
//
//        fixed_part_in_bytes =
//            RoundNumberOfBytesToNearestWord(num_elements * element_size_in_bytes);
//
//        // If the type is binary like or decimal precision > 18, need to calculated the
//        // variable part size
//        if (arrow::is_binary_like(row_array->type_id())) {
//          auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(row_array);
//          using offset_type = typename arrow::BinaryType::offset_type;
//          offset_type length;
//          for (auto k = 0; k < num_elements; k++) {
//            if (!binary_array->IsNull(k)) {
//              auto value = binary_array->GetValue(k, &length);
//              variable_part_in_bytes += RoundNumberOfBytesToNearestWord(length);
//            }
//          }
//        }
//
//        if (row_array->type_id() == arrow::Decimal128Type::type_id) {
//          auto dtype = dynamic_cast<arrow::Decimal128Type*>(row_array->type().get());
//          int32_t precision = dtype->precision();
//          int32_t null_count = row_array->null_count();
//          // TODO: the size in UnsafeArrayData is not 16 and is the
//          // RoundNumberOfBytesToNearestWord(real size)
//          if (precision > 18) variable_part_in_bytes += 16 * (num_elements - null_count);
//        }
//
//        lengths_[j] += (header_in_bytes + fixed_part_in_bytes + variable_part_in_bytes);
//      }
//    }
//  }
//  // Calculated the offsets_  and total memory size based on lengths_
//  int64_t total_memory_size = lengths_[0];
//  for (auto i = 1; i < num_rows_; i++) {
//    offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
//    total_memory_size += lengths_[i];
//  }
//
//  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size, memory_pool_));
//
//  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);
//
//  buffer_address_ = buffer_->mutable_data();
//  return arrow::Status::OK();
}





}  // namespace columnartorow
}  // namespace sparkcolumnarplugin