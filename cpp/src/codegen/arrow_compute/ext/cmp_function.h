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

#include <arrow/type.h>
#include <arrow/array.h>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

struct CompareFunction {
  CompareFunction(const std::vector<arrow::ArrayVector>& array_vectors, 
                  std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                  std::vector<int> key_index_list) {
    for (int key_idx = 0; key_idx < key_field_list.size(); key_idx++) {
      auto field = key_field_list[key_idx];
      int key_col_id = key_index_list[key_idx];
      arrow::ArrayVector col = array_vectors[key_col_id];
      if (field->type()->id() == arrow::Type::UINT8) {
        std::vector<std::shared_ptr<arrow::UInt8Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::UInt8Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        uint8_array_vectors_.push_back(arrays);
        uint8_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::INT8) {
        std::vector<std::shared_ptr<arrow::Int8Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Int8Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        int8_array_vectors_.push_back(arrays);
        int8_key_idx.push_back(key_idx);
       } else if (field->type()->id() == arrow::Type::UINT16) {
        std::vector<std::shared_ptr<arrow::UInt16Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::UInt16Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        uint16_array_vectors_.push_back(arrays);
        uint16_key_idx.push_back(key_idx);
        } else if (field->type()->id() == arrow::Type::INT16) {
        std::vector<std::shared_ptr<arrow::Int16Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Int16Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        int16_array_vectors_.push_back(arrays);
        int16_key_idx.push_back(key_idx);  
      } else if (field->type()->id() == arrow::Type::UINT32) {
        std::vector<std::shared_ptr<arrow::UInt32Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::UInt32Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        uint32_array_vectors_.push_back(arrays);
        uint32_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::INT32) {
        std::vector<std::shared_ptr<arrow::Int32Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Int32Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        int32_array_vectors_.push_back(arrays);
        int32_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::UINT64) {
        std::vector<std::shared_ptr<arrow::UInt64Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::UInt64Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        uint64_array_vectors_.push_back(arrays);
        uint64_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::INT64) {
        std::vector<std::shared_ptr<arrow::Int64Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Int64Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        int64_array_vectors_.push_back(arrays);
        int64_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::DATE32) {
        std::vector<std::shared_ptr<arrow::Date32Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Date32Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        date32_array_vectors_.push_back(arrays);
        date32_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::DATE64) {
        std::vector<std::shared_ptr<arrow::Date64Array>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::Date64Array>(col[array_id]);
          arrays.push_back(typed_array);
        }
        date64_array_vectors_.push_back(arrays);
        date64_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::FLOAT) {
        std::vector<std::shared_ptr<arrow::FloatArray>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::FloatArray>(col[array_id]);
          arrays.push_back(typed_array);
        }
        float_array_vectors_.push_back(arrays);
        float_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::DOUBLE) {
        std::vector<std::shared_ptr<arrow::DoubleArray>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::DoubleArray>(col[array_id]);
          arrays.push_back(typed_array);
        }
        double_array_vectors_.push_back(arrays);
        double_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::BOOL) {
        std::vector<std::shared_ptr<arrow::BooleanArray>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::BooleanArray>(col[array_id]);
          arrays.push_back(typed_array);
        }
        bool_array_vectors_.push_back(arrays);
        bool_key_idx.push_back(key_idx);
      } else if (field->type()->id() == arrow::Type::STRING) {
        std::vector<std::shared_ptr<arrow::StringArray>> arrays;
        for (int array_id = 0; array_id < col.size(); array_id++) {
          auto typed_array = 
              std::static_pointer_cast<arrow::StringArray>(col[array_id]);
          arrays.push_back(typed_array);
        }
        str_array_vectors_.push_back(arrays);
        str_key_idx.push_back(key_idx);
      }
    }
  }

  /************************ Compare Functions for UINT8 ************************/
  void cmp_uint8_no_null_asc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_uint8_no_null_desc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_uint8_nulls_first_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    bool is_left_null = uint8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint8_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    bool is_left_null = uint8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint8_nulls_first_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    bool is_left_null = uint8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_uint8_nulls_last_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint8_key_idx.begin(), uint8_key_idx.end(), key_idx);
    int idx = it - uint8_key_idx.begin();
    bool is_left_null = uint8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
 /************************ Compare Functions for INT8 ************************/
  void cmp_int8_no_null_asc(int key_idx,
                            int left_array_id,
                            int right_array_id,
                            int64_t left_id,
                            int64_t right_id,
                            int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_int8_no_null_desc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_int8_nulls_first_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    bool is_left_null = int8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int8_nulls_last_asc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    bool is_left_null = int8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int8_nulls_first_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    bool is_left_null = int8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_int8_nulls_last_desc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(int8_key_idx.begin(), int8_key_idx.end(), key_idx);
    int idx = it - int8_key_idx.begin();
    bool is_left_null = int8_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int8_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int8_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int8_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for UINT16 ************************/
  void cmp_uint16_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_uint16_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_uint16_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    bool is_left_null = uint16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint16_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    bool is_left_null = uint16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint16_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    bool is_left_null = uint16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_uint16_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint16_key_idx.begin(), uint16_key_idx.end(), key_idx);
    int idx = it - uint16_key_idx.begin();
    bool is_left_null = uint16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for INT16 ************************/
  void cmp_int16_no_null_asc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_int16_no_null_desc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_int16_nulls_first_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    bool is_left_null = int16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int16_nulls_last_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    bool is_left_null = int16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int16_nulls_first_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    bool is_left_null = int16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_int16_nulls_last_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int16_key_idx.begin(), int16_key_idx.end(), key_idx);
    int idx = it - int16_key_idx.begin();
    bool is_left_null = int16_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int16_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int16_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int16_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for UINT32 ************************/
  void cmp_uint32_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_uint32_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_uint32_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    bool is_left_null = uint32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint32_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    bool is_left_null = uint32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint32_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    bool is_left_null = uint32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_uint32_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint32_key_idx.begin(), uint32_key_idx.end(), key_idx);
    int idx = it - uint32_key_idx.begin();
    bool is_left_null = uint32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for INT32 ************************/
  void cmp_int32_no_null_asc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_int32_no_null_desc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_int32_nulls_first_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    bool is_left_null = int32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int32_nulls_last_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    bool is_left_null = int32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int32_nulls_first_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    bool is_left_null = int32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_int32_nulls_last_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int32_key_idx.begin(), int32_key_idx.end(), key_idx);
    int idx = it - int32_key_idx.begin();
    bool is_left_null = int32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for UINT64 ************************/
  void cmp_uint64_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_uint64_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_uint64_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    bool is_left_null = uint64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint64_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    bool is_left_null = uint64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_uint64_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    bool is_left_null = uint64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_uint64_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(uint64_key_idx.begin(), uint64_key_idx.end(), key_idx);
    int idx = it - uint64_key_idx.begin();
    bool is_left_null = uint64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = uint64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = uint64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = uint64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for INT64 ************************/
  void cmp_int64_no_null_asc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(int64_key_idx.begin(), int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_int64_no_null_desc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(int64_key_idx.begin(), int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_int64_nulls_first_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int64_key_idx.begin(), int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    bool is_left_null = int64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int64_nulls_last_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(int64_key_idx.begin(),int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    bool is_left_null = int64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_int64_nulls_first_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(int64_key_idx.begin(), int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    bool is_left_null = int64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_int64_nulls_last_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(int64_key_idx.begin(), int64_key_idx.end(), key_idx);
    int idx = it - int64_key_idx.begin();
    bool is_left_null = int64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = int64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = int64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = int64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for DATE32 ************************/
  void cmp_date32_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_date32_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_date32_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    bool is_left_null = date32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_date32_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    bool is_left_null = date32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_date32_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    bool is_left_null = date32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_date32_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(date32_key_idx.begin(), date32_key_idx.end(), key_idx);
    int idx = it - date32_key_idx.begin();
    bool is_left_null = date32_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date32_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = date32_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date32_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for DATE64 ************************/
  void cmp_date64_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_date64_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_date64_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    bool is_left_null = date64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_date64_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    bool is_left_null = date64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_date64_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    bool is_left_null = date64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_date64_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(date64_key_idx.begin(), date64_key_idx.end(), key_idx);
    int idx = it - date64_key_idx.begin();
    bool is_left_null = date64_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = date64_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = date64_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = date64_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }  
/************************ Compare Functions for FLOAT ************************/
  void cmp_float_no_null_asc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(float_key_idx.begin(), float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_float_no_null_desc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(float_key_idx.begin(), float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_float_nulls_first_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(float_key_idx.begin(), float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    bool is_left_null = float_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = float_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_float_nulls_last_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(float_key_idx.begin(),float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    bool is_left_null = float_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = float_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_float_nulls_first_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(float_key_idx.begin(), float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    bool is_left_null = float_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = float_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_float_nulls_last_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(float_key_idx.begin(), float_key_idx.end(), key_idx);
    int idx = it - float_key_idx.begin();
    bool is_left_null = float_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = float_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = float_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = float_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
/************************ Compare Functions for DOUBLE ************************/
  void cmp_double_no_null_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(double_key_idx.begin(), double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_double_no_null_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(double_key_idx.begin(), double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_double_nulls_first_asc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(double_key_idx.begin(), double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    bool is_left_null = double_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = double_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_double_nulls_last_asc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(double_key_idx.begin(),double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    bool is_left_null = double_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = double_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_double_nulls_first_desc(int key_idx,
                                   int left_array_id,
                                   int right_array_id,
                                   int64_t left_id,
                                   int64_t right_id,
                                   int& cmp_res) const {
    auto it = find(double_key_idx.begin(), double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    bool is_left_null = double_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = double_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_double_nulls_last_desc(int key_idx,
                                  int left_array_id,
                                  int right_array_id,
                                  int64_t left_id,
                                  int64_t right_id,
                                  int& cmp_res) const {
    auto it = find(double_key_idx.begin(), double_key_idx.end(), key_idx);
    int idx = it - double_key_idx.begin();
    bool is_left_null = double_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = double_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = double_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = double_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for BOOL ************************/
  void cmp_bool_no_null_asc(int key_idx,
                            int left_array_id,
                            int right_array_id,
                            int64_t left_id,
                            int64_t right_id,
                            int& cmp_res) const {
    auto it = find(bool_key_idx.begin(), bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_bool_no_null_desc(int key_idx,
                             int left_array_id,
                             int right_array_id,
                             int64_t left_id,
                             int64_t right_id,
                             int& cmp_res) const {
    auto it = find(bool_key_idx.begin(), bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
    auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_bool_nulls_first_asc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(bool_key_idx.begin(), bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    bool is_left_null = bool_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = bool_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_bool_nulls_last_asc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(bool_key_idx.begin(),bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    bool is_left_null = bool_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = bool_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_bool_nulls_first_desc(int key_idx,
                                 int left_array_id,
                                 int right_array_id,
                                 int64_t left_id,
                                 int64_t right_id,
                                 int& cmp_res) const {
    auto it = find(bool_key_idx.begin(), bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    bool is_left_null = bool_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = bool_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_bool_nulls_last_desc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(bool_key_idx.begin(), bool_key_idx.end(), key_idx);
    int idx = it - bool_key_idx.begin();
    bool is_left_null = bool_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = bool_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = bool_array_vectors_[idx][left_array_id]->GetView(left_id);
        auto right = bool_array_vectors_[idx][right_array_id]->GetView(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  /************************ Compare Functions for STRING ************************/
  void cmp_str_no_null_asc(int key_idx,
                           int left_array_id,
                           int right_array_id,
                           int64_t left_id,
                           int64_t right_id,
                           int& cmp_res) const {
    auto it = find(str_key_idx.begin(), str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
    auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
    if (left != right) {
      cmp_res = left < right;
    }
  }
  void cmp_str_no_null_desc(int key_idx,
                            int left_array_id,
                            int right_array_id,
                            int64_t left_id,
                            int64_t right_id,
                            int& cmp_res) const {
    auto it = find(str_key_idx.begin(), str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
    auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
    if (left != right) {
      cmp_res = left > right;
    }
  }
  void cmp_str_nulls_first_asc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(str_key_idx.begin(), str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    bool is_left_null = str_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = str_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
        auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_str_nulls_last_asc(int key_idx,
                              int left_array_id,
                              int right_array_id,
                              int64_t left_id,
                              int64_t right_id,
                              int& cmp_res) const {
    auto it = find(str_key_idx.begin(),str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    bool is_left_null = str_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = str_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
        auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
        if (left != right) {
          cmp_res = left < right;
        }
      }
    }
  }
  void cmp_str_nulls_first_desc(int key_idx,
                                int left_array_id,
                                int right_array_id,
                                int64_t left_id,
                                int64_t right_id,
                                int& cmp_res) const {
    auto it = find(str_key_idx.begin(), str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    bool is_left_null = str_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = str_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 1;
      } else if (is_right_null) {
        cmp_res = 0;
      } else {
        auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
        auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }
  void cmp_str_nulls_last_desc(int key_idx,
                               int left_array_id,
                               int right_array_id,
                               int64_t left_id,
                               int64_t right_id,
                               int& cmp_res) const {
    auto it = find(str_key_idx.begin(), str_key_idx.end(), key_idx);
    int idx = it - str_key_idx.begin();
    bool is_left_null = str_array_vectors_[idx][left_array_id]->IsNull(left_id);
    bool is_right_null = str_array_vectors_[idx][right_array_id]->IsNull(right_id);
    if (!is_left_null || !is_right_null) {
      if (is_left_null) {
        cmp_res = 0;
      } else if (is_right_null) {
        cmp_res = 1;
      } else {
        auto left = str_array_vectors_[idx][left_array_id]->GetString(left_id);
        auto right = str_array_vectors_[idx][right_array_id]->GetString(right_id);
        if (left != right) {
          cmp_res = left > right;
        }
      }
    }
  }

  private:
  std::vector<std::vector<std::shared_ptr<arrow::UInt8Array>>> uint8_array_vectors_;
  std::vector<int> uint8_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Int8Array>>> int8_array_vectors_;
  std::vector<int> int8_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::UInt16Array>>> uint16_array_vectors_;
  std::vector<int> uint16_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Int16Array>>> int16_array_vectors_;
  std::vector<int> int16_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::UInt32Array>>> uint32_array_vectors_;
  std::vector<int> uint32_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Int32Array>>> int32_array_vectors_;
  std::vector<int> int32_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::UInt64Array>>> uint64_array_vectors_;
  std::vector<int> uint64_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Int64Array>>> int64_array_vectors_;
  std::vector<int> int64_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Date32Array>>> date32_array_vectors_;
  std::vector<int> date32_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::Date64Array>>> date64_array_vectors_;
  std::vector<int> date64_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::FloatArray>>> float_array_vectors_;
  std::vector<int> float_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::DoubleArray>>> double_array_vectors_;
  std::vector<int> double_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::BooleanArray>>> bool_array_vectors_;
  std::vector<int> bool_key_idx;
  std::vector<std::vector<std::shared_ptr<arrow::StringArray>>> str_array_vectors_;
  std::vector<int> str_key_idx;
};

void GenCmpFunction(std::vector<std::shared_ptr<arrow::Field>> key_field_list, 
                    std::vector<bool> sort_directions, 
                    std::vector<bool> nulls_order,
                    std::vector<uint64_t> null_total_list,
                    std::vector<std::function<void(const CompareFunction&, 
                    int, int, int, int64_t, int64_t, int&)>>& cmp_functions) {
  std::function<void(const CompareFunction&, int, int, 
                     int, int64_t, int64_t, int&)> cmp_func;
  for (int key_idx = 0; key_idx < key_field_list.size(); key_idx++) {
    auto field = key_field_list[key_idx];
    bool asc = sort_directions[key_idx];
    bool nulls_first = nulls_order[key_idx];
    uint64_t null_count = null_total_list[key_idx];
    if (field->type()->id() == arrow::Type::UINT8) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_uint8_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_uint8_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint8_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint8_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint8_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint8_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::INT8) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_int8_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_int8_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int8_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int8_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int8_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int8_nulls_last_desc;
          }
        }
      }      
    } else if (field->type()->id() == arrow::Type::UINT16) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_uint16_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_uint16_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint16_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint16_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint16_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint16_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::INT16) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_int16_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_int16_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int16_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int16_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int16_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int16_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::UINT32) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_uint32_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_uint32_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint32_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint32_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint32_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint32_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::INT32) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_int32_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_int32_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int32_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int32_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int32_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int32_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::UINT64) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_uint64_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_uint64_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint64_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint64_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_uint64_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_uint64_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::INT64) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_int64_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_int64_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int64_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int64_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_int64_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_int64_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::DATE32) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_date32_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_date32_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_date32_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_date32_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_date32_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_date32_nulls_last_desc;
          }
        }
      }      
    } else if (field->type()->id() == arrow::Type::DATE64) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_date64_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_date64_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_date64_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_date64_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_date64_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_date64_nulls_last_desc;
          }
        }
      }      
    } else if (field->type()->id() == arrow::Type::FLOAT) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_float_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_float_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_float_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_float_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_float_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_float_nulls_last_desc;
          }
        }
      }
    } else if (field->type()->id() == arrow::Type::DOUBLE) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_double_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_double_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_double_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_double_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_double_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_double_nulls_last_desc;
          }
        }
      }      
    } else if (field->type()->id() == arrow::Type::BOOL) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_bool_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_bool_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_bool_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_bool_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_bool_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_bool_nulls_last_desc;
          }
        }
      }      
    } else if (field->type()->id() == arrow::Type::STRING) {
      if (null_count == 0) {
        if (asc) {
          cmp_func = &CompareFunction::cmp_str_no_null_asc;
        } else {
          cmp_func = &CompareFunction::cmp_str_no_null_desc;
        }
      } else {
        if (nulls_first) {
          if (asc) {
            cmp_func = &CompareFunction::cmp_str_nulls_first_asc;
          } else {
            cmp_func = &CompareFunction::cmp_str_nulls_first_desc;
          }
        } else {
          if (asc) {
            cmp_func = &CompareFunction::cmp_str_nulls_last_asc;
          } else {
            cmp_func = &CompareFunction::cmp_str_nulls_last_desc;
          }
        }
      }      
    }
    cmp_functions.push_back(cmp_func);
  }
}

template <typename DataType, typename CType>
class TypedComparator {
 public:
  TypedComparator() {}

  ~TypedComparator() {}

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, 
      bool nulls_first, uint64_t null_total) {
    // cast arrays into typed arrays    
    std::vector<std::shared_ptr<ArrayType>> typed_arrays;
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      auto typed_array = std::dynamic_pointer_cast<ArrayType>(arrays[array_id]);
      typed_arrays.push_back(typed_array);
    }
    if (null_total == 0) {
      if (asc) {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          CType left = typed_arrays[left_array_id]->GetView(left_id);
          CType right = typed_arrays[right_array_id]->GetView(right_id);
          if (left != right) {
            cmp_res = left < right;
          }
        };
      } else {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          CType left = typed_arrays[left_array_id]->GetView(left_id);
          CType right = typed_arrays[right_array_id]->GetView(right_id);
          if (left != right) {
            cmp_res = left > right;
          }
        };
      }
    } else if (asc) {
      if (nulls_first) {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
              cmp_res = 1;
            } else if (is_right_null) {
              cmp_res = 0;
            } else {
              CType left = typed_arrays[left_array_id]->GetView(left_id);
              CType right = typed_arrays[right_array_id]->GetView(right_id);
              if (left != right) {
                cmp_res = left < right;
              }
            }
          }
        };
      } else {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
              cmp_res = 0;
            } else if (is_right_null) {
              cmp_res = 1;
            } else {
              CType left = typed_arrays[left_array_id]->GetView(left_id);
              CType right = typed_arrays[right_array_id]->GetView(right_id);
              if (left != right) {
                cmp_res = left < right;
              }
            }
          }
        };
      }
    } else if (nulls_first) {
      return [=](int left_array_id, int right_array_id, 
                 int64_t left_id, int64_t right_id, int& cmp_res) {
        bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
        if (!is_left_null || !is_right_null) {
          if (is_left_null) {
            cmp_res = 1;
          } else if (is_right_null) {
            cmp_res = 0;
          } else {
            CType left = typed_arrays[left_array_id]->GetView(left_id);
            CType right = typed_arrays[right_array_id]->GetView(right_id);
            if (left != right) {
              cmp_res = left > right;
            }
          }
        }
      };
    } else {
      return [=](int left_array_id, int right_array_id, 
                 int64_t left_id, int64_t right_id, int& cmp_res) {
        bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
        if (!is_left_null || !is_right_null) {
          if (is_left_null) {
            cmp_res = 0;
          } else if (is_right_null) {
            cmp_res = 1;
          } else {
            CType left = typed_arrays[left_array_id]->GetView(left_id);
            CType right = typed_arrays[right_array_id]->GetView(right_id);
            if (left != right) {
              cmp_res = left > right;
            }
          }
        }
      };
    }
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
};

template <typename DataType, typename CType>
class StringComparator {
 public:
  StringComparator() {}

  ~StringComparator() {}

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, 
      bool nulls_first, uint64_t null_total) {
    // cast arrays into typed arrays
    std::vector<std::shared_ptr<ArrayType>> typed_arrays;
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      auto typed_array = std::dynamic_pointer_cast<ArrayType>(arrays[array_id]);
      typed_arrays.push_back(typed_array);
    }    
    if (null_total == 0) {
      if (asc) {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          CType left = typed_arrays[left_array_id]->GetString(left_id);
          CType right = typed_arrays[right_array_id]->GetString(right_id);
          if (left != right) {
            cmp_res = left < right;
          }
        };
      } else {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          CType left = typed_arrays[left_array_id]->GetString(left_id);
          CType right = typed_arrays[right_array_id]->GetString(right_id);
          if (left != right) {
            cmp_res = left > right;
          }
        };
      }
    } else if (asc) {
      if (nulls_first) {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
              cmp_res = 1;
            } else if (is_right_null) {
              cmp_res = 0;
            } else {
              CType left = typed_arrays[left_array_id]->GetString(left_id);
              CType right = typed_arrays[right_array_id]->GetString(right_id);
              if (left != right) {
                cmp_res = left < right;
              }
            }
          }
        };
      } else {
        return [=](int left_array_id, int right_array_id, 
                   int64_t left_id, int64_t right_id, int& cmp_res) {
          bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
              cmp_res = 0;
            } else if (is_right_null) {
              cmp_res = 1;
            } else {
              CType left = typed_arrays[left_array_id]->GetString(left_id);
              CType right = typed_arrays[right_array_id]->GetString(right_id);
              if (left != right) {
                cmp_res = left < right;
              }
            }
          }
        };
      }
    } else if (nulls_first) {
      return [=](int left_array_id, int right_array_id, 
                 int64_t left_id, int64_t right_id, int& cmp_res) {
        bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
        if (!is_left_null || !is_right_null) {
          if (is_left_null) {
            cmp_res = 1;
          } else if (is_right_null) {
            cmp_res = 0;
          } else {
            CType left = typed_arrays[left_array_id]->GetString(left_id);
            CType right = typed_arrays[right_array_id]->GetString(right_id);
            if (left != right) {
              cmp_res = left > right;
            }
          }
        }
      };
    } else {
      return [=](int left_array_id, int right_array_id, 
                 int64_t left_id, int64_t right_id, int& cmp_res) {
        bool is_left_null = typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->IsNull(right_id);
        if (!is_left_null || !is_right_null) {
          if (is_left_null) {
            cmp_res = 0;
          } else if (is_right_null) {
            cmp_res = 1;
          } else {
            CType left = typed_arrays[left_array_id]->GetString(left_id);
            CType right = typed_arrays[right_array_id]->GetString(right_id);
            if (left != right) {
              cmp_res = left > right;
            }
          }
        }
      };
    }
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
};

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)
static arrow::Status MakeCmpFunction(
    const std::vector<arrow::ArrayVector>& array_vectors,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<int> key_index_list,
    std::vector<bool> sort_directions, 
    std::vector<bool> nulls_order,
    std::vector<uint64_t> null_total_list,
    std::vector<std::function<void(int, int, int64_t, int64_t, int&)>>& cmp_functions) {
  for (int i = 0; i < key_field_list.size(); i++) {
    auto type = key_field_list[i]->type();
    int key_col_id = key_index_list[i];
    arrow::ArrayVector col = array_vectors[key_col_id];
    bool asc = sort_directions[i];
    bool nulls_first = nulls_order[i];
    uint64_t null_total = null_total_list[i];
    if (type->id() == arrow::Type::STRING) {
      auto comparator_ptr = 
          std::make_shared<StringComparator<arrow::StringType, std::string>>();
      cmp_functions.push_back(
          comparator_ptr->GetCompareFunc(col, asc, nulls_first, null_total));
    } else {
      switch (type->id()) {
  #define PROCESS(InType)                                                           \
      case InType::type_id: {                                                       \
        using CType = typename arrow::TypeTraits<InType>::CType;                    \
        auto comparator_ptr = std::make_shared<TypedComparator<InType, CType>>();   \
        cmp_functions.push_back(comparator_ptr->GetCompareFunc(col, asc, nulls_first, null_total));\
      } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
  #undef PROCESS
        default: {
          std::cout << "MakeCmpFunction type not supported, type is " << type << std::endl;
        } break;
      }
    }
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES


}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
