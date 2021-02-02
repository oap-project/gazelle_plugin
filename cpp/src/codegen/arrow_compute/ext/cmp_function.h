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
#include "third_party/function.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

template <typename DataType, typename CType>
class TypedComparator {
 public:
  TypedComparator() {}

  ~TypedComparator() {}

  func::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    uint64_t null_total = 0;
    std::vector<std::shared_ptr<ArrayType>> typed_arrays;
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total += arrays[array_id]->null_count();
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
          bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
              typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
              typed_arrays[right_array_id]->IsNull(right_id);
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
          bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
              typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
              typed_arrays[right_array_id]->IsNull(right_id);
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
        bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
            typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
            typed_arrays[right_array_id]->IsNull(right_id);
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
        bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
            typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
            typed_arrays[right_array_id]->IsNull(right_id);
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

  func::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    uint64_t null_total = 0;
    std::vector<std::shared_ptr<ArrayType>> typed_arrays;
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total += arrays[array_id]->null_count();
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
          bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
              typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
              typed_arrays[right_array_id]->IsNull(right_id);
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
          bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
              typed_arrays[left_array_id]->IsNull(left_id);
          bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
              typed_arrays[right_array_id]->IsNull(right_id);
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
        bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
            typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
            typed_arrays[right_array_id]->IsNull(right_id);
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
        bool is_left_null = typed_arrays[left_array_id]->null_count() > 0 && 
            typed_arrays[left_array_id]->IsNull(left_id);
        bool is_right_null = typed_arrays[right_array_id]->null_count() > 0 &&
            typed_arrays[right_array_id]->IsNull(right_id);
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
    std::vector<func::function<void(int, int, int64_t, int64_t, int&)>>& cmp_functions) {
  for (int i = 0; i < key_field_list.size(); i++) {
    auto type = key_field_list[i]->type();
    int key_col_id = key_index_list[i];
    arrow::ArrayVector col = array_vectors[key_col_id];
    bool asc = sort_directions[i];
    bool nulls_first = nulls_order[i];
    if (type->id() == arrow::Type::STRING) {
      auto comparator_ptr = 
          std::make_shared<StringComparator<arrow::StringType, std::string>>();
      cmp_functions.push_back(
          comparator_ptr->GetCompareFunc(col, asc, nulls_first));
    } else {
      switch (type->id()) {
  #define PROCESS(InType)                                                           \
      case InType::type_id: {                                                       \
        using CType = typename arrow::TypeTraits<InType>::CType;                    \
        auto comparator_ptr = std::make_shared<TypedComparator<InType, CType>>();   \
        cmp_functions.push_back(comparator_ptr->GetCompareFunc(col, asc, nulls_first));\
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
