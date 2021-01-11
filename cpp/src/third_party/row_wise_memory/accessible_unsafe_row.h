#pragma once

#include <arrow/util/decimal.h>
#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include "third_party/row_wise_memory/native_memory.h"
#include <vector>
#include <arrow/type.h>

#define TEMP_ACCESSIBLE_UNSAFEROW_BUFFER_SIZE 128
#define FIXED_UNSAFEROW_NUMERIC_SIZE 8

/* Accessible Unsafe Row Layout
 * (This accessible unsafe row is used to append all fields data as easy-to-access memory)
 *
 * | validity | col 0 | col 1 | col 2 | ...
 * explain:
 * validity: n fields = (n/8 + 1) bytes
 * col: each col has variable size
 * cursor_: used to point to numeric pos, as well as str length and offset pos
 * str_cursor_: used to point to str pos
 *
 */
struct AccessibleUnsafeRow {
  int numFields_;
  char* data = nullptr;
  int validity_size_;
  int cursor_ = 0;
  int str_cursor_ = 0;
  std::vector<std::shared_ptr<arrow::Field>> col_field_list_;

  AccessibleUnsafeRow() {}
  AccessibleUnsafeRow(std::vector<std::shared_ptr<arrow::Field>> col_field_list)
      : numFields_(col_field_list.size()),
        col_field_list_(col_field_list) {
    validity_size_ = (numFields_ / 8) + 1;
    str_cursor_ = col_field_list.size() * FIXED_UNSAFEROW_NUMERIC_SIZE;
    data = (char*)nativeMalloc(TEMP_ACCESSIBLE_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
    memset(data, 0, validity_size_);
  }
  ~AccessibleUnsafeRow() {
    if (data) {
      nativeFree(data);
    }
  }
  int sizeInBytes() { return validity_size_ + str_cursor_; }
  void reset() {
    memset(data, 0, validity_size_ + str_cursor_);
    cursor_ = 0;
    str_cursor_ = 0;
  }
  bool isNullExists() {
    for (int i = 0; i < ((numFields_ / 8) + 1); i++) {
      if (data[i] != 0) return true;
    }
    return false;
  }
  bool isNullAt(int index) {
    assert((index >= 0) && (index < numFields_));
    auto bitSetIdx = index >> 3;  // mod 8
    bool is_null = (*(data + bitSetIdx) & kBitmask[index % 8]) != 0;
    return is_null;
  }
  auto getData() {
    return data;
  }
  template <typename T>
  int compareInternal(T left, T right, bool asc, bool nulls_first) {
    int comparison;
    if (left == right) {
      comparison = 2;
    } else {
      if (asc) {
        comparison = left < right;
      } else {
        comparison = left > right;
      }
    }
    return comparison;
  }
  int compareInternal(std::string& left, std::string& right, bool asc, bool nulls_first) {
    int comparison;
    if (left == right) {
      comparison = 2;
    } else {
      if (asc) {
        comparison = left < right;
      } else {
        comparison = left > right;
      }
    }
    return comparison;
  }
  int compare(std::shared_ptr<AccessibleUnsafeRow> row_to_compare, int key_idx, 
      bool asc, bool nulls_first) {
    auto field = col_field_list_[key_idx];
    bool is_left_null = isNullAt(key_idx);
    bool is_right_null = row_to_compare->isNullAt(key_idx);
    if (is_left_null && is_right_null) {
      return 2;
    } else if (is_left_null) {
      return nulls_first ? 1 : 0;
    } else if (is_right_null) {
      return nulls_first ? 0 : 1;
    }
    int offset = validity_size_ + key_idx * FIXED_UNSAFEROW_NUMERIC_SIZE;
    if (field->type()->id() == arrow::Type::UINT8) {
      auto left = *((uint8_t*)(data + offset));
      auto right = *((uint8_t*)(row_to_compare->getData() + offset));
      return compareInternal<uint8_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::INT8) {
      auto left = *((int8_t*)(data + offset));
      auto right = *((int8_t*)(row_to_compare->getData() + offset));
      return compareInternal<int8_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::UINT16) {
      auto left = *((uint16_t*)(data + offset));
      auto right = *((uint16_t*)(row_to_compare->getData() + offset));
      return compareInternal<uint16_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::INT16) {
      auto left = *((int16_t*)(data + offset));
      auto right = *((int16_t*)(row_to_compare->getData() + offset));
      return compareInternal<int16_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::UINT32) {
      auto left = *((uint32_t*)(data + offset));
      auto right = *((uint32_t*)(row_to_compare->getData() + offset));
      return compareInternal<uint32_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::INT32) {
      auto left = *((int*)(data + offset));
      auto right = *((int*)(row_to_compare->getData() + offset));
      return compareInternal<int>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::UINT64) {
      auto left = *((uint64_t*)(data + offset));
      auto right = *((uint64_t*)(row_to_compare->getData() + offset));
      return compareInternal<uint64_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::INT64) {
      auto left = *((int64_t*)(data + offset));
      auto right = *((int64_t*)(row_to_compare->getData() + offset));
      return compareInternal<int64_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::DATE32) {
      auto left = *((int*)(data + offset));
      auto right = *((int*)(row_to_compare->getData() + offset));
      return compareInternal<int>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::DATE64) {
      auto left = *((int64_t*)(data + offset));
      auto right = *((int64_t*)(row_to_compare->getData() + offset));
      return compareInternal<int64_t>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::DOUBLE) {
      auto left = *((double*)(data + offset));
      auto right = *((double*)(row_to_compare->getData() + offset));
      return compareInternal<double>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::FLOAT) {
      auto left = *((float*)(data + offset));
      auto right = *((float*)(row_to_compare->getData() + offset));
      return compareInternal<float>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::BOOL) {
      auto left = *((bool*)(data + offset));
      auto right = *((bool*)(row_to_compare->getData() + offset));
      return compareInternal<bool>(left, right, asc, nulls_first);
    } else if (field->type()->id() == arrow::Type::STRING) {
      // Firstly, we need to know the length and offset of the string
      int str_len_cursor = validity_size_ + key_idx * FIXED_UNSAFEROW_NUMERIC_SIZE;
      int str_offset_cursor = validity_size_ + key_idx * FIXED_UNSAFEROW_NUMERIC_SIZE + 
          FIXED_UNSAFEROW_NUMERIC_SIZE / 2;
      std::string left;
      // length of left string
      int left_bytes = *((int*)(data + str_len_cursor));
      if (left_bytes != 0) {
        // offset of left string
        int left_offset = *((int*)(data + str_offset_cursor));
        // get the left string
        for (int i = 0; i < left_bytes; i++) {
          char letter = *((char*)(data + validity_size_ + left_offset + i));
          left.push_back(letter);
        }
      }
      std::string right;
      // length of right string
      int right_bytes = *((int*)(row_to_compare->getData() + str_len_cursor));
      if (right_bytes != 0) {
        // offset of right string
        int right_offset = *((int*)(row_to_compare->getData() + str_offset_cursor));
        // get the right string
        for (int i = 0; i < right_bytes; i++) {
          char letter = 
              *((char*)(row_to_compare->getData() + validity_size_ + right_offset + i));
          right.push_back(letter);
        }
      }
      return compareInternal(left, right, asc, nulls_first);
    }
    std::cout << "Unsupported type: " << field->type() << std::endl;
    return -1;
  }
};

static inline int calculateBitSetWidthInBytesAccessible(int numFields) {
  return ((numFields / 8) + 1);
}

static inline int getSizeInBytesAccessible(AccessibleUnsafeRow* row) { 
  return row->validity_size_ + row->str_cursor_;
}

static inline int roundNumberOfBytesToNearestWordAccessible(int numBytes) {
  int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }
}

static inline void zeroOutPaddingBytesAccessible(AccessibleUnsafeRow* row, int numBytes) {
  if ((numBytes & 0x07) > 0) {
    *((int64_t*)(char*)(row->data + row->validity_size_ + row->str_cursor_ + 
                ((numBytes >> 3) << 3))) = 0L;
  }
}

static inline void setNullAtAccessible(AccessibleUnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields_));
  auto bitSetIdx = index >> 3;  // mod 8
  *(row->data + bitSetIdx) |= kBitmask[index % 8];
  row->cursor_ += FIXED_UNSAFEROW_NUMERIC_SIZE;
}

template <typename T>
using is_number_alike = std::integral_constant<bool, std::is_arithmetic<T>::value ||
                                               std::is_floating_point<T>::value>;

template <typename T, typename std::enable_if_t<is_number_alike<T>::value>* = nullptr>
static inline void appendToAccessibleUnsafeRow(
    AccessibleUnsafeRow* row, const int& index, const T& val) {
  *((T*)(row->data + row->validity_size_ + row->cursor_)) = val;
  // null value also takes 8 empty bytes
  row->cursor_ += FIXED_UNSAFEROW_NUMERIC_SIZE;
}

static inline void appendToAccessibleUnsafeRow(
    AccessibleUnsafeRow* row, const int& index, const std::string& str) {
  // For string, four bytes are used to store length, and other four bytes are used to 
  // store offset.
  int numBytes = str.size();
  *((int*)(row->data + row->validity_size_ + row->cursor_)) = numBytes;
  int offset = row->str_cursor_;
  *((int*)(row->data + row->validity_size_ + row->cursor_ + 
      FIXED_UNSAFEROW_NUMERIC_SIZE / 2)) = offset;
  if (numBytes > 0) {
    memcpy(row->data + row->validity_size_ + row->str_cursor_, 
           str.c_str(), numBytes);
  }
  // move the cursor forward.
  row->cursor_ += FIXED_UNSAFEROW_NUMERIC_SIZE;
  row->str_cursor_ += numBytes;
}

class RowComparator {
 public:
  RowComparator(
      std::vector<std::vector<std::shared_ptr<AccessibleUnsafeRow>>>& unsafe_rows, 
      std::vector<bool> sort_directions, 
      std::vector<bool> nulls_order) 
      : unsafe_rows_(unsafe_rows),
        sort_directions_(sort_directions),
        nulls_order_(nulls_order) {
      }

  int compareInternal(int left_array_id, int64_t left_id, 
                      int right_array_id, int64_t right_id) {
    int key_idx = 0;
    int keys_num = sort_directions_.size();
    // In comparison, 2 represents equal, 1 represents true, 0 represents false
    while (key_idx < keys_num) {
      bool asc = sort_directions_[key_idx];
      bool nulls_first = nulls_order_[key_idx];
      int comparison = unsafe_rows_[left_array_id][left_id]->compare(
          unsafe_rows_[right_array_id][right_id], key_idx, asc, nulls_first);
      if (comparison != 2) {
        return comparison;
      }
      key_idx += 1;
    }
    return 2;
  }

  bool compare(int left_array_id, int64_t left_len, 
               int right_array_id, int64_t right_len) {
    if (compareInternal(left_array_id, left_len, right_array_id, right_len) == 1) {
      return true;
    }
    return false;
  }

 private:
  std::vector<std::vector<std::shared_ptr<AccessibleUnsafeRow>>> unsafe_rows_;
  std::vector<bool> sort_directions_;
  std::vector<bool> nulls_order_;
};
