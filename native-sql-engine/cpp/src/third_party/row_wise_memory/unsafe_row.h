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

#include <arrow/util/decimal.h>
#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include "third_party/row_wise_memory/native_memory.h"

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define TEMP_UNSAFEROW_BUFFER_SIZE 8192
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

/* Unsafe Row Layout (This unsafe row only used to append all fields data as
 * continuous memory, unable to be get data from)
 *
 * | validity | col 0 | col 1 | col 2 | ...
 * explain:
 * validity: n fields = (n/8 + 1) bytes
 * col: each col has variable size
 * cursor: used to pointer to cur unused pos
 *
 */
struct UnsafeRow {
  int numFields;
  char* data = nullptr;
  int cursor;
  int validity_size;
  int is_empty_size;
  UnsafeRow() {}
  UnsafeRow(int numFields) : numFields(numFields) {
    validity_size = (numFields / 8) + 1;
    is_empty_size = (numFields / 8) + 1;
    cursor = validity_size + is_empty_size;
    data = (char*)nativeMalloc(TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
    memset(data, 0, validity_size + is_empty_size);
  }
  ~UnsafeRow() {
    if (data) {
      nativeFree(data);
    }
  }
  int sizeInBytes() { return cursor; }
  void reset() {
    validity_size = (numFields / 8) + 1;
    is_empty_size = (numFields / 8) + 1;
    memset(data, 0, cursor);
    cursor = validity_size + is_empty_size;
  }
  bool isNullExists() {
    for (int i = 0; i < ((numFields / 8) + 1); i++) {
      if (data[i] != 0) return true;
    }
    return false;
  }
};

static inline int calculateBitSetWidthInBytes(int numFields) {
  return ((numFields / 8) + 1);
}

static inline int getSizeInBytes(UnsafeRow* row) { return row->cursor; }

static inline int roundNumberOfBytesToNearestWord(int numBytes) {
  int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }
}

static inline void zeroOutPaddingBytes(UnsafeRow* row, int numBytes) {
  if ((numBytes & 0x07) > 0) {
    *((int64_t*)(char*)(row->data + row->cursor + ((numBytes >> 3) << 3))) = 0L;
  }
}

static inline void setNullAt(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  auto bitSetIdx = index >> 3;  // mod 8
  *(row->data + bitSetIdx) |= kBitmask[index % 8];
}

static inline void setEmptyAt(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  auto bitSetIdx = index >> 3;  // mod 8
  *(row->data + row->validity_size + bitSetIdx) |= kBitmask[index % 8];
}

template <typename T>
using is_number_alike =
    std::integral_constant<bool, std::is_arithmetic<T>::value ||
                                     std::is_floating_point<T>::value>;

template <typename T, typename std::enable_if_t<is_number_alike<T>::value>* = nullptr>
static inline void appendToUnsafeRow(UnsafeRow* row, const int& index, const T& val) {
  if (unlikely(row->cursor + sizeof(T) > TEMP_UNSAFEROW_BUFFER_SIZE))
    row->data =
        (char*)nativeRealloc(row->data, 2 * TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
  *((T*)(row->data + row->cursor)) = val;
  row->cursor += sizeof(T);
}

static inline void appendToUnsafeRow(UnsafeRow* row, const int& index,
                                     arrow::util::string_view str) {
  if (unlikely(row->cursor + str.size() > TEMP_UNSAFEROW_BUFFER_SIZE))
    row->data =
        (char*)nativeRealloc(row->data, 2 * TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
  memcpy(row->data + row->cursor, str.data(), str.size());
  row->cursor += str.size();
}

static inline void appendToUnsafeRow(UnsafeRow* row, const int& index,
                                     const arrow::Decimal128& dcm) {
  if (unlikely(row->cursor + 16 > TEMP_UNSAFEROW_BUFFER_SIZE))
    row->data =
        (char*)nativeRealloc(row->data, 2 * TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
  int numBytes = 16;
  zeroOutPaddingBytes(row, numBytes);
  memcpy(row->data + row->cursor, dcm.ToBytes().data(), numBytes);
  // move the cursor forward.
  row->cursor += numBytes;
}
