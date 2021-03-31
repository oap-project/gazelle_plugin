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

#define TEMP_UNSAFEROW_BUFFER_SIZE 1024
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
  UnsafeRow() {}
  UnsafeRow(int numFields) : numFields(numFields) {
    auto validity_size = (numFields / 8) + 1;
    cursor = validity_size;
    data = (char*)nativeMalloc(TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
    memset(data, 0, validity_size);
  }
  ~UnsafeRow() {
    if (data) {
      nativeFree(data);
    }
  }
  int sizeInBytes() { return cursor; }
  void reset() {
    memset(data, 0, cursor);
    auto validity_size = (numFields / 8) + 1;
    cursor = validity_size;
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

template <typename T, typename std::enable_if_t<std::is_arithmetic<T>::value &&
                                     !std::is_floating_point<T>::value>* = nullptr>
static inline void appendToUnsafeRow(UnsafeRow* row, const int& index, const T& val) {
  *((T*)(row->data + row->cursor)) = val;
  row->cursor += sizeof(T);
}

template <typename T, typename std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
static inline void appendToUnsafeRow(UnsafeRow* row, const int& index, const T& val) {
  if (val < 0 && std::abs(val) < 0.0000001) {
    // regard -0.0 as the same as 0.0
    *((T*)(row->data + row->cursor)) = 0.0;
  } else {
    *((T*)(row->data + row->cursor)) = val;
  }
  row->cursor += sizeof(T);
}

static inline void appendToUnsafeRow(UnsafeRow* row, const int& index,
                                     const std::string& str) {
  int numBytes = str.size();
  // int roundedSize = roundNumberOfBytesToNearestWord(numBytes);

  // zeroOutPaddingBytes(row, numBytes);
  memcpy(row->data + row->cursor, str.c_str(), numBytes);

  // move the cursor forward.
  row->cursor += numBytes;
}

static inline void appendToUnsafeRow(UnsafeRow* row, const int& index,
                                     const arrow::Decimal128& dcm) {
  int numBytes = 16;
  zeroOutPaddingBytes(row, numBytes);
  memcpy(row->data + row->cursor, dcm.ToBytes().data(), numBytes);
  // move the cursor forward.
  row->cursor += numBytes;
}
