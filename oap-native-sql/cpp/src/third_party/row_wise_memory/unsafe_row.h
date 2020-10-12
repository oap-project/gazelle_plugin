#pragma once

#include <arrow/util/decimal.h>
#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include "third_party/row_wise_memory/native_memory.h"

#define TEMP_UNSAFEROW_BUFFER_SIZE 1024

typedef struct {
  int numFields;
  int sizeInBytes;
  char* data;
  int cursor;
} UnsafeRow;

/* Unsafe Row Layout as below
 *
 * | validity | col 0 | col 1 | col 2 | ... | cursor |
 * explain:
 * validity: 64 * n fields = 8 * n bytes
 * col: each col is 8 bytes, string col should be offsetAndLength
 * cursor: used by string data
 *
 */

static inline int calculateBitSetWidthInBytes(int numFields) {
  return ((numFields + 63) / 64) * 8;
}

static inline int64_t getFieldOffset(UnsafeRow* row, int ordinal) {
  int bitSetWidthInBytes = calculateBitSetWidthInBytes(row->numFields);
  return bitSetWidthInBytes + ordinal * 8LL;
}

static inline void initTempUnsafeRow(UnsafeRow* row, int numFields) {
  if (row) {
    row->numFields = numFields;
    row->data = (char*)nativeMalloc(TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
    auto validity_size = calculateBitSetWidthInBytes(numFields);
    memset(row->data, 0, validity_size);
    row->cursor = getFieldOffset(row, numFields);
    row->sizeInBytes = row->cursor;
  }
}

static inline void releaseTempUnsafeRow(UnsafeRow* row) {
  if (row && row->data) {
    nativeFree(row->data);
  }
}

static inline int getSizeInBytes(UnsafeRow* row) { return row->sizeInBytes; }

static inline bool isEqualUnsafeRow(UnsafeRow* row0, UnsafeRow* row1) {
  if (row0->sizeInBytes != row1->sizeInBytes) return false;

  return (memcmp(row0->data, row1->data, row0->sizeInBytes) == 0);
}

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
  int64_t mask = 1LL << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * WORD_SIZE;
  int64_t word = *((int64_t*)(char*)(row->data + wordOffset));
  // set validity
  *((int64_t*)(row->data + wordOffset)) = word | mask;
  // set data
  *((int64_t*)(row->data + getFieldOffset(row, index))) = 0;
}

static inline void setNotNullAt(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  int64_t mask = 1LL << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * WORD_SIZE;
  int64_t word = *((int64_t*)(char*)(row->data + wordOffset));
  // set validity
  *((int64_t*)(row->data + wordOffset)) = word & ~mask;
}

static inline void setOffsetAndSize(UnsafeRow* row, int ordinal, int64_t size) {
  int64_t relativeOffset = row->cursor;
  int64_t fieldOffset = getFieldOffset(row, ordinal);
  int64_t offsetAndSize = (relativeOffset << 32) | size;
  // set data
  *((int64_t*)(char*)(row->data + fieldOffset)) = offsetAndSize;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, bool val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  *((int64_t*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int8_t val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  *((int64_t*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint8_t val) {
  appendToUnsafeRow(row, index, static_cast<int8_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int16_t val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  uint64_t longValue = (uint64_t)(uint16_t)val;
  *((int64_t*)(char*)(row->data + offset)) = longValue;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint16_t val) {
  appendToUnsafeRow(row, index, static_cast<int16_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int32_t val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  uint64_t longValue = (uint64_t)(unsigned int)val;
  *((int64_t*)(char*)(row->data + offset)) = longValue;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint32_t val) {
  appendToUnsafeRow(row, index, static_cast<int32_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int64_t val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  *((int64_t*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint64_t val) {
  appendToUnsafeRow(row, index, static_cast<int64_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, float val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  double longValue = (double)val;
  *((double*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, double val) {
  setNotNullAt(row, index);
  int64_t offset = getFieldOffset(row, index);
  *((double*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, std::string str) {
  int numBytes = str.size();
  int roundedSize = roundNumberOfBytesToNearestWord(numBytes);

  setNotNullAt(row, index);

  zeroOutPaddingBytes(row, numBytes);

  memcpy(row->data + row->cursor, str.c_str(), numBytes);

  setOffsetAndSize(row, index, numBytes);

  // move the cursor forward.
  row->cursor += roundedSize;
  row->sizeInBytes = row->cursor;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, arrow::Decimal128 dcm,
                                     int precision, int scale) {}
