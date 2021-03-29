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

#include <arrow/memory_pool.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "third_party/row_wise_memory/unsafe_row.h"

#define MAX_HASH_MAP_CAPACITY (1 << 29)  // must be power of 2

#define HASH_NEW_KEY -1
#define HASH_FOUND_MATCH -2
#define HASH_FULL -3

#define loadFactor 0.5

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndex;

/** HashMap Layout
 *
 * keyArray: Fixed Size Array to map hash key to payloads
 * each item has 8 bytes, 4 for a hash32 key and 4 for location in bytesMap
 * | key-hash(4 bytes) | bytesMap offset(4 bytes) |
 *
 * BytesMap: map to store key and value data
 * each item has format as below, same key items will be linked (Min size is 8
 *bytes when key and value both 0) | total-length(2 bytes) | key-length(2 bytes)
 *| key data(variable-size) | value data(variable-size) | next value ptr(4
 *bytes) |
 *
 **/

template <typename T>
using is_number_alike =
    std::integral_constant<bool, std::is_arithmetic<T>::value ||
                                     std::is_floating_point<T>::value>;

template <typename T>
using is_number_or_decimal_type =
    std::integral_constant<bool, is_number_alike<T>::value ||
                                     std::is_same<T, arrow::Decimal128>::value>;

typedef struct {
  int arrayCapacity;  // The size of the keyArray
  uint8_t bytesInKeyArray;
  size_t mapSize;  // The size of the bytesMap
  int cursor;
  int numKeys;
  bool needSpill;
  char* keyArray;  //<32-bit key hash,32-bit offset>, hash slot itself.
  char* bytesMap;  // use to save the  key-row, and value row.
  void* pool;
} unsafeHashMap; /*general purpose hash structure*/

static inline void dump(unsafeHashMap* hm) {
  printf("=================== HashMap DUMP =======================\n");
  printf("keyarray capacity is %d\n", hm->arrayCapacity);
  printf("bytemap capacity is %lu\n", hm->mapSize);
  printf("bytesInKey is %d\n", hm->bytesInKeyArray);
  printf("cursor is %d\n", hm->cursor);
  printf("numKeys is %d\n", hm->numKeys);
  printf("keyArray[offset_in_bytesMap, hashVal] is\n");
  for (int i = 0; i < hm->arrayCapacity * hm->bytesInKeyArray;
       i = i + hm->bytesInKeyArray) {
    char* pos = hm->keyArray + i;
    if (*((int*)pos) == -1) continue;
    printf("%d: ", i / hm->bytesInKeyArray);
    auto numFields = (hm->bytesInKeyArray % 4) > 0 ? (hm->bytesInKeyArray / 4 + 1)
                                                   : (hm->bytesInKeyArray / 4);
    for (int j = 0; j < numFields; j++) {
      if ((hm->bytesInKeyArray - j * 4) < 4) {
        int tmp = 0;
        memcpy(&tmp, (pos + j * 4), (hm->bytesInKeyArray - j * 4));
        printf("%04x  ", tmp);  // value_data
      } else {
        printf("%04x    ", *((int*)(pos + j * 4)));
      }
    }
    printf("\n");
  }
  printf("bytesMap is\n");
  int pos = 0;
  int idx = 0;
  while (pos < hm->cursor) {
    printf("%d: ", idx++);
    auto first_4 = *(int*)(hm->bytesMap + pos);
    auto total_length = first_4 >> 16;
    auto key_length = first_4 & 0x00ff;
    auto value_length =
        total_length - key_length - 8;  // 12 includes first 4 bytes, last 4 bytes
    printf("[%04x, %d, %d, %d]", pos, total_length, key_length, value_length);
    printf("%04x  ", first_4);  // total_length + key_length
    int i = 0;
    while (i < key_length) {
      if ((key_length - i) < 4) {
        int tmp = 0;
        memcpy(&tmp, (hm->bytesMap + pos + 4 + i), (key_length - i));
        printf("%04x  ", tmp);  // value_data
        i = key_length;
      } else {
        printf("%04x  ", *(int*)(hm->bytesMap + pos + 4 + i));  // key_data
        i += 4;
      }
    }
    i = 0;
    while (i < value_length) {
      if ((value_length - i) < 4) {
        int tmp = 0;
        memcpy(&tmp, (hm->bytesMap + pos + 4 + key_length + i), (value_length - i));
        printf("%04x  ", tmp);  // value_data
        i = value_length;
      } else {
        printf("%04x  ",
               *(int*)(hm->bytesMap + pos + 4 + key_length + i));  // value_data
        i += 4;
      }
    }
    printf("%04x  ",
           *(int*)(hm->bytesMap + pos + 4 + key_length + value_length));  // next_ptr
    printf("\n");
    pos += total_length;
  }
}

static inline int getTotalLength(char* base) { return *((int*)base) >> 16; }

static inline int getKeyLength(char* base) { return *((int*)(base)) & 0x00ff; }

/* If keySize > 0, we should put raw key also in keyArray */
/* Other wise we put key in bytesMap */
static inline unsafeHashMap* createUnsafeHashMap(arrow::MemoryPool* pool,
                                                 int initArrayCapacity,
                                                 int initialHashCapacity,
                                                 int keySize = -1) {
  unsafeHashMap* hashMap;
  pool->Allocate(sizeof(unsafeHashMap), (uint8_t**)&hashMap);
  uint8_t bytesInKeyArray = (keySize == -1) ? 8 : 8 + keySize;
  hashMap->bytesInKeyArray = bytesInKeyArray;
  pool->Allocate(initArrayCapacity * bytesInKeyArray, (uint8_t**)&hashMap->keyArray);
  hashMap->arrayCapacity = initArrayCapacity;
  memset(hashMap->keyArray, -1, initArrayCapacity * bytesInKeyArray);

  // hashMap->bytesMap = (char*)nativeMalloc(initialHashCapacity,
  // MEMTYPE_HASHMAP);
  pool->Allocate(initialHashCapacity, (uint8_t**)&hashMap->bytesMap);
  hashMap->mapSize = initialHashCapacity;

  hashMap->cursor = 0;
  hashMap->numKeys = 0;
  hashMap->needSpill = false;
  hashMap->pool = (void*)pool;
  return hashMap;
}

static inline void destroyHashMap(unsafeHashMap* hm) {
  if (hm != NULL) {
    // if (hm->keyArray != NULL) nativeFree(hm->keyArray);
    // if (hm->bytesMap != NULL) nativeFree(hm->bytesMap);
    // nativeFree(hm);
    auto pool = (arrow::MemoryPool*)hm->pool;
    if (hm->keyArray != NULL)
      pool->Free((uint8_t*)hm->keyArray, hm->arrayCapacity * hm->bytesInKeyArray);
    if (hm->bytesMap != NULL) pool->Free((uint8_t*)hm->bytesMap, hm->mapSize);
    pool->Free((uint8_t*)hm, sizeof(unsafeHashMap));
  }
}

static inline int getRecordLengthFromBytesMap(char* record) {
  return *((int*)record) >> 16;
}

static inline int getkLenFromBytesMap(char* record) {
  int klen = *((int*)(record)) & 0x00ff;
  return klen;
}

static inline int getvLenFromBytesMap(char* record) {
  int totalLengh = *((int*)record) >> 16;
  int klen = *((int*)(record)) & 0x00ff;
  return (totalLengh - 8 - klen);
}

static inline char* getKeyFromBytesMap(char* record) { return (record + 4); }

static inline char* getValueFromBytesMap(char* record) {
  int klen = *((int*)(record)) & 0x00ff;
  return (record + 4 + klen);
}

static inline int getNextOffsetFromBytesMap(char* record) {
  int totalLengh = *((int*)record) >> 16;
  return *((int*)(record + totalLengh - 4));
}

static inline int getvLenFromBytesMap(unsafeHashMap* hashMap, int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  return getvLenFromBytesMap(record);
}

static inline int getNextOffsetFromBytesMap(unsafeHashMap* hashMap,
                                            int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  return getNextOffsetFromBytesMap(record);
}

static inline int getValueFromBytesMapByOffset(unsafeHashMap* hashMap,
                                               int KeyAddressOffset, char* output) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  memcpy(output, getValueFromBytesMap(record), getvLenFromBytesMap(record));
  return KeyAddressOffset;
}

static inline bool shrinkToFit(unsafeHashMap* hashMap) {
  if (hashMap->cursor >= hashMap->mapSize) {
    return true;
  }
  auto pool = (arrow::MemoryPool*)hashMap->pool;
  auto status =
      pool->Reallocate(hashMap->mapSize, hashMap->cursor, (uint8_t**)&hashMap->bytesMap);
  if (status.ok()) {
    hashMap->mapSize = hashMap->cursor;
  }
  return status.ok();
}

static inline bool growHashBytesMap(unsafeHashMap* hashMap) {
  std::cout << "growHashBytesMap" << std::endl;
  int oldSize = hashMap->mapSize;
  int newSize = oldSize << 1;
  auto pool = (arrow::MemoryPool*)hashMap->pool;
  pool->Reallocate(hashMap->mapSize, newSize, (uint8_t**)&hashMap->bytesMap);
  if (hashMap->bytesMap == NULL) return false;

  hashMap->mapSize = newSize;
  return true;
}

static inline bool growAndRehashKeyArray(unsafeHashMap* hashMap) {
  assert(hashMap->keyArray != NULL);
  std::cout << "growAndRehashKeyArray" << std::endl;

  int oldCapacity = hashMap->arrayCapacity;
  int newCapacity = (oldCapacity << 1);
  newCapacity =
      (newCapacity >= MAX_HASH_MAP_CAPACITY) ? MAX_HASH_MAP_CAPACITY : newCapacity;

  // Allocate the new keyArray and zero it
  char* origKeyArray = hashMap->keyArray;
  auto pool = (arrow::MemoryPool*)hashMap->pool;
  pool->Allocate(newCapacity * hashMap->bytesInKeyArray, (uint8_t**)&hashMap->keyArray);
  if (hashMap->keyArray == NULL) return false;

  memset(hashMap->keyArray, -1, newCapacity * hashMap->bytesInKeyArray);
  int mask = newCapacity - 1;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  if (keySizeInBytes > 8) {
    // Rehash the map
    for (int pos = 0; pos < oldCapacity; pos++) {
      int keyOffset = *(int*)(origKeyArray + pos * keySizeInBytes);
      int hashcode = *(int*)(origKeyArray + pos * keySizeInBytes + 4);

      if (keyOffset == -1) continue;

      int newPos = hashcode & mask;
      int step = 1;
      while (*(int*)(hashMap->keyArray + newPos * keySizeInBytes) != -1) {
        newPos = (newPos + step) & mask;
        step++;
      }
      memcpy(hashMap->keyArray + newPos * keySizeInBytes,
             origKeyArray + pos * keySizeInBytes, keySizeInBytes);
    }
  } else {
    // Rehash the map
    for (int pos = 0; pos < oldCapacity; pos++) {
      int keyOffset = *(int*)(origKeyArray + pos * keySizeInBytes);
      int hashcode = *(int*)(origKeyArray + pos * keySizeInBytes + 4);

      if (keyOffset == -1) continue;

      int newPos = hashcode & mask;
      int step = 1;
      while (*(int*)(hashMap->keyArray + newPos * keySizeInBytes) != -1) {
        newPos = (newPos + step) & mask;
        step++;
      }
      *(int*)(hashMap->keyArray + newPos * keySizeInBytes) = keyOffset;
      *(int*)(hashMap->keyArray + newPos * keySizeInBytes + 4) = hashcode;
    }
  }

  hashMap->arrayCapacity = newCapacity;
  pool->Free((uint8_t*)origKeyArray, oldCapacity * keySizeInBytes);
  return true;
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
template <typename CType,
          typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        if (keySizeInBytes > 8) {
          if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
            return 0;
          }
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          char* record = base + KeyAddressOffset;
          if (keyRow == *((CType*)getKeyFromBytesMap(record))) {
            return 0;
          }
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

template <typename CType, typename std::enable_if_t<
                              std::is_same<CType, arrow::Decimal128>::value>* = nullptr>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = 16;
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        if (keySizeInBytes > 8) {
          if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
            return 0;
          }
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          char* record = base + KeyAddressOffset;
          if (keyRow == *((CType*)getKeyFromBytesMap(record))) {
            return 0;
          }
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, const char* keyRow, size_t keyRowLen,
                             int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRowLen;
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            memcmp(keyRow, getKeyFromBytesMap(record), keyLength) == 0) {
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
template <typename CType,
          typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal,
                             std::vector<ArrayItemIndex>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        assert(keySizeInBytes > 8);
        if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
          (*output).clear();
          if (!((KeyAddressOffset >> 31) == 0)) {
            char* record = base + (KeyAddressOffset & 0x7FFFFFFF);
            while (record != nullptr) {
              (*output).push_back(*((ArrayItemIndex*)getValueFromBytesMap(record)));
              KeyAddressOffset = getNextOffsetFromBytesMap(record);
              record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
            }
          } else {
            (*output).push_back(*((ArrayItemIndex*)&KeyAddressOffset));
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

template <typename CType, typename std::enable_if_t<
                              std::is_same<CType, arrow::Decimal128>::value>* = nullptr>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal,
                             std::vector<ArrayItemIndex>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = 16;
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        assert(keySizeInBytes > 8);
        if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
          (*output).clear();
          if (!((KeyAddressOffset >> 31) == 0)) {
            char* record = base + (KeyAddressOffset & 0x7FFFFFFF);
            while (record != nullptr) {
              (*output).push_back(*((ArrayItemIndex*)getValueFromBytesMap(record)));
              KeyAddressOffset = getNextOffsetFromBytesMap(record);
              record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
            }
          } else {
            (*output).push_back(*((ArrayItemIndex*)&KeyAddressOffset));
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, const char* keyRow, size_t keyRowLen,
                             int hashVal, std::vector<ArrayItemIndex>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRowLen;
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            memcmp(keyRow, getKeyFromBytesMap(record), keyLength) == 0) {
          // there may be more than one record
          (*output).clear();
          while (record != nullptr) {
            (*output).push_back(*((ArrayItemIndex*)getValueFromBytesMap(record)));
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal, std::vector<ArrayItemIndex>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          // there may be more than one record
          (*output).clear();
          while (record != nullptr) {
            (*output).push_back(*((ArrayItemIndex*)getValueFromBytesMap(record)));
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool append(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal,
                          char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;
  int klen = keyRow->sizeInBytes();
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = 8;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      hashMap->cursor += recordLength;
      break;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          if (cursor + recordLength >= hashMap->mapSize) {
            // Grow the hash table
            assert(growHashBytesMap(hashMap));
            base = hashMap->bytesMap;
            record = base + cursor;
          }

          // link current record next ptr to new record
          int cur_record_lengh = *((int*)record) >> 16;
          auto nextOffset = (int*)(record + cur_record_lengh - 4);
          while (*nextOffset != 0) {
            record = base + *nextOffset;
            cur_record_lengh = *((int*)record) >> 16;
            nextOffset = (int*)(record + cur_record_lengh - 4);
          }
          *nextOffset = cursor;
          record = base + cursor;
          klen = 0;

          // Update hashMap
          hashMap->cursor += (4 + klen + vlen + 4);
          break;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  assert((klen & 0xff00) == 0);
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  memcpy(record + 4, keyRow->data, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
template <typename CType,
          typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
static inline bool append(unsafeHashMap* hashMap, CType keyRow, int hashVal, char* value,
                          size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;
  int klen = 0;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  // chendi: Add a optimization here, use offset first bit to indicate if this
  // offset is ArrayItemIndex or bytesmap offset if first key, it will be
  // arrayItemIndex first bit is 0 if multiple same key, it will be offset first
  // bit is 1

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      int keyArrayPos = pos;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = *(int*)value;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      *(CType*)(keyArrayBase + pos * keySizeInBytes + 8) = keyRow;
      return true;
    } else {
      char* previous_value = nullptr;
      if (((int)keyHashCode == hashVal) &&
          (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8))) {
        if ((KeyAddressOffset >> 31) == 0) {
          // we should move in keymap value to bytesmap
          record = base + cursor;
          // copy keyRow and valueRow into hashmap
          auto total_key_length = ((8 + klen + vlen) << 16) | klen;
          *((int*)record) = total_key_length;
          *((int*)(record + 4 + klen)) = KeyAddressOffset;
          *((int*)(record + 4 + klen + vlen)) = 0;

          // Update hashMap
          KeyAddressOffset = hashMap->cursor;
          *(int*)(keyArrayBase + pos * keySizeInBytes) = (KeyAddressOffset | 0x80000000);
          record = base + KeyAddressOffset;
          hashMap->cursor += (4 + klen + vlen + 4);
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          record = base + (KeyAddressOffset & 0x7FFFFFFF);
        }
        if (hashMap->cursor + recordLength >= hashMap->mapSize) {
          // Grow the hash table
          assert(growHashBytesMap(hashMap));
          base = hashMap->bytesMap;
          record = base + hashMap->cursor;
        }

        // link current record next ptr to new record
        int cur_record_lengh = *((int*)record) >> 16;
        auto nextOffset = (int*)(record + cur_record_lengh - 4);
        while (*nextOffset != 0) {
          record = base + *nextOffset;
          cur_record_lengh = *((int*)record) >> 16;
          nextOffset = (int*)(record + cur_record_lengh - 4);
        }
        *nextOffset = hashMap->cursor;
        record = base + hashMap->cursor;

        // Update hashMap
        hashMap->cursor += (4 + klen + vlen + 4);
        break;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  // memcpy(record + 4, &keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
template <typename CType, typename std::enable_if_t<
                              std::is_same<CType, arrow::Decimal128>::value>* = nullptr>
static inline bool append(unsafeHashMap* hashMap, CType keyRow, int hashVal, char* value,
                          size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = 16; /*sizeof Deimal128*/
  char* base = hashMap->bytesMap;
  int klen = 0;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  // chendi: Add a optimization here, use offset first bit to indicate if this
  // offset is ArrayItemIndex or bytesmap offset if first key, it will be
  // arrayItemIndex first bit is 0 if multiple same key, it will be offset first
  // bit is 1

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      int keyArrayPos = pos;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = *(int*)value;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      *(CType*)(keyArrayBase + pos * keySizeInBytes + 8) = keyRow;
      return true;
    } else {
      char* previous_value = nullptr;
      if (((int)keyHashCode == hashVal) &&
          (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8))) {
        if ((KeyAddressOffset >> 31) == 0) {
          // we should move in keymap value to bytesmap
          record = base + cursor;
          // copy keyRow and valueRow into hashmap
          auto total_key_length = ((8 + klen + vlen) << 16) | klen;
          *((int*)record) = total_key_length;
          *((int*)(record + 4 + klen)) = KeyAddressOffset;
          *((int*)(record + 4 + klen + vlen)) = 0;

          // Update hashMap
          KeyAddressOffset = hashMap->cursor;
          *(int*)(keyArrayBase + pos * keySizeInBytes) = (KeyAddressOffset | 0x80000000);
          record = base + KeyAddressOffset;
          hashMap->cursor += (4 + klen + vlen + 4);
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          record = base + (KeyAddressOffset & 0x7FFFFFFF);
        }
        if (hashMap->cursor + recordLength >= hashMap->mapSize) {
          // Grow the hash table
          assert(growHashBytesMap(hashMap));
          base = hashMap->bytesMap;
          record = base + hashMap->cursor;
        }

        // link current record next ptr to new record
        int cur_record_lengh = *((int*)record) >> 16;
        auto nextOffset = (int*)(record + cur_record_lengh - 4);
        while (*nextOffset != 0) {
          record = base + *nextOffset;
          cur_record_lengh = *((int*)record) >> 16;
          nextOffset = (int*)(record + cur_record_lengh - 4);
        }
        *nextOffset = hashMap->cursor;
        record = base + hashMap->cursor;

        // Update hashMap
        hashMap->cursor += (4 + klen + vlen + 4);
        break;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  // memcpy(record + 4, &keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool append(unsafeHashMap* hashMap, const char* keyRow, size_t keyLength,
                          int hashVal, char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  char* base = hashMap->bytesMap;
  int klen = keyLength;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      hashMap->cursor += recordLength;
      break;
    } else {
      record = base + KeyAddressOffset;
      if (((int)keyHashCode == hashVal) &&
          (memcmp(keyRow, getKeyFromBytesMap(record), keyLength) == 0)) {
        // Full hash code matches.  Let's compare the keys for equality.
        if (cursor + recordLength >= hashMap->mapSize) {
          // Grow the hash table
          assert(growHashBytesMap(hashMap));
          base = hashMap->bytesMap;
          record = base + cursor;
        }

        // link current record next ptr to new record
        int cur_record_lengh = *((int*)record) >> 16;
        auto nextOffset = (int*)(record + cur_record_lengh - 4);
        while (*nextOffset != 0) {
          record = base + *nextOffset;
          cur_record_lengh = *((int*)record) >> 16;
          nextOffset = (int*)(record + cur_record_lengh - 4);
        }
        *nextOffset = cursor;
        record = base + cursor;
        klen = 0;

        // Update hashMap
        hashMap->cursor += (4 + klen + vlen + 4);
        break;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  assert((klen & 0xff00) == 0);
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  memcpy(record + 4, keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool appendNewKey(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal,
                                char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;
  int klen = keyRow->sizeInBytes();
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = 8;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      hashMap->cursor += recordLength;
      break;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          return true;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  assert((klen & 0xff00) == 0);
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  memcpy(record + 4, keyRow->data, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
template <typename CType,
          typename std::enable_if_t<is_number_alike<CType>::value>* = nullptr>
static inline bool appendNewKey(unsafeHashMap* hashMap, CType keyRow, int hashVal,
                                char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;
  int klen = 0;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  // chendi: Add a optimization here, use offset first bit to indicate if this
  // offset is ArrayItemIndex or bytesmap offset if first key, it will be
  // arrayItemIndex first bit is 0 if multiple same key, it will be offset first
  // bit is 1

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      int keyArrayPos = pos;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = *(int*)value;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      *(CType*)(keyArrayBase + pos * keySizeInBytes + 8) = keyRow;
      return true;
    } else {
      char* previous_value = nullptr;
      if (((int)keyHashCode == hashVal) &&
          (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8))) {
        return true;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  // memcpy(record + 4, &keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
template <typename CType, typename std::enable_if_t<
                              std::is_same<CType, arrow::Decimal128>::value>* = nullptr>
static inline bool appendNewKey(unsafeHashMap* hashMap, CType keyRow, int hashVal,
                                char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = 16; /*sizeof Deimal128*/
  char* base = hashMap->bytesMap;
  int klen = 0;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  // chendi: Add a optimization here, use offset first bit to indicate if this
  // offset is ArrayItemIndex or bytesmap offset if first key, it will be
  // arrayItemIndex first bit is 0 if multiple same key, it will be offset first
  // bit is 1

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset == -1) {
      // This is a new key.
      int keyArrayPos = pos;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = *(int*)value;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      *(CType*)(keyArrayBase + pos * keySizeInBytes + 8) = keyRow;
      return true;
    } else {
      char* previous_value = nullptr;
      if (((int)keyHashCode == hashVal) &&
          (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8))) {
        return true;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  // memcpy(record + 4, &keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool appendNewKey(unsafeHashMap* hashMap, const char* keyRow,
                                size_t keyLength, int hashVal, char* value,
                                size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  char* base = hashMap->bytesMap;
  int klen = keyLength;
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      hashMap->cursor += recordLength;
      break;
    } else {
      record = base + KeyAddressOffset;
      if (((int)keyHashCode == hashVal) &&
          (memcmp(keyRow, getKeyFromBytesMap(record), keyLength) == 0)) {
        return true;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  assert((klen & 0xff00) == 0);
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  memcpy(record + 4, keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}