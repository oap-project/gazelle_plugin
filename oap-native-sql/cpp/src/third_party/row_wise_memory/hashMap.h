#pragma once

#include <stdlib.h>
#include <string.h>

#include "third_party/row_wise_memory/unsafe_row.h"

#define MAX_HASH_MAP_CAPACITY (1 << 29)  // must be power of 2

#define HASH_NEW_KEY -1

#define loadFactor 0.5

typedef struct {
  int arrayCapacity;  // The size of the keyArray
  size_t mapSize;     // The size of the bytesMap
  int cursor;
  int numKeys;
  int numValues;
  bool needSpill;
  int* keyArray;   //<32-bit key hash,32-bit offset>, hash slot itself.
  char* bytesMap;  // use to save the  key-row, and value row.
} unsafeHashMap;   /*general purpose hash structure*/

static inline int getTotalLength(char* base) { return *((int*)base); }

static inline int getKeyLength(char* base) { return *((int*)(base + 4)); }

static inline unsafeHashMap* createUnsafeHashMap(int initArrayCapacity,
                                                 int initialHashCapacity) {
  unsafeHashMap* hashMap =
      (unsafeHashMap*)nativeMalloc(sizeof(unsafeHashMap), MEMTYPE_HASHMAP);

  hashMap->keyArray =
      (int*)nativeMalloc(initArrayCapacity * sizeof(int) * 2, MEMTYPE_HASHMAP);
  hashMap->arrayCapacity = initArrayCapacity;
  memset(hashMap->keyArray, -1, initArrayCapacity * sizeof(int) * 2);

  hashMap->bytesMap = (char*)nativeMalloc(initialHashCapacity, MEMTYPE_HASHMAP);
  hashMap->mapSize = initialHashCapacity;

  hashMap->cursor = 0;
  hashMap->numKeys = 0;
  hashMap->numValues = 0;
  hashMap->needSpill = false;
  return hashMap;
}

static inline void destroyHashMap(unsafeHashMap* hm) {
  if (hm != NULL) {
    if (hm->keyArray != NULL) nativeFree(hm->keyArray);
    if (hm->bytesMap != NULL) nativeFree(hm->bytesMap);

    nativeFree(hm);
  }
}

static inline void clearHashMap(unsafeHashMap* hm) {
  memset(hm->keyArray, -1, hm->arrayCapacity * sizeof(int) * 2);

  hm->cursor = 0;
  hm->numKeys = 0;
  hm->numValues = 0;
  hm->needSpill = false;
}

static inline int getRecordLengthFromBytesMap(char* record) {
  int totalLengh = *((int*)record);
  return (4 + totalLengh + 4);
}

static inline int getkLenFromBytesMap(char* record) {
  int klen = *((int*)(record + 4));
  return klen;
}

static inline int getvLenFromBytesMap(char* record) {
  int totalLengh = *((int*)record);
  int klen = getkLenFromBytesMap(record);
  return (totalLengh - 4 - klen);
}

static inline char* getKeyFromBytesMap(char* record) { return (record + 8); }

static inline char* getValueFromBytesMap(char* record) {
  int klen = getkLenFromBytesMap(record);
  return (record + klen + 8);
}

static inline bool growHashBytesMap(unsafeHashMap* hashMap) {
  int oldSize = hashMap->mapSize;
  int newSize = oldSize << 1;
  char* newBytesMap = (char*)nativeRealloc(hashMap->bytesMap, newSize, MEMTYPE_HASHMAP);
  if (newBytesMap == NULL) return false;

  hashMap->bytesMap = newBytesMap;
  hashMap->mapSize = newSize;
  return true;
}

static inline bool growAndRehashKeyArray(unsafeHashMap* hashMap) {
  assert(hashMap->keyArray != NULL);

  int i;
  int oldCapacity = hashMap->arrayCapacity;
  int newCapacity = (oldCapacity << 1);
  newCapacity =
      (newCapacity >= MAX_HASH_MAP_CAPACITY) ? MAX_HASH_MAP_CAPACITY : newCapacity;
  int* oldKeyArray = hashMap->keyArray;

  // Allocate the new keyArray and zero it
  int* newKeyArray = (int*)nativeMalloc(newCapacity * sizeof(int) * 2, MEMTYPE_HASHMAP);
  if (newKeyArray == NULL) return false;

  memset(newKeyArray, -1, newCapacity * sizeof(int) * 2);
  int mask = newCapacity - 1;

  // Rehash the map
  for (i = 0; i < (oldCapacity << 1); i += 2) {
    int keyOffset = oldKeyArray[i];
    if (keyOffset < 0) continue;

    int hashcode = oldKeyArray[i + 1];
    int newPos = hashcode & mask;
    int step = 1;
    while (newKeyArray[newPos * 2] >= 0) {
      newPos = (newPos + step) & mask;
      step++;
    }
    newKeyArray[newPos * 2] = keyOffset;
    newKeyArray[newPos * 2 + 1] = hashcode;
  }

  hashMap->keyArray = newKeyArray;
  hashMap->arrayCapacity = newCapacity;

  nativeFree(oldKeyArray);
  return true;
}

/*
 * return:
 *   HASH_NEW_KEY:  keyArrayPos index to the empty slot.
 *   HASH_FOUND_MATCH: keyArrayPos index to the empty slot. valueRow: point to the mathced
 * value. HASH_FULL:  need grow.
 */
static inline int safeLookup(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes;
  char* base = hashMap->bytesMap;

  while (true) {
    int KeyAddressOffset = hashMap->keyArray[pos * 2];
    int keyHashCode = hashMap->keyArray[pos * 2 + 1];

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
// Full hash code matches.  Let's compare the keys for equality.
#ifdef DEBUG
        printf("keyLength is %d, Find Key is ", keyLength);
        for (int j = 0; j < keyLength; j++) printf("%02X ", keyRow->data[j]);
        printf(", Inmap key is ");
        auto inmap_data = getKeyFromBytesMap(base + KeyAddressOffset);
        for (int j = 0; j < keyLength; j++) printf("%02X ", inmap_data[j]);
        printf("\n");
#endif
        if ((getKeyLength(base + KeyAddressOffset) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(base + KeyAddressOffset),
                    keyLength) == 0)) {
          int value;
          memcpy(&value, getValueFromBytesMap(base + KeyAddressOffset), sizeof(int32_t));
          return value;
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
 * maintain a list for each item if have same hash key:
 */
static inline int getOrInsert(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal,
                              int value) {
  assert(hashMap->keyArray != NULL);
  int keyArrayPos;
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes;
  char* base = hashMap->bytesMap;

  while (true) {
    int KeyAddressOffset = hashMap->keyArray[pos * 2];
    int keyHashCode = hashMap->keyArray[pos * 2 + 1];

    if (KeyAddressOffset < 0) {
      // This is a new key.
      keyArrayPos = pos;
      break;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        if ((getKeyLength(base + KeyAddressOffset) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(base + KeyAddressOffset),
                    keyLength) == 0)) {
          int value;
          memcpy(&value, getValueFromBytesMap(base + KeyAddressOffset), sizeof(int32_t));
          return value;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  assert((keyRow->sizeInBytes % 8) == 0);
  assert(hashMap->keyArray != NULL);

  int klen = keyRow->sizeInBytes;
  int vlen = 4;
  int cursor = hashMap->cursor;

  // Here, we'll copy the data into hashMap. The format is like following:
  // (8 byte key length) (key) (value) (8 byte pointer to next value)
  int recordLength = 8 + klen + vlen + 4;
  if (cursor + recordLength >= hashMap->mapSize) {
    if (hashMap->needSpill) return HASH_NEW_KEY;

    // Grow the hash table
    if (!growHashBytesMap(hashMap)) {
      hashMap->needSpill = true;
      return HASH_NEW_KEY;
    }
  }

  // Save the raw data into bytesMap, and follow the layout as:
  // (8 byte key length) (key) (4 byte pointer to next value offset)
  int keyAddressOffset = cursor;
  *((int*)(hashMap->bytesMap + cursor)) = klen + vlen + 4;
  *((int*)(hashMap->bytesMap + cursor + 4)) = klen;
  cursor += 8;
  memcpy(hashMap->bytesMap + cursor, keyRow->data, klen);
  cursor += klen;
  *((int*)(hashMap->bytesMap + cursor)) = value;
  cursor += vlen;
  *((int*)(hashMap->bytesMap + cursor)) = hashMap->keyArray[keyArrayPos * 2];

  // Update keyArray in hashMap
  if (hashMap->keyArray[keyArrayPos * 2] < 0) hashMap->numKeys++;

  hashMap->keyArray[keyArrayPos * 2] = keyAddressOffset;
  hashMap->keyArray[keyArrayPos * 2 + 1] = hashVal;
  hashMap->cursor += recordLength;
  hashMap->numValues++;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return value;
}
