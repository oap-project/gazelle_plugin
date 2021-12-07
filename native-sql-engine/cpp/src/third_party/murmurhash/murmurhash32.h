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
#include <arrow/type_fwd.h>
#include <arrow/util/decimal.h>
#include <string.h>

#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace sparkcolumnarplugin {
namespace thirdparty {
namespace murmurhash32 {

template <typename T>
using is_int64 = std::is_same<int64_t, T>;

template <typename T>
using enable_if_int64 = typename std::enable_if<is_int64<T>::value, int32_t>::type;

template <typename T>
using is_string = std::is_same<std::string, T>;

template <typename T>
using is_stringview = std::is_same<arrow::util::string_view, T>;

template <typename T>
using is_string_or_stringview =
    std::integral_constant<bool, is_string<T>::value || is_stringview<T>::value>;

template <typename T>
using enable_if_string = typename std::enable_if<is_string<T>::value, int32_t>::type;

template <typename T>
using enable_if_string_or_stringview = typename std::enable_if<is_string_or_stringview<T>::value, int32_t>::type;

template <typename T>
using enable_if_decimal =
    typename std::enable_if<std::is_same<T, arrow::Decimal128>::value, int32_t>::type;
template <typename T>
using is_string_or_decimal =
    std::integral_constant<bool, is_string<T>::value || is_stringview<T>::value ||
                                     std::is_same<T, arrow::Decimal128>::value>;

template <typename T>
using enable_if_not_string_or_decimal =
    typename std::enable_if<!is_string_or_decimal<T>::value, int32_t>::type;

template <typename T>
using is_string_or_int64 =
    std::integral_constant<bool, is_string<T>::value || is_int64<T>::value>;

template <typename T>
using enable_if_not_int64 =
    typename std::enable_if<!is_string_or_int64<T>::value, int32_t>::type;

inline int64_t rotate_left(int64_t val, int distance) {
  return (val << distance) | (val >> (64 - distance));
}

inline int64_t fmix64(int64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccduLL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53uLL;
  k ^= k >> 33;
  return k;
}

template <typename T>
inline enable_if_int64<T> hash32(T val, int32_t seed) {
  int64_t c1 = 0xcc9e2d51ull;
  int64_t c2 = 0x1b873593ull;
  int length = 8;
  int64_t UINT_MASK = 0xffffffffull;
  int64_t lh1 = seed & UINT_MASK;
  for (int i = 0; i < 2; i++) {
    int64_t lk1 = ((val >> i * 32) & UINT_MASK);
    lk1 *= c1;
    lk1 &= UINT_MASK;

    lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

    lk1 *= c2;
    lk1 &= UINT_MASK;

    lh1 ^= lk1;
    lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >> 19);

    lh1 = lh1 * 5 + 0xe6546b64L;
    lh1 = UINT_MASK & lh1;
  }
  lh1 ^= length;

  lh1 ^= lh1 >> 16;
  lh1 *= 0x85ebca6bull;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 13;
  lh1 *= 0xc2b2ae35ull;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 16;

  return static_cast<int32_t>(lh1);
}

template <typename T>
inline enable_if_string_or_stringview<T> hash32(T val, bool validity, int32_t seed) {
  if (!validity) return seed;
  auto key = val.data();
  auto len = val.length();
  const int64_t c1 = 0xcc9e2d51ull;
  const int64_t c2 = 0x1b873593ull;
  const int64_t UINT_MASK = 0xffffffffull;
  int64_t lh1 = seed;
  const int32_t* blocks = reinterpret_cast<const int32_t*>(key);
  int nblocks = len / 4;
  const uint8_t* tail = reinterpret_cast<const uint8_t*>(key + nblocks * 4);
  for (int i = 0; i < nblocks; i++) {
    int64_t lk1 = static_cast<int64_t>(blocks[i]);

    // k1 *= c1;
    lk1 *= c1;
    lk1 &= UINT_MASK;

    lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

    lk1 *= c2;
    lk1 = lk1 & UINT_MASK;
    lh1 ^= lk1;
    lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >> 19);

    lh1 = lh1 * 5 + 0xe6546b64ull;
    lh1 = UINT_MASK & lh1;
  }

  // tail
  int64_t lk1 = 0;

  switch (len & 3) {
    case 3:
      lk1 = (tail[2] & 0xff) << 16;
    case 2:
      lk1 |= (tail[1] & 0xff) << 8;
    case 1:
      lk1 |= (tail[0] & 0xff);
      lk1 *= c1;
      lk1 = UINT_MASK & lk1;
      lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

      lk1 *= c2;
      lk1 = lk1 & UINT_MASK;

      lh1 ^= lk1;
  }

  // finalization
  lh1 ^= len;

  lh1 ^= lh1 >> 16;
  lh1 *= 0x85ebca6b;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 13;

  lh1 *= 0xc2b2ae35;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 16;

  return static_cast<int32_t>(lh1 & UINT_MASK);
}

template <typename T>
inline enable_if_string_or_stringview<T> hash32(T val, bool validity) {
  return hash32(val, validity, 0);
}

template <typename T>
inline enable_if_decimal<T> hash32(T val, bool validity, int32_t seed) {
  if (!validity) return seed;
  auto arr = val.ToBytes();
  auto key = arr.data();
  auto len = arr.size();
  const int64_t c1 = 0xcc9e2d51ull;
  const int64_t c2 = 0x1b873593ull;
  const int64_t UINT_MASK = 0xffffffffull;
  int64_t lh1 = seed;
  const int32_t* blocks = reinterpret_cast<const int32_t*>(key);
  int nblocks = len / 4;
  const uint8_t* tail = reinterpret_cast<const uint8_t*>(key + nblocks * 4);
  for (int i = 0; i < nblocks; i++) {
    int64_t lk1 = static_cast<int64_t>(blocks[i]);

    // k1 *= c1;
    lk1 *= c1;
    lk1 &= UINT_MASK;

    lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

    lk1 *= c2;
    lk1 = lk1 & UINT_MASK;
    lh1 ^= lk1;
    lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >> 19);

    lh1 = lh1 * 5 + 0xe6546b64ull;
    lh1 = UINT_MASK & lh1;
  }

  // tail
  int64_t lk1 = 0;

  switch (len & 3) {
    case 3:
      lk1 = (tail[2] & 0xff) << 16;
    case 2:
      lk1 |= (tail[1] & 0xff) << 8;
    case 1:
      lk1 |= (tail[0] & 0xff);
      lk1 *= c1;
      lk1 = UINT_MASK & lk1;
      lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

      lk1 *= c2;
      lk1 = lk1 & UINT_MASK;

      lh1 ^= lk1;
  }

  // finalization
  lh1 ^= len;

  lh1 ^= lh1 >> 16;
  lh1 *= 0x85ebca6b;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 13;

  lh1 *= 0xc2b2ae35;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 16;

  return static_cast<int32_t>(lh1 & UINT_MASK);
}

template <typename T>
inline enable_if_decimal<T> hash32(T val, bool validity) {
  return hash32(val, validity, 0);
}

inline int64_t double_to_long_bits(double value) {
  int64_t result;
  memcpy(&result, &value, sizeof(result));
  return result;
}

template <typename T>
inline enable_if_not_string_or_decimal<T> hash32(T in, bool validity, int32_t seed) {
  return validity ? hash32(double_to_long_bits(static_cast<double>(in)), seed) : seed;
}

template <typename T>
inline enable_if_not_string_or_decimal<T> hash32(T in, bool validity) {
  return hash32(in, validity, 0);
}

// Wrappers for the varlen types

}  // namespace murmurhash32
}  // namespace thirdparty
}  // namespace sparkcolumnarplugin
