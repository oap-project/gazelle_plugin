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
#include <arrow/type_fwd.h>
#include <stdint.h>
#include <string.h>

#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace sparkcolumnarplugin {
namespace thirdparty {
namespace murmurhash64 {

template <typename T>
using is_int64 = std::is_same<int64_t, T>;

template <typename T>
using enable_if_int64 =
    typename std::enable_if<is_int64<T>::value, int64_t>::type;

template <typename T>
using is_string = std::is_same<std::string, T>;

template <typename T>
using enable_if_string =
    typename std::enable_if<is_string<T>::value, int64_t>::type;

template <typename T>
using is_string_or_int64 =
    std::integral_constant<bool, is_string<T>::value || is_int64<T>::value>;

template <typename T>
using enable_if_not_int64 =
    typename std::enable_if<!is_string_or_int64<T>::value, int64_t>::type;

static inline int64_t rotate_left(int64_t val, int distance) {
  return (val << distance) | (val >> (64 - distance));
}

static inline int64_t fmix64(int64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccduLL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53uLL;
  k ^= k >> 33;
  return k;
}

template <typename T>
inline enable_if_int64<T> hash64(T val, int32_t seed) {
  int64_t h1 = seed;
  int64_t h2 = seed;

  int64_t c1 = 0x87c37b91114253d5ull;
  int64_t c2 = 0x4cf5ad432745937full;

  int length = 8;
  int64_t k1 = 0;

  k1 = val;
  k1 *= c1;
  k1 = rotate_left(k1, 31);
  k1 *= c2;
  h1 ^= k1;

  h1 ^= length;
  h2 ^= length;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;

  // h2 += h1;
  // murmur3_128 should return 128 bit (h1,h2), now we return only 64bits,
  return h1;
}

static inline int64_t double_to_long_bits(double value) {
  int64_t result;
  memcpy(&result, &value, sizeof(result));
  return result;
}

template <typename T>
inline enable_if_not_int64<T> hash64(T in, bool validity, int32_t seed) {
  return validity ? hash64(double_to_long_bits(static_cast<double>(in)), seed)
                  : seed;
}

template <typename T>
inline enable_if_not_int64<T> hash64(T in, bool validity) {
  return hash64(in, validity, 0);
}

template <typename T>
inline enable_if_string<T> int64_t hash64(T val, bool validity, int32_t seed) {
  if (!validity) return seed;
  auto key = val.data();
  auto len = val.length();
  int64_t h1 = seed;
  int64_t h2 = seed;
  int64_t c1 = 0x87c37b91114253d5ull;
  int64_t c2 = 0x4cf5ad432745937full;

  const int64_t* blocks = reinterpret_cast<const int64_t*>(key);
  int nblocks = len / 16;
  for (int i = 0; i < nblocks; i++) {
    int64_t k1 = blocks[i * 2 + 0];
    int64_t k2 = blocks[i * 2 + 1];

    k1 *= c1;
    k1 = rotate_left(k1, 31);
    k1 *= c2;
    h1 ^= k1;
    h1 = rotate_left(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;
    k2 *= c2;
    k2 = rotate_left(k2, 33);
    k2 *= c1;
    h2 ^= k2;
    h2 = rotate_left(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  // tail
  int64_t k1 = 0;
  int64_t k2 = 0;

  const uint8_t* tail = reinterpret_cast<const uint8_t*>(key + nblocks * 16);
  switch (len & 15) {
    case 15:
      k2 = static_cast<int64_t>(tail[14]) << 48;
    case 14:
      k2 ^= static_cast<int64_t>(tail[13]) << 40;
    case 13:
      k2 ^= static_cast<int64_t>(tail[12]) << 32;
    case 12:
      k2 ^= static_cast<int64_t>(tail[11]) << 24;
    case 11:
      k2 ^= static_cast<int64_t>(tail[10]) << 16;
    case 10:
      k2 ^= static_cast<int64_t>(tail[9]) << 8;
    case 9:
      k2 ^= static_cast<int64_t>(tail[8]);
      k2 *= c2;
      k2 = rotate_left(k2, 33);
      k2 *= c1;
      h2 ^= k2;
    case 8:
      k1 ^= static_cast<int64_t>(tail[7]) << 56;
    case 7:
      k1 ^= static_cast<int64_t>(tail[6]) << 48;
    case 6:
      k1 ^= static_cast<int64_t>(tail[5]) << 40;
    case 5:
      k1 ^= static_cast<int64_t>(tail[4]) << 32;
    case 4:
      k1 ^= static_cast<int64_t>(tail[3]) << 24;
    case 3:
      k1 ^= static_cast<int64_t>(tail[2]) << 16;
    case 2:
      k1 ^= static_cast<int64_t>(tail[1]) << 8;
    case 1:
      k1 ^= static_cast<int64_t>(tail[0]) << 0;
      k1 *= c1;
      k1 = rotate_left(k1, 31);
      k1 *= c2;
      h1 ^= k1;
  }

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  // h2 += h1;
  // returning 64-bits of the 128-bit hash.
  return h1;
}

template <typename T>
inline enable_if_string<T> int64_t hash64(T val, bool validity) {
  return hash64(val, validity, 0);
}

// Wrappers for the varlen types

}  // namespace murmurhash64
}  // namespace thirdparty
}  // namespace sparkcolumnarplugin
