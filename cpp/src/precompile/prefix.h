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

#include "array.h"
#include <byteswap.h>

namespace sparkcolumnarplugin {
namespace precompile {

template <typename ArrayType>
class PrefixCalculator {
 public:
  PrefixCalculator(const std::vector<std::shared_ptr<ArrayType>>& in, bool nulls_first) 
   : in_(in), nullAsLeast_(nulls_first) {
     isLittleEndien_ = isCPULittleEndian();
   }
  
  ~PrefixCalculator() {}

  template <typename T>
  auto calPrefix(int array_id, int64_t id, uint64_t& prefix) -> 
      typename std::enable_if_t<std::is_same<T, DoubleArray>::value || 
                                std::is_same<T, FloatArray>::value> {
    if (in_[array_id]->null_count() > 0 && in_[array_id]->IsNull(id)) {
      if (nullAsLeast_) {
        prefix = 0;
      } else {
        prefix = UINT64_MAX;
      }
    } else {
      double val = (double)in_[array_id]->GetView(id);
      uint64_t bits;
      if (std::isnan(val)) {
        bits = 0x7ff8000000000000L;
      } else {
        bits = doubleToRawBits(val);
      }
      uint64_t mask = -(bits >> 63) | 0x8000000000000000L;
      prefix = bits ^ mask;
    }
  }

  template <typename T>
  auto calPrefix(int array_id, int64_t id, uint64_t& prefix) -> 
      typename std::enable_if_t<std::is_same<T, StringArray>::value> {
    uint64_t mask = 0;
    if (in_[array_id]->null_count() > 0 && in_[array_id]->IsNull(id)) {
      if (nullAsLeast_) {
        prefix = 0;
      } else {
        prefix = UINT64_MAX;
      }
    } else {
      std::string val = in_[array_id]->GetString(id);
      uint32_t bytes = val.size();
      if (isLittleEndien_) {
        if (bytes >= 8) {
          memcpy(&prefix, val.c_str(), sizeof prefix);
        } else if (bytes > 4) {
          memcpy(&prefix, val.c_str(), sizeof prefix);
          mask = (1L << (8 - bytes) * 8) - 1;
        } else if (bytes > 0) {
          uint32_t tmp = 0;
          memcpy(&tmp, val.c_str(), sizeof tmp);
          prefix = (uint64_t)tmp;
          mask = (1L << (8 - bytes) * 8) - 1;
        } else {
          prefix = 0;
        }
        prefix = bswap_64(prefix);
      } else {
        if (bytes >= 8) {
          memcpy(&prefix, val.c_str(), sizeof prefix);
        } else if (bytes > 4) {
          memcpy(&prefix, val.c_str(), sizeof prefix);
          mask = (1L << (8 - bytes) * 8) - 1;
        } else if (bytes > 0) {
          uint32_t tmp = 0;
          memcpy(&tmp, val.c_str(), sizeof tmp);
          prefix = (uint64_t)tmp;
          prefix = prefix << 32;
          mask = (1L << (8 - bytes) * 8) - 1;
        } else {
          prefix = 0;
        }
      }
      prefix &= ~mask;
    }
  }

  template <typename T>
  auto calPrefix(int array_id, int64_t id, uint64_t& prefix) -> 
      typename std::enable_if_t<!std::is_same<T, StringArray>::value && 
                                !std::is_same<T, DoubleArray>::value &&
                                !std::is_same<T, FloatArray>::value> {
    if (in_[array_id]->null_count() > 0 && in_[array_id]->IsNull(id)) {
      if (nullAsLeast_) {
        prefix = 0;
      } else {
        prefix = UINT64_MAX;
      }
    } else {
      prefix = (uint64_t)in_[array_id]->GetView(id);
    }
  }

 private:
  static inline uint64_t doubleToRawBits(double x) {
    uint64_t bits;
    memcpy(&bits, &x, sizeof bits);
    return bits;
  }

  // This method is used to check CPU is Little-endian or Big-endian.
  bool isCPULittleEndian() {
    union {
      uint32_t a;
      uint8_t b;
    } c;
    
    c.a = 1;
    // True: Little-endian
    // False: Big-endian
    return (c.b == 1);
  }

  std::vector<std::shared_ptr<ArrayType>> in_;
  bool nullAsLeast_;
  bool isLittleEndien_;
};

}  // namespace precompile
}  // namespace sparkcolumnarplugin
