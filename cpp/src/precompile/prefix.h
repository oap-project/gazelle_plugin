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

namespace sparkcolumnarplugin {
namespace precompile {

template <typename ArrayType>
class PrefixCalculator {
 public:
  PrefixCalculator(const std::vector<std::shared_ptr<ArrayType>>& in, 
                   bool asc, bool nulls_first) 
   : in_(in), nullAsLeast_(nulls_first) {}
  
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

  std::vector<std::shared_ptr<ArrayType>> in_;
  bool nullAsLeast_;
};

}  // namespace precompile
}  // namespace sparkcolumnarplugin
