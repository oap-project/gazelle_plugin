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

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "precompile/array.h"
#include "tests/test_utils.h"
#include "precompile/gandiva.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, BooleanArrayTest) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("bool_col", arrow::boolean())});
  std::vector<std::string> input_data = {
      "[true, true, false, false, true, null, true, false]"};
  MakeInputBatch(input_data, sch, &input_batch);

  auto bool_array = std::make_shared<precompile::BooleanArray>(input_batch->column(0));
  for (int i = 0; i < bool_array->length(); i++) {
    if (bool_array->IsNull(i)) {
      std::cout << i << ": Null" << std::endl;
    } else {
      std::cout << i << ": " << bool_array->GetView(i) << std::endl;
    }
  }
}

TEST(TestArrowCompute, ArithmeticDecimalTest) {
  auto left = arrow::Decimal128("32342423.012875");
  auto right = arrow::Decimal128("2347.012874535");
  int32_t left_scale = 6;
  int32_t right_scale = 9;
  int32_t out_precision = 22;
  int32_t out_scale = 10;
  auto res = castDECIMAL(left, left_scale, out_precision, out_scale);
  std::cout << "castDECIMAL res is: " << res.ToString(out_scale) << std::endl;
  res = add(left, left_scale, right, right_scale, out_precision, out_scale);
  std::cout << "add res is: " << res.ToString(out_scale) << std::endl;
  res = subtract(left, left_scale, right, right_scale, out_precision, out_scale);
  std::cout << "subtract res is: " << res.ToString(out_scale) << std::endl;
  res = multiply(left, left_scale, right, right_scale, out_precision, out_scale);
  std::cout << "multiply res is: " << res.ToString(out_scale) << std::endl;
  res = divide(left, left_scale, right, right_scale, out_precision, out_scale);
  std::cout << "divide res is: " << res.ToString(out_scale) << std::endl;
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin