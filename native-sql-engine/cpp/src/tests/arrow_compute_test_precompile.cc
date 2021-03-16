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
#include "precompile/gandiva.h"
#include "tests/test_utils.h"

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
  int32_t left_precision = 14;
  int32_t right_precision = 13;
  int32_t out_precision = 22;
  int32_t out_scale = 10;
  auto res = castDECIMAL(left, left_precision, left_scale, out_precision, out_scale);
  ASSERT_EQ(res, arrow::Decimal128("32342423.0128750000"));
  bool overflow = false;
  res = castDECIMALNullOnOverflow(left, left_precision, left_scale, out_precision, 
                                  out_scale, &overflow);
  ASSERT_EQ(res, arrow::Decimal128("32342423.0128750000"));
  res = add(left, left_precision, left_scale, right, right_precision, right_scale,
            17, 9);
  ASSERT_EQ(res, arrow::Decimal128("32344770.025749535"));
  res = subtract(left, left_precision, left_scale, right, right_precision, right_scale,
                 17, 9);
  ASSERT_EQ(res, arrow::Decimal128("32340076.000000465"));
  res = multiply(left, left_precision, left_scale, right, right_precision, right_scale,
                 28, 15, &overflow);
  ASSERT_EQ(res, arrow::Decimal128("75908083204.874689064638125"));
  res = divide(left, left_precision, left_scale, right, right_precision, right_scale,
               out_precision, out_scale, &overflow);
  ASSERT_EQ(res, arrow::Decimal128("13780.2495094037"));
  res = round(left, left_precision, left_scale, &overflow, 4);
  ASSERT_EQ(res, arrow::Decimal128("32342423.0129"));
  res = arrow::Decimal128("-32342423.012875").Abs();
  ASSERT_EQ(res, left);
}

TEST(TestArrowCompute, ArithmeticComparisonTest) {
  double v1 = std::numeric_limits<double>::quiet_NaN();
  double v2 = 1.0;
  bool res = less_than_with_nan(v1, v2);
  ASSERT_EQ(res, false);
  res = less_than_with_nan(v1, v1);
  ASSERT_EQ(res, false);
  res = less_than_or_equal_to_with_nan(v1, v2);
  ASSERT_EQ(res, false);
  res = less_than_or_equal_to_with_nan(v1, v1);
  ASSERT_EQ(res, true);
  res = greater_than_with_nan(v1, v2);
  ASSERT_EQ(res, true);
  res = greater_than_with_nan(v1, v1);
  ASSERT_EQ(res, false);
  res = greater_than_or_equal_to_with_nan(v1, v2);
  ASSERT_EQ(res, true);
  res = greater_than_or_equal_to_with_nan(v1, v1);
  ASSERT_EQ(res, true);
  res = equal_with_nan(v1, v2);
  ASSERT_EQ(res, false);
  res = equal_with_nan(v1, v1);
  ASSERT_EQ(res, true);
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
