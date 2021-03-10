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
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "precompile/gandiva.h"

using arrow::int64;
using arrow::uint32;
using gandiva::TreeExprBuilder;

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowComputeWindow, DoubleTest) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("col_int", arrow::int32()), field("col_dou", arrow::float64())});
  std::vector<std::string> input_data = {
      "[1, 2, 1]",
      "[35.612, 37.244, 82.664]"};
  MakeInputBatch(input_data, sch, &input_batch);

  std::shared_ptr<Field> res = field("window_res", arrow::float64());

  auto f_window = TreeExprBuilder::MakeExpression(TreeExprBuilder::MakeFunction("window", {
      TreeExprBuilder::MakeFunction("sum",
          {TreeExprBuilder::MakeField(field("col_dou", arrow::float64()))}, null()),
      TreeExprBuilder::MakeFunction("partitionSpec",
          {TreeExprBuilder::MakeField(field("col_int", arrow::int32()))}, null()),
  }, binary()), res);

  arrow::compute::ExecContext ctx;
  std::shared_ptr<CodeGenerator> expr;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), sch, {f_window}, {res}, &expr, true))
  ASSERT_NOT_OK(expr->evaluate(input_batch, nullptr))
  ASSERT_NOT_OK(expr->finish(&out))

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_output_data = {
      "[118.276, 37.244, 118.276]"};

  MakeInputBatch(expected_output_data, arrow::schema({res}), &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(out.at(0).get())));
}

TEST(TestArrowComputeWindow, DecimalTest) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("col_int", arrow::int32()), field("col_dec", arrow::decimal128(8, 3))});
  std::vector<std::string> input_data = {
      "[1, 2, 1]",
      "[\"35.612\", \"37.244\", \"82.664\"]"};
  MakeInputBatch(input_data, sch, &input_batch);

  std::shared_ptr<Field> res = field("window_res", arrow::decimal128(8, 3));

  auto f_window = TreeExprBuilder::MakeExpression(TreeExprBuilder::MakeFunction("window", {
      TreeExprBuilder::MakeFunction("sum",
                                    {TreeExprBuilder::MakeField(field("col_dec", arrow::decimal128(8, 3)))}, null()),
      TreeExprBuilder::MakeFunction("partitionSpec",
                                    {TreeExprBuilder::MakeField(field("col_int", arrow::int32()))}, null()),
  }, binary()), res);

  arrow::compute::ExecContext ctx;
  std::shared_ptr<CodeGenerator> expr;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), sch, {f_window}, {res}, &expr, true))
  ASSERT_NOT_OK(expr->evaluate(input_batch, nullptr))
  ASSERT_NOT_OK(expr->finish(&out))

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_output_data = {
      "[\"118.276\", \"37.244\", \"118.276\"]"};

  MakeInputBatch(expected_output_data, arrow::schema({res}), &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(out.at(0).get())));
}

TEST(TestArrowComputeWindow, DecimalAvgTest) {
  return; // fixme decimal avg not supported?
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("col_int", arrow::int32()), field("col_dec", arrow::decimal128(8, 3))});
  std::vector<std::string> input_data = {
      "[1, 2, 1]",
      "[\"35.612\", \"37.244\", \"82.664\"]"};
  MakeInputBatch(input_data, sch, &input_batch);

  std::shared_ptr<Field> res = field("window_res", arrow::decimal128(8, 3));

  auto f_window = TreeExprBuilder::MakeExpression(TreeExprBuilder::MakeFunction("window", {
      TreeExprBuilder::MakeFunction("avg",
                                    {TreeExprBuilder::MakeField(field("col_dec", arrow::decimal128(8, 3)))}, null()),
      TreeExprBuilder::MakeFunction("partitionSpec",
                                    {TreeExprBuilder::MakeField(field("col_int", arrow::int32()))}, null()),
  }, binary()), res);

  arrow::compute::ExecContext ctx;
  std::shared_ptr<CodeGenerator> expr;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), sch, {f_window}, {res}, &expr, true))
  ASSERT_NOT_OK(expr->evaluate(input_batch, nullptr))
  ASSERT_NOT_OK(expr->finish(&out))

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_output_data = {
      "[\"118.276\", \"37.244\", \"118.276\"]"};

  MakeInputBatch(expected_output_data, arrow::schema({res}), &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(out.at(0).get())));
}

TEST(TestArrowComputeWindow, DecimalRankTest) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("col_int", arrow::int32()), field("col_dec", arrow::decimal128(8, 3))});
  std::vector<std::string> input_data = {
      "[1, 2, 1]",
      "[\"35.612\", \"37.244\", \"35.613\"]"};
  MakeInputBatch(input_data, sch, &input_batch);

  std::shared_ptr<Field> res = field("window_res", arrow::int32());

  auto f_window = TreeExprBuilder::MakeExpression(TreeExprBuilder::MakeFunction("window", {
      TreeExprBuilder::MakeFunction("rank_desc",
                                    {TreeExprBuilder::MakeField(field("col_dec", arrow::decimal128(8, 3)))}, null()),
      TreeExprBuilder::MakeFunction("partitionSpec",
                                    {TreeExprBuilder::MakeField(field("col_int", arrow::int32()))}, null()),
  }, binary()), res);

  arrow::compute::ExecContext ctx;
  std::shared_ptr<CodeGenerator> expr;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), sch, {f_window}, {res}, &expr, true))
  ASSERT_NOT_OK(expr->evaluate(input_batch, nullptr))
  ASSERT_NOT_OK(expr->finish(&out))

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_output_data = {
      "[2, 1, 1]"};

  MakeInputBatch(expected_output_data, arrow::schema({res}), &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(out.at(0).get())));
}

TEST(TestArrowComputeWindow, DecimalRankTest2) {
  std::shared_ptr<arrow::RecordBatch> input_batch;
  auto sch = arrow::schema({field("col_int", arrow::int32()), field("col_dec", arrow::decimal128(8, 3))});
  std::vector<std::string> input_data = {
      "[1, 2, 1]",
      "[\"35.612\", \"37.244\", \"35.612\"]"};
  MakeInputBatch(input_data, sch, &input_batch);

  std::shared_ptr<Field> res = field("window_res", arrow::int32());

  auto f_window = TreeExprBuilder::MakeExpression(TreeExprBuilder::MakeFunction("window", {
      TreeExprBuilder::MakeFunction("rank_desc",
                                    {TreeExprBuilder::MakeField(field("col_dec", arrow::decimal128(8, 3)))}, null()),
      TreeExprBuilder::MakeFunction("partitionSpec",
                                    {TreeExprBuilder::MakeField(field("col_int", arrow::int32()))}, null()),
  }, binary()), res);

  arrow::compute::ExecContext ctx;
  std::shared_ptr<CodeGenerator> expr;
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), sch, {f_window}, {res}, &expr, true))
  ASSERT_NOT_OK(expr->evaluate(input_batch, nullptr))
  ASSERT_NOT_OK(expr->finish(&out))

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_output_data = {
      "[1, 1, 1]"};

  MakeInputBatch(expected_output_data, arrow::schema({res}), &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(out.at(0).get())));
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
