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

#include <arrow/array.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, AggregatewithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f2 = field("f2", uint64());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", int32());
  auto f_float = field("float", float64());
  auto f_res = field("res", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);
  auto n_sum = TreeExprBuilder::MakeFunction("sum", {arg_0}, uint64());
  auto n_count = TreeExprBuilder::MakeFunction("count", {arg_0}, uint64());
  auto n_sum_count = TreeExprBuilder::MakeFunction("sum_count", {arg_0}, uint64());
  auto n_avg = TreeExprBuilder::MakeFunction("avgByCount", {arg_2, arg_1}, uint64());
  auto n_min = TreeExprBuilder::MakeFunction("min", {arg_0}, uint64());
  auto n_max = TreeExprBuilder::MakeFunction("max", {arg_0}, uint64());
  auto n_stddev_samp_partial =
      TreeExprBuilder::MakeFunction("stddev_samp_partial", {arg_0}, float64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);
  auto sum_count_expr = TreeExprBuilder::MakeExpression(n_sum_count, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);
  auto min_expr = TreeExprBuilder::MakeExpression(n_min, f_res);
  auto max_expr = TreeExprBuilder::MakeExpression(n_max, f_res);
  auto stddev_samp_partial_expr =
      TreeExprBuilder::MakeExpression(n_stddev_samp_partial, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      sum_expr, count_expr, sum_count_expr,          avg_expr,
      min_expr, max_expr,   stddev_samp_partial_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {
      f_sum, f_count, f_sum, f_count, f_float, f_res, f_res, f_float, f_float, f_float};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  std::vector<std::string> input_data_string = {"[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]",
                                                "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
                                                "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  std::vector<std::string> input_data_2_string = {
      "[8, 10, 9, 20, null, 42, 28, 32, 54, 70]", "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
      "[8, 10, 9, 20, null, 42, 28, 32, 54, 70]"};
  MakeInputBatch(input_data_2_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[601]", "[19]", "[601]", "[19]",      "[30.05]",
      "[8]",   "[70]", "[19]",  "[31.6316]", "[8080.42]"};
  auto res_sch = arrow::schema(
      {f_sum, f_count, f_sum, f_count, f_float, f_res, f_res, f_float, f_float, f_float});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint32());
  auto f_count = field("count", uint32());
  auto f_avg = field("avg", uint32());
  auto f_res = field("res", uint32());
  auto f_m2 = field("m2", uint32());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {n_split, arg1}, uint32());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avg", {n_split, arg1}, uint32());
  auto n_sum_count =
      TreeExprBuilder::MakeFunction("action_sum_count", {n_split, arg1}, uint32());
  auto n_min = TreeExprBuilder::MakeFunction("action_min", {n_split, arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {n_split, arg1}, uint32());
  auto n_stddev_samp_partial = TreeExprBuilder::MakeFunction("action_stddev_samp_partial",
                                                             {n_split, arg1}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);
  auto sum_count_expr = TreeExprBuilder::MakeExpression(n_sum_count, f_res);
  auto avg_min = TreeExprBuilder::MakeExpression(n_min, f_res);
  auto avg_max = TreeExprBuilder::MakeExpression(n_max, f_res);
  auto stddev_samp_partial_expr =
      TreeExprBuilder::MakeExpression(n_stddev_samp_partial, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr,    sum_expr, count_expr, avg_expr,
      sum_count_expr, avg_min,  avg_max,    stddev_samp_partial_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum,   f_count, f_avg,
                                                   f_sum,    f_count, f_res,   f_res,
                                                   f_count,  f_avg,   f_m2};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, null, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, null, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, null, 6, 7, 8 ,9, 10]",
      "[8, 10, 9, 20, 45, 10, 42, 28, 32, 54, 70]",
      "[8, 5, 3, 5, 9, 2, 7, 4, 4, 6, 7]",
      "[1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]",
      "[8, 10, 9, 20, 45, 10, 42, 28, 32, 54, 70]",
      "[8, 5, 3, 5, 9, 2, 7, 4, 4, 6, 7]",
      "[1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]",
      "[1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]",
      "[8, 5, 3, 5, 9, 2, 7, 4, 4, 6, 7]",
      "[1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]",
      "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]"};
  auto res_sch = arrow::schema({f_unique, f_sum, f_count, f_avg, f_sum, f_count, f_res,
                                f_res, f_count, f_avg, f_m2});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAvgWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f_sum = field("sum", float64());
  auto f_count = field("count", int64());
  auto f_unique = field("unique", utf8());
  auto f_avg = field("avg", float64());
  auto f_res = field("res", uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0}, utf8());

  auto arg_sum = TreeExprBuilder::MakeField(f_sum);
  auto arg_count = TreeExprBuilder::MakeField(f_count);
  auto n_split = TreeExprBuilder::MakeFunction(
      "splitArrayListWithAction", {n_pre, arg0, arg_sum, arg_count}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique = TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, utf8());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avgByCount",
                                             {n_split, arg_sum, arg_count}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto avg_expr = TreeExprBuilder::MakeExpression(n_avg, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     avg_expr};
  auto sch = arrow::schema({f0, f_sum, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[1, 4, 9, 16, 25, 25, 16, 1, 4, 4, 1, 1, 1, 16, 16, 9, 25, 25, 25, 25]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD", "LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[36, 49, 64, 81, 100, 100, 81, 36, 49, 49, 36, 36, 36, 81, 81, 64, 100, 100, 100, "
      "100]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ", "LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 4, 9, 64, 25, 25, 100, 1, 4, 49, 36, 36, 1, 81, 16, 81, 25, 64, 25, 25]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"};
  auto res_sch = arrow::schema({f_unique, f_avg});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByCountAllWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f_unique = field("unique", utf8());
  auto f_count = field("avg", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0}, utf8());

  auto n_split =
      TreeExprBuilder::MakeFunction("splitArrayListWithAction", {n_pre, arg0}, uint32());

  auto n_unique = TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, utf8());
  auto n_count =
      TreeExprBuilder::MakeFunction("action_countLiteral_1", {n_split}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_unique);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_count);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f_count});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD", "LA", "LA", "NY", "AU", "AU", "AU", "AU"])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ", "LA", "HZ", "LA", "WH", "NY", "WH", "WH"])"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  auto res_sch = arrow::schema({f_unique, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByTwoAggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f2 = field("f2", uint32());
  auto f_unique_0 = field("unique", uint32());
  auto f_unique_1 = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_res = field("res", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0, arg1}, uint32());

  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1, arg2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique_0 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_unique_1 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg2}, uint32());

  auto unique_expr_0 = TreeExprBuilder::MakeExpression(n_unique_0, f_res);
  auto unique_expr_1 = TreeExprBuilder::MakeExpression(n_unique_1, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr_0, unique_expr_1, sum_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_0, f_unique_1, f_sum};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]", "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]",
      "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]"};
  auto res_sch = arrow::schema({f_unique_0, f_unique_1, f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByTwoUtf8AggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", utf8());
  auto f2 = field("f2", uint32());
  auto f_unique_0 = field("unique", utf8());
  auto f_unique_1 = field("unique", utf8());
  auto f_sum = field("sum", uint64());
  auto f_res = field("res", uint64());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg0, arg1}, uint32());

  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1, arg2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique_0 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_unique_1 =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg2}, uint32());

  auto unique_expr_0 = TreeExprBuilder::MakeExpression(n_unique_0, f_res);
  auto unique_expr_1 = TreeExprBuilder::MakeExpression(n_unique_1, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr_0, unique_expr_1, sum_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_0, f_unique_1, f_sum};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["a", "b", "c", "d", "e", "e", "d", "a", "b", "b", "a", "a", "a", "d", "d", "c", "e", "e", "e", "e"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      R"([1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["f", "g", "h", "i", "j", "j", "i", "g", "h", "i", "g", "g", "g", "j", "i", "f", "f", "i", "j", "j"])",
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "DL", "NY", "LA", "DL", "DL", "DL", "AU", "LA", "CD", "CD", "LA", "AU", "AU"])",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])",
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY", "LA", "AU"])",
      "[5, 6, 6, 16, 30, 24, 31, 15, 44, 49]"};
  auto res_sch = arrow::schema({f_unique_0, f_unique_1, f_sum});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);

  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAggregateWithMultipleBatchOutputWoKeyTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", uint64());
  auto f_res = field("res", uint64());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_split =
      TreeExprBuilder::MakeFunction("splitArrayListWithAction", {n_pre, arg1}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {n_split, arg1}, uint32());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]", "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  auto res_sch = arrow::schema({f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, StddevSampFinalTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", float64());
  auto f2 = field("f2", float64());

  auto f_float = field("float", float64());
  auto f_res = field("res", uint32());

  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);

  auto n_stddev_samp_final = TreeExprBuilder::MakeFunction(
      "stddev_samp_final", {arg_0, arg_1, arg_2}, float64());
  auto final_expr = TreeExprBuilder::MakeExpression(n_stddev_samp_final, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {final_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_float};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  std::vector<std::string> input_data_string = {
      "[2, 32, 14, 16, 18, 3, 4, 7, 9, 10]", "[2, 32, 14, 16, 18, 12, 32, 11, 12, 14]",
      "[2, 16, 14, 16, 18, 23, 32, 45, 43, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  std::vector<std::string> input_data_2_string = {"[8, 57, 59, 12, 1, 12, 3, 5, 7, 8]",
                                                  "[8, 57, 59, 12, 1, 15, 21, 13, 15, 6]",
                                                  "[8, 57, 59, 12, 1, 8, 6, 3, 6, 12]"};
  MakeInputBatch(input_data_2_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[21.0435]"};
  auto res_sch = arrow::schema({f_float});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByStddevSampPartialWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f_1 = field("f1", float64());
  auto f_2 = field("f2", int64());
  auto f_unique = field("unique", utf8());
  auto f_count = field("count", int64());
  auto f_avg = field("avg", float64());
  auto f_res = field("res", uint32());
  auto f_m2 = field("m2", float64());

  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_0}, utf8());
  auto arg_1 = TreeExprBuilder::MakeField(f_1);
  auto arg_2 = TreeExprBuilder::MakeField(f_2);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg_0, arg_1, arg_2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);

  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg_0}, utf8());
  auto n_stddev_samp_partial = TreeExprBuilder::MakeFunction("action_stddev_samp_partial",
                                                             {n_split, arg_1}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto stddev_samp_partial_expr =
      TreeExprBuilder::MakeExpression(n_stddev_samp_partial, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr, stddev_samp_partial_expr};
  auto sch = arrow::schema({f0, f_1, f_2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_count, f_avg, f_m2};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "WH", "HZ", "BJ", "SH", "SH", "BJ", "BJ", "BJ", "HZ", "HZ", "SZ", "WH", "WH", "WH", "WH"])",
      "[2, 4, 9, 11, 12, 25, 12, 7, 5, 9, 9, 15, 23, 32, 2, 12, 23, 56, 35, 68]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      R"(["CD", "DL", "NY", "LA", "AU", "AU", "LA", "CD", "DL", "DL", "CD", "CD", "CD", "LA", "LA", "NY", "AU", "AU", "AU", "AU"])",
      "[12, 49, 64, 18, 20, 100, 81, 36, 12, 24, 23, 12, 6, 22, 12, 12, 12, 35, 76, 24]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      R"(["BJ", "SH", "SZ", "NY", "WH", "WH", "AU", "BJ", "SH", "DL", "CD", "CD", "BJ", "LA", "HZ", "LA", "WH", "NY", "WH", "WH"])",
      "[1, 12, 1, 25, 12, 9, 78, 10, 8, 15, 3, 7, 3, 1, 5, 6, 16, 25, 30, 22]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "SH", "SZ", "HZ", "WH", "CD", "DL", "NY" ,"LA", "AU"])",
      "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]",
      "[8.75, 7.6, 7.33333, 12.4, 28, 14.1429, 25, 31.5, 23.3333, 49.2857]",
      "[385.5, 41.2, 64.6667, 549.2, 3524, 806.857, 846, 1521, 4283.33, 7201.43]"};
  auto res_sch = arrow::schema({f_unique, f_count, f_avg, f_m2});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByStddevSampFinalWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", float64());
  auto f2 = field("f2", float64());
  auto f3 = field("f3", float64());
  auto f_unique = field("unique", uint32());
  auto f_stddev = field("stddev", float64());
  auto f_res = field("res", uint32());

  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_0}, uint32());
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);
  auto arg_3 = TreeExprBuilder::MakeField(f3);
  auto n_split = TreeExprBuilder::MakeFunction(
      "splitArrayListWithAction", {n_pre, arg_0, arg_1, arg_2, arg_3}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);

  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg_0}, uint32());
  auto n_stddev = TreeExprBuilder::MakeFunction("action_stddev_samp_final",
                                                {n_split, arg_1, arg_2, arg_3}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto stddev_expr = TreeExprBuilder::MakeExpression(n_stddev, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     stddev_expr};
  auto sch = arrow::schema({f0, f1, f2, f3});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_stddev};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, null, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[2, 4, 5, 7, 8, 2, 45, 32, 23, 12, 14, 16, 18, 19, 23, 25, 57, 59, 12, 1]",
      "[2, 4, 5, 7, 8, 2, 45, 32, 23, 12, 14, 16, 18, 19, 23, 25, 57, 59, 12, 1]",
      "[2, 4, 5, 7, 8, 2, 45, 32, 23, 12, 14, 16, 18, 19, 23, 25, 57, 59, 12, 1]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[7, 8, 4, 5, 6, 1, 34, 54, 65, 66, 78, 12, 32, 24, 32, 45, 12, 24, 35, 46]",
      "[2, 4, 5, 7, 8, 2, 45, 32, 23, 12, 14, 16, 18, 19, 23, 25, 57, 59, 12, 1]",
      "[2, 4, 5, 7, 8, 2, 45, 32, 23, 12, 14, 16, 18, 19, 23, 25, 57, 59, 12, 1]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, null, 6, 7, 8 ,9, 10]",
      "[8.49255, 6.93137, 7.6489, 13.5708, 17.4668, 1.41421, 8.52779, 6.23633, 5.58903, "
      "12.535, 24.3544]"};
  auto res_sch = arrow::schema({f_unique, f_stddev});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupbySumCountMergeTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", float64());
  auto f2 = field("f2", int64());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", float64());
  auto f_count = field("count", int64());
  auto f_res = field("res", uint32());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1, arg2}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_merge = TreeExprBuilder::MakeFunction("action_sum_count_merge",
                                               {n_split, arg1, arg2}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto merge_expr = TreeExprBuilder::MakeExpression(n_merge, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {unique_expr,
                                                                     merge_expr};
  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, null, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, null, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, null, 6, 7, 8 ,9, 10]",
      "[8, 10, 9, 20, 45, 10, 42, 28, 32, 54, 70]",
      "[8, 10, 9, 20, 45, 10, 42, 28, 32, 54, 70]"};
  auto res_sch = arrow::schema({f_unique, f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
