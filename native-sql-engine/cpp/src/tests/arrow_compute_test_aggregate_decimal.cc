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
#include <arrow/util/decimal.h>
#include <gandiva/basic_decimal_scalar.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"
using arrow::Decimal128;
using gandiva::DecimalScalar128;

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, AggregateTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f_res = field("res", int32());
  auto f1 = field("f1", decimal128(6, 6));
  auto f2 = field("f2", int64());

  auto f_min = field("min", decimal128(6, 6));
  auto f_max = field("max", decimal128(6, 6));
  auto f_sum = field("sum", decimal128(6, 6));
  auto f_sum1 = field("sum1", decimal128(6, 6));
  auto f_count1 = field("count1", int64());
  auto f_sum2 = field("sum2", decimal128(6, 6));
  auto f_count2 = field("count2", int64());
  auto f_avg = field("avg", decimal128(6, 6));
  auto f_stddev_count = field("stddev_count", float64());
  auto f_stddev_avg = field("stddev_avg", float64());
  auto f_stddev_m2 = field("stddev_m2", float64());

  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);

  auto n_min = TreeExprBuilder::MakeFunction("action_min", {arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {arg1}, uint32());
  auto n_sum_count =
      TreeExprBuilder::MakeFunction("action_sum_count", {arg1}, uint32());
  auto n_sum_count_merge = TreeExprBuilder::MakeFunction(
      "action_sum_count_merge", {arg1, arg2}, uint32());
  auto n_avg_by_count = TreeExprBuilder::MakeFunction("action_avgByCount",
                                                      {arg1, arg2}, uint32());
  auto n_stddev_partial = TreeExprBuilder::MakeFunction(
      "action_stddev_samp_partial", {arg1}, uint32());
  auto n_proj = TreeExprBuilder::MakeFunction("aggregateExpressions",
                                              {arg1, arg2}, uint32());
  auto n_action = TreeExprBuilder::MakeFunction(
      "aggregateActions",
      {n_min, n_max, n_sum, n_sum_count, n_sum_count_merge, n_avg_by_count,
       n_stddev_partial},
      uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "resultSchema",
      {TreeExprBuilder::MakeField(f_min), TreeExprBuilder::MakeField(f_max),
       TreeExprBuilder::MakeField(f_sum), TreeExprBuilder::MakeField(f_sum1),
       TreeExprBuilder::MakeField(f_count1), TreeExprBuilder::MakeField(f_sum2),
       TreeExprBuilder::MakeField(f_count2), TreeExprBuilder::MakeField(f_avg),
       TreeExprBuilder::MakeField(f_stddev_count),
       TreeExprBuilder::MakeField(f_stddev_avg),
       TreeExprBuilder::MakeField(f_stddev_m2)},
      uint32());
  auto n_result_expr = TreeExprBuilder::MakeFunction(
      "resultExpressions",
      {TreeExprBuilder::MakeField(f_min), TreeExprBuilder::MakeField(f_max),
       TreeExprBuilder::MakeField(f_sum), TreeExprBuilder::MakeField(f_sum1),
       TreeExprBuilder::MakeField(f_count1), TreeExprBuilder::MakeField(f_sum2),
       TreeExprBuilder::MakeField(f_count2), TreeExprBuilder::MakeField(f_avg),
       TreeExprBuilder::MakeField(f_stddev_count),
       TreeExprBuilder::MakeField(f_stddev_avg),
       TreeExprBuilder::MakeField(f_stddev_m2)},
      uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays", {n_proj, n_action, n_result, n_result_expr},
      uint32());
  auto n_child =
      TreeExprBuilder::MakeFunction("standalone", {n_aggr}, uint32());
  auto aggr_expr = TreeExprBuilder::MakeExpression(n_child, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {
      f_min,    f_max, f_sum,          f_sum1,       f_count1,   f_sum2,
      f_count2, f_avg, f_stddev_count, f_stddev_avg, f_stddev_m2};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector,
                                    ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  std::shared_ptr<ResultIteratorBase> aggr_result_iterator_base;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator_base));
  aggr_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          aggr_result_iterator_base);

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["8.762800", "2.350000", "12.887800", "4.200000", "3.768008",
      null, "5.200089", "3.872900", "0.350000", "9.350089",
      "1.126710", "0.832783", "1.109009", "2.278000", "4.200000",
      "6.888800", "5.768001", "3.768002", "5.768003", "5.768004"])",
      R"([5, 2, 8, 9, 1, 3, 78, 111, 8, 10, 8, 9, 7, 1, 1, 3, 78, 34, 8, 10])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  std::vector<std::string> input_data_2 = {
      R"(["7.040001", "7.378700", "8.000000", "3.666700", "9.999999",
      "111.000000", "9.666701", "6.010000", "7.378700", "7.378700",
      "7.030000", "6.010900", "2.010099", "9.676700", "3.666699", "8.008800",
      "13.000000", "10.090000", "10.000700", "10.000000"])",
      R"([8, 9, 7, 1, 1, 3, 78, 34, 8, 10, 5, 2, 8, 9, 1, 3, 78, 111, 8, 10])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {R"(["0.350000"])",
                                                     R"(["111.000000"])",
                                                     R"(["345.262397"])",
                                                     R"(["345.262397"])",
                                                     "[39]",
                                                     R"(["345.262397"])",
                                                     "[785]",
                                                     R"(["0.439824"])",
                                                     R"([39])",
                                                     R"([8.85288])",
                                                     R"([11113.3])"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByAggregateTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", decimal128(6, 6));
  auto arg0 = TreeExprBuilder::MakeField(f0);

  auto f_unique = field("unique", decimal128(6, 6));
  auto f_multiply = field("multiply", decimal128(6, 6));
  auto f_res = field("res", int32());

  auto f1 = field("f1", decimal128(6, 6));
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto f2 = field("f2", int64());
  auto arg2 = TreeExprBuilder::MakeField(f2);

  auto f_min = field("min", decimal128(6, 6));
  auto f_max = field("max", decimal128(6, 6));
  auto f_sum = field("sum", decimal128(6, 6));
  auto f_sum1 = field("sum1", decimal128(6, 6));
  auto f_count1 = field("count1", int64());
  auto f_sum2 = field("sum2", decimal128(6, 6));
  auto f_count2 = field("count2", int64());
  auto f_avg = field("avg", decimal128(16, 10));

  auto n_groupby =
      TreeExprBuilder::MakeFunction("action_groupby", {arg0}, uint32());
  auto n_min = TreeExprBuilder::MakeFunction("action_min", {arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {arg1}, uint32());
  auto n_sum_count =
      TreeExprBuilder::MakeFunction("action_sum_count", {arg1}, uint32());
  auto n_sum_count_merge = TreeExprBuilder::MakeFunction(
      "action_sum_count_merge", {arg1, arg2}, uint32());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avgByCount", {arg1, arg2},
                                             uint32());
  auto n_proj = TreeExprBuilder::MakeFunction("aggregateExpressions",
                                              {arg0, arg1, arg2}, uint32());
  auto n_action = TreeExprBuilder::MakeFunction(
      "aggregateActions",
      {n_groupby, n_min, n_max, n_sum, n_sum_count, n_sum_count_merge, n_avg},
      uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "resultSchema",
      {TreeExprBuilder::MakeField(f_unique), TreeExprBuilder::MakeField(f_min),
       TreeExprBuilder::MakeField(f_max), TreeExprBuilder::MakeField(f_sum),
       TreeExprBuilder::MakeField(f_sum1), TreeExprBuilder::MakeField(f_count1),
       TreeExprBuilder::MakeField(f_sum2), TreeExprBuilder::MakeField(f_count2),
       TreeExprBuilder::MakeField(f_avg)},
      uint32());
  auto n_multiply = TreeExprBuilder::MakeFunction(
      "multiply",
      {TreeExprBuilder::MakeField(f_unique),
       TreeExprBuilder::MakeDecimalLiteral(DecimalScalar128("2.5", 6, 6))},
      decimal128(6, 6));
  auto n_result_expr = TreeExprBuilder::MakeFunction(
      "resultExpressions",
      {TreeExprBuilder::MakeField(f_unique), n_multiply,
       TreeExprBuilder::MakeField(f_min), TreeExprBuilder::MakeField(f_max),
       TreeExprBuilder::MakeField(f_sum), TreeExprBuilder::MakeField(f_sum1),
       TreeExprBuilder::MakeField(f_count1), TreeExprBuilder::MakeField(f_sum2),
       TreeExprBuilder::MakeField(f_count2), TreeExprBuilder::MakeField(f_avg)},
      uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays", {n_proj, n_action, n_result, n_result_expr},
      uint32());
  auto n_child =
      TreeExprBuilder::MakeFunction("standalone", {n_aggr}, uint32());
  auto aggr_expr = TreeExprBuilder::MakeExpression(n_child, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {
      f_unique, f_multiply, f_min,  f_max,    f_sum,
      f_sum1,   f_count1,   f_sum2, f_count2, f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector,
                                    ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  std::shared_ptr<ResultIteratorBase> aggr_result_iterator_base;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator_base));
  aggr_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          aggr_result_iterator_base);

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000",
      null, "4.200000", "1.100000", "2.350000", "2.350000",
      "1.100000", "1.100000", "1.100000", "4.200000", "4.200000",
      "3.888800", "5.768000", "5.768000", "5.768000", "5.768000"])",
      R"(["8.762800", "2.350000", "12.887800", "4.200000", "3.768008",
      null, "5.200089", "3.872900", "0.350000", "9.350089",
      "1.126710", "0.832783", "1.109009", "2.278000", "4.200000",
      "6.888800", "5.768001", "3.768002", "5.768003", "5.768004"])",
      R"([5, 2, 8, 9, 1, 3, 78, 111, 8, 10, 8, 9, 7, 1, 1, 3, 78, 34, 8, 10])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  std::vector<std::string> input_data_2 = {
      R"(["6.010000", "7.378700", "8.000000", "9.666700", "10.000000",
      "10.000000", "9.666700", "6.010000", "7.378700", "7.378700",
      "6.010000", "6.010000", "6.010000", "9.666700", "9.666700", "8.000000",
      "10.000000", "10.000000", "10.000000", "10.000000"])",
      R"(["7.040001", "7.378700", "8.000000", "3.666700", "9.999999",
      "111.000000", "9.666701", "6.010000", "7.378700", "7.378700",
      "7.030000", "6.010900", "2.010099", "9.676700", "3.666699", "8.008800",
      "13.000000", "10.090000", "10.000700", "10.000000"])",
      R"([8, 9, 7, 1, 1, 3, 78, 34, 8, 10, 5, 2, 8, 9, 1, 3, 78, 111, 8, 10])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000", null,
      "6.010000", "7.378700", "8.000000", "9.666700", "10.000000"])",
      R"(["27.500000", "58.750000", "97.220000", "105.000000","144.200000", null,
      "150.250000", "184.467500", "200.000000", "241.667500", "250.000000"])",
      R"(["0.832783", "0.350000", "6.888800", "2.278000", "3.768002", null,
      "2.010099", "7.378700", "8.000000", "3.666699", "9.999999"])",
      R"(["8.762800", "9.350089", "12.887800", "5.200089", "5.768004",
      null, "7.040001", "7.378700", "8.008800", "9.676700", "111.000000"])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"([5, 3, 2, 4, 5, null, 5, 3, 2, 4, 6])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"([140, 20, 11, 89, 131, null, 57, 27, 10, 89, 211])",
      R"(["0.1121728714", "0.6025044500", "1.7978727272", "0.1784054943", "0.1896184580",
      null, "0.4930000000", "0.8198555555", "1.6008800000", "0.2997393258", "0.7776810379"])"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByAggregateWSCGTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", decimal128(6, 6));
  auto arg0 = TreeExprBuilder::MakeField(f0);

  auto f_unique = field("unique", decimal128(6, 6));
  auto f_multiply = field("multiply", decimal128(6, 6));
  auto f_res = field("res", int32());

  auto f1 = field("f1", decimal128(6, 6));
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto f2 = field("f2", int64());
  auto arg2 = TreeExprBuilder::MakeField(f2);

  auto f_min = field("min", decimal128(6, 6));
  auto f_max = field("max", decimal128(6, 6));
  auto f_sum = field("sum", decimal128(6, 6));
  auto f_sum1 = field("sum1", decimal128(6, 6));
  auto f_count1 = field("count1", int64());
  auto f_sum2 = field("sum2", decimal128(6, 6));
  auto f_count2 = field("count2", int64());
  auto f_avg = field("avg", decimal128(16, 10));

  auto n_groupby =
      TreeExprBuilder::MakeFunction("action_groupby", {arg0}, uint32());
  auto n_min = TreeExprBuilder::MakeFunction("action_min", {arg1}, uint32());
  auto n_max = TreeExprBuilder::MakeFunction("action_max", {arg1}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {arg1}, uint32());
  auto n_sum_count =
      TreeExprBuilder::MakeFunction("action_sum_count", {arg1}, uint32());
  auto n_sum_count_merge = TreeExprBuilder::MakeFunction(
      "action_sum_count_merge", {arg1, arg2}, uint32());
  auto n_avg = TreeExprBuilder::MakeFunction("action_avgByCount", {arg1, arg2},
                                             uint32());
  auto n_proj = TreeExprBuilder::MakeFunction("aggregateExpressions",
                                              {arg0, arg1, arg2}, uint32());
  auto n_action = TreeExprBuilder::MakeFunction(
      "aggregateActions",
      {n_groupby, n_min, n_max, n_sum, n_sum_count, n_sum_count_merge, n_avg},
      uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "resultSchema",
      {TreeExprBuilder::MakeField(f_unique), TreeExprBuilder::MakeField(f_min),
       TreeExprBuilder::MakeField(f_max), TreeExprBuilder::MakeField(f_sum),
       TreeExprBuilder::MakeField(f_sum1), TreeExprBuilder::MakeField(f_count1),
       TreeExprBuilder::MakeField(f_sum2), TreeExprBuilder::MakeField(f_count2),
       TreeExprBuilder::MakeField(f_avg)},
      uint32());
  auto n_multiply = TreeExprBuilder::MakeFunction(
      "multiply",
      {TreeExprBuilder::MakeField(f_unique),
       TreeExprBuilder::MakeDecimalLiteral(DecimalScalar128("2.5", 6, 6))},
      decimal128(6, 6));
  auto n_result_expr = TreeExprBuilder::MakeFunction(
      "resultExpressions",
      {TreeExprBuilder::MakeField(f_unique), n_multiply,
       TreeExprBuilder::MakeField(f_min), TreeExprBuilder::MakeField(f_max),
       TreeExprBuilder::MakeField(f_sum), TreeExprBuilder::MakeField(f_sum1),
       TreeExprBuilder::MakeField(f_count1), TreeExprBuilder::MakeField(f_sum2),
       TreeExprBuilder::MakeField(f_count2), TreeExprBuilder::MakeField(f_avg)},
      uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays", {n_proj, n_action, n_result, n_result_expr},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_aggr}, uint32());
  auto n_wscg =
      TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto aggr_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f0, f1, f2});
  std::vector<std::shared_ptr<Field>> ret_types = {
      f_unique, f_multiply, f_min,  f_max,    f_sum,
      f_sum1,   f_count1,   f_sum2, f_count2, f_avg};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector,
                                    ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  std::shared_ptr<ResultIteratorBase> aggr_result_iterator_base;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator_base));
  aggr_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          aggr_result_iterator_base);

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000",
      null, "4.200000", "1.100000", "2.350000", "2.350000",
      "1.100000", "1.100000", "1.100000", "4.200000", "4.200000",
      "3.888800", "5.768000", "5.768000", "5.768000", "5.768000"])",
      R"(["8.762800", "2.350000", "12.887800", "4.200000", "3.768008",
      null, "5.200089", "3.872900", "0.350000", "9.350089",
      "1.126710", "0.832783", "1.109009", "2.278000", "4.200000",
      "6.888800", "5.768001", "3.768002", "5.768003", "5.768004"])",
      R"([5, 2, 8, 9, 1, 3, 78, 111, 8, 10, 8, 9, 7, 1, 1, 3, 78, 34, 8, 10])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  std::vector<std::string> input_data_2 = {
      R"(["6.010000", "7.378700", "8.000000", "9.666700", "10.000000",
      "10.000000", "9.666700", "6.010000", "7.378700", "7.378700",
      "6.010000", "6.010000", "6.010000", "9.666700", "9.666700", "8.000000",
      "10.000000", "10.000000", "10.000000", "10.000000"])",
      R"(["7.040001", "7.378700", "8.000000", "3.666700", "9.999999",
      "111.000000", "9.666701", "6.010000", "7.378700", "7.378700",
      "7.030000", "6.010900", "2.010099", "9.676700", "3.666699", "8.008800",
      "13.000000", "10.090000", "10.000700", "10.000000"])",
      R"([8, 9, 7, 1, 1, 3, 78, 34, 8, 10, 5, 2, 8, 9, 1, 3, 78, 111, 8, 10])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000", null,
      "6.010000", "7.378700", "8.000000", "9.666700", "10.000000"])",
      R"(["27.500000", "58.750000", "97.220000", "105.000000","144.200000", null,
      "150.250000", "184.467500", "200.000000", "241.667500", "250.000000"])",
      R"(["0.832783", "0.350000", "6.888800", "2.278000", "3.768002", null,
      "2.010099", "7.378700", "8.000000", "3.666699", "9.999999"])",
      R"(["8.762800", "9.350089", "12.887800", "5.200089", "5.768004",
      null, "7.040001", "7.378700", "8.008800", "9.676700", "111.000000"])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"([5, 3, 2, 4, 5, null, 5, 3, 2, 4, 6])",
      R"(["15.704202", "12.050089", "19.776600", "15.878089", "24.840018",
      null, "28.101000", "22.136100", "16.008800", "26.676800", "164.090699"])",
      R"([140, 20, 11, 89, 131, null, 57, 27, 10, 89, 211])",
      R"(["0.1121728714", "0.6025044500", "1.7978727272", "0.1784054943", "0.1896184580",
      null, "0.4930000000", "0.8198555555", "1.6008800000", "0.2997393258", "0.7776810379"])"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowCompute, GroupByAggregateWithTwoKeysTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", decimal128(6, 6));
  auto f1 = field("f1", utf8());

  auto f_unique_decimal = field("unique_decimal", decimal128(6, 6));
  auto f_unique_utf = field("unique_utf", utf8());
  auto f_res = field("res", int32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);

  auto n_groupby_0 =
      TreeExprBuilder::MakeFunction("action_groupby", {arg0}, uint32());
  auto n_groupby_1 =
      TreeExprBuilder::MakeFunction("action_groupby", {arg1}, uint32());
  auto n_proj = TreeExprBuilder::MakeFunction("aggregateExpressions",
                                              {arg0, arg1}, uint32());
  auto n_action = TreeExprBuilder::MakeFunction(
      "aggregateActions", {n_groupby_0, n_groupby_1}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "resultSchema",
      {TreeExprBuilder::MakeField(f_unique_decimal),
       TreeExprBuilder::MakeField(f_unique_utf)},
      uint32());
  auto n_result_expr = TreeExprBuilder::MakeFunction(
      "resultExpressions",
      {TreeExprBuilder::MakeField(f_unique_decimal),
       TreeExprBuilder::MakeField(f_unique_utf)},
      uint32());
  auto n_aggr = TreeExprBuilder::MakeFunction(
      "hashAggregateArrays", {n_proj, n_action, n_result, n_result_expr},
      uint32());
  auto n_child =
      TreeExprBuilder::MakeFunction("standalone", {n_aggr}, uint32());
  auto aggr_expr = TreeExprBuilder::MakeExpression(n_child, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {aggr_expr};

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique_decimal,
                                                   f_unique_utf};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, expr_vector,
                                    ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> aggr_result_iterator;
  std::shared_ptr<ResultIteratorBase> aggr_result_iterator_base;
  ASSERT_NOT_OK(expr->finish(&aggr_result_iterator_base));
  aggr_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          aggr_result_iterator_base);

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000",
      null, "4.200000", "1.100000", "2.350000", "2.350000",
      "1.100000", "1.100000", "1.100000", "4.200000", "4.200000",
      "3.888800", "5.768000", "5.768000", "5.768000", "5.768000"])",
      R"(["A", "B", "C", "D", "E", "A", "D", "A", "B", "B", 
      "A", "A", "A", "D", "D", "C", "E", "E", "E", "E"])"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  std::vector<std::string> input_data_2 = {
      R"(["6.010000", "7.378700", "8.000000", "9.666700", "10.000000",
      "10.000000", "9.666700", "6.010000", "7.378700", "7.378700",
      "6.010000", "6.010000", "6.010000", "9.666700", "9.666700", "8.000000",
      "10.000000", "10.000000", "10.000000", "10.000000"])",
      R"(["F", "G", "H", "I", "J", "J", "I", "F", "G", "G",
      "F", "F", "F", "I", "I", "H", "J", "J", "J", "J"])"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(
      aggr_result_iterator->ProcessAndCacheOne(input_batch->columns()));

  ////////////////////// Finish //////////////////////////
  std::shared_ptr<arrow::RecordBatch> result_batch;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["1.100000", "2.350000", "3.888800", "4.200000", "5.768000", null,
      "6.010000", "7.378700", "8.000000", "9.666700", "10.000000"])",
      R"(["A", "B", "C", "D", "E", "A", "F", "G", "H", "I", "J"])"};
  auto res_sch = arrow::schema(ret_types);
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  if (aggr_result_iterator->HasNext()) {
    ASSERT_NOT_OK(aggr_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
