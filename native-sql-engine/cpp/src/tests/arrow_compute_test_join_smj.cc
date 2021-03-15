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
#include <arrow/ipc/json_simple.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowComputeMergeJoin, JoinTestUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysInner", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe,
      true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[1, 3, 3, 3, 5, 7, 9, 10]",
                                                "[1, 3, 3, 3, 5, 7, 9, 10]",
                                                "[1, 3, 3, 3, 5, 7, 9, 10]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[11, 12, 13, 14, 15, 20]", "[11, 12, 13, 14, 15, 20]",
                       "[11, 12, 13, 14, 15, 20]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 12]",
                                                  "[1, 2, 3, 4, 5, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[13, 14, 15, 15, 17, 19]",
                         "[13, 14, 15, 15, 17, 19]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 3, 3, 5, 12]", "[1, 3, 3, 3, 5, 12]", "[1, 3, 3, 3, 5, 12]",
      "[1, 3, 3, 3, 5, 12]", "[1, 3, 3, 3, 5, 12]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[13, 14, 15, 15]", "[13, 14, 15, 15]",
                            "[13, 14, 15, 15]", "[13, 14, 15, 15]",
                            "[13, 14, 15, 15]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < table_1.size(); i++) {
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingOuterJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysOuter", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe,
      true));

  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[0, 0, 2, 2, 3, 5, 7, 9, 11]", "[null, null, 2, 2, 3, 5, 7, 9, 11]",
      "[null, null, 2, 2, 3, 5, 7, 9, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[12, 13, 14, 15, 16]", "[12, 13, 14, 15, 16]",
                       "[12, 13, 14, 15, 16]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[0, 1, 2, 3, 4, 5, 6]",
                                                  "[null, 1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[0, 0, null, 2, 2, 3, null, 5, null]",
      "[null, null, null, 2, 2, 3, null, 5, null]",
      "[null, null, null, 2, 2, 3, null, 5, null]",
      "[0, 0, 1, 2, 2, 3, 4, 5, 6]", "[null, null, 1, 2, 2, 3, 4, 5, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, null, 9, null, 11, 12]",
                            "[7, null, 9, null, 11, 12]",
                            "[7, null, 9, null, 11, 12]",
                            "[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingAntiJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysAnti", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {probeArrays_expr}, {table1_f0, table1_f1},
                                    &expr_probe, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[12, 13, 14, 15, 16]", "[12, 13, 14, 15, 16]",
                       "[12, 13, 14, 15, 16]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 17]", "[7, 8, 9, 10, 11, 17]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  auto res_sch = arrow::schema({f_res, f_res});
  std::vector<std::string> expected_result_string = {"[1, 4, 6]", "[1, 4, 6]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 17]", "[8, 10, 17]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingSemiJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysSemi", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {probeArrays_expr}, {table1_f0, table1_f1},
                                    &expr_probe, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[12, 13, 14, 15, 16]", "[12, 13, 14, 15, 16]",
                       "[12, 13, 14, 15, 16]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 17]", "[7, 8, 9, 10, 11, 17]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  auto res_sch = arrow::schema({f_res, f_res});
  std::vector<std::string> expected_result_string = {"[2, 3, 5]", "[2, 3, 5]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 9, 11]", "[7, 9, 11]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingSemiJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto greater_than_function =
      TreeExprBuilder::MakeFunction("greater_than",
                                    {TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    arrow::boolean());
  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysSemi",
      {n_left_key, n_right_key, greater_than_function}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {probeArrays_expr}, {table1_f0, table1_f1},
                                    &expr_probe, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[2, 2, 3, 5, 7, 9, 11]",
                                                "[2, 7, 3, 5, 7, 9, 11]",
                                                "[2, 7, 3, 5, 7, 9, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[12, 13, 14, 15, 16]", "[12, 13, 14, 15, 16]",
                       "[12, 13, 14, 15, 16]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 1, 3, 4, 4, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 17]", "[6, 8, 10, 10, 10, 17]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  auto res_sch = arrow::schema({f_res, f_res});
  std::vector<std::string> expected_result_string = {"[2, 5]", "[1, 4]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 11]", "[6, 10]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingInnerJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto greater_than_function =
      TreeExprBuilder::MakeFunction("greater_than",
                                    {TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    arrow::boolean());
  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysInner",
      {n_left_key, n_right_key, greater_than_function}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe,
      true));

  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[1, 3, 5, 7, 9, 10]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[13, 14, 15, 15, 17, 19]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 4, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1]", "[10]", "[10]",
                                                     "[1]", "[1]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[9, 10]", "[13, 11]", "[13, 11]", "[9, 10]",
                            "[9, 4]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < table_1.size(); i++) {
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestWithTwoKeysUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", int32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", utf8());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key =
      TreeExprBuilder::MakeFunction("codegen_left_key_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1)},
                                    uint32());
  auto n_right_key =
      TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysInner", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto f_res_utf = field("res", utf8());

  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe,
      true));

  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      R"(["a", "b", "c", "e"])", R"(["A", "B", "C", "E"])", "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["f", "j", "n"])", R"(["F", "J", "N"])",
                       "[12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["a", "b", "c", "d", "e", "f"])", R"(["A", "B", "C", "D", "E", "F"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["i", "j", "k", "l", "m", "n"])",
                         R"(["I", "J", "K", "L", "M", "N"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "e", "f"])", R"(["A", "B", "C", "E", "F"])",
      "[10, 3, 1, 2, 12]", R"(["a", "b", "c", "e", "f"])",
      R"(["A", "B", "C", "E", "F"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {R"(["j","n"])", R"(["J", "N"])", "[5, 8]",
                            R"(["j", "n"])", R"(["J", "N"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeMergeJoin, JoinTestUsingAntiJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto greater_than_function =
      TreeExprBuilder::MakeFunction("greater_than",
                                    {TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    arrow::boolean());
  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysAnti",
      {n_left_key, n_right_key, greater_than_function}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {probeArrays_expr}, {table1_f0, table1_f1},
                                    &expr_probe, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]", "[2, 3, 5, 7, 9, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[12, 12, 13, 14, 15, 16]", "[12, 14, 13, 14, 15, 16]",
                       "[12, 13, 13, 14, 15, 16]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 1, 2, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[6, 8, 9, 10, 10, 13]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  auto res_sch = arrow::schema({f_res, f_res});
  std::vector<std::string> expected_result_string = {"[1, 4, 5, 6]",
                                                     "[1, 4, 5, 6]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 9, 10]", "[8, 9, 10]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int i = 0; i < right_batch->num_columns(); i++) {
      input.push_back(right_batch->column(i));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

// Test case for "exists" col locating in the middle of result_schema
TEST(TestArrowComputeMergeJoin, JoinTestUsingExistenceJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema",
                                    {TreeExprBuilder::MakeField(table0_f0),
                                     TreeExprBuilder::MakeField(table0_f1),
                                     TreeExprBuilder::MakeField(table0_f2)},
                                    uint32());
  auto n_right =
      TreeExprBuilder::MakeFunction("codegen_right_schema",
                                    {TreeExprBuilder::MakeField(table1_f0),
                                     TreeExprBuilder::MakeField(table1_f1)},
                                    uint32());
  auto f_res = field("res", uint32());
  auto f_exist = field("res", boolean());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedJoinArraysExistence", {n_left_key, n_right_key}, uint32());
  auto n_codegen_probe = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_probeArrays, n_left, n_right}, uint32());
  auto probeArrays_expr =
      TreeExprBuilder::MakeExpression(n_codegen_probe, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  arrow::compute::ExecContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {probeArrays_expr},
      {table1_f0, field("table1_exists", boolean()), table1_f1}, &expr_probe,
      true));

  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[1, 3, 3, 3, 4, 5, 7, 9]",
                                                "[1, 3, 3, 3, 4, 5, 7, 9]",
                                                "[1, 3, 3, 3, 4, 5, 7, 9]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[10, 12, 13, 14, 16, 20]", "[10, 12, 13, 14, 16, 20]",
                       "[10, 12, 13, 14, 16, 20]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 3, 4, 5, 6]", "[true, false, true, true, true, true, false]",
      "[1, 2, 3, 3, 4, 5, 6]"};
  auto res_sch = arrow::schema({f_res, f_exist, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 8, 9, 10, 11, 12]",
                            "[true, false, true, true, false, true]",
                            "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));
  probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  for (int i = 0; i < 2; i++) {
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch;
    std::vector<std::shared_ptr<arrow::Array>> input;
    for (int j = 0; j < right_batch->num_columns(); j++) {
      input.push_back(right_batch->column(j));
    }

    ASSERT_NOT_OK(probe_result_iterator->Process(input, &result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
