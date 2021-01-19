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
#include <gandiva/tree_expr_builder.h>
#include <gtest/gtest.h>

#include <memory>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

using arrow::boolean;
using arrow::int64;
using arrow::uint32;
using arrow::uint64;
using gandiva::TreeExprBuilder;

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowComputeWSCG, WSCGTestSingleInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_add = TreeExprBuilder::MakeFunction(
      "add",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      uint64());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than", {n_add, TreeExprBuilder::MakeField(table0_f2)}, boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child_probe = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  ////////////////////////////////////////////////////////
  auto n_project_input = TreeExprBuilder::MakeFunction(
      "codegen_input_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_project_func = TreeExprBuilder::MakeFunction(
      "codegen_project",
      {TreeExprBuilder::MakeField(table1_f1), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_project = TreeExprBuilder::MakeFunction(
      "project", {n_project_input, n_project_func}, uint32());
  //////////////////////////////////////////////////////////////////////////
  auto n_child =
      TreeExprBuilder::MakeFunction("child", {n_project, n_child_probe}, uint32());
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f1, table0_f2}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 1, 2, 3, 3, 5, 6, 6]",
                                                     "[1, 11, 2, 3, 13, 5, 6, 16]"};
  auto res_sch = arrow::schema({table1_f1, table0_f2});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 10, 12]", "[8, 10, 110, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestProjectKeyInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint64());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_right_project_key = TreeExprBuilder::MakeFunction(
      "castBIGINT", {TreeExprBuilder::MakeField(table1_f0)}, uint64());
  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {n_right_project_key}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_add = TreeExprBuilder::MakeFunction(
      "add",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      uint64());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than", {n_add, TreeExprBuilder::MakeField(table0_f2)}, boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child_probe = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  ////////////////////////////////////////////////////////
  auto n_project_input = TreeExprBuilder::MakeFunction(
      "codegen_input_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_project_func = TreeExprBuilder::MakeFunction(
      "codegen_project",
      {TreeExprBuilder::MakeField(table1_f1), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_project = TreeExprBuilder::MakeFunction(
      "project", {n_project_input, n_project_func}, uint32());
  //////////////////////////////////////////////////////////////////////////
  auto n_child =
      TreeExprBuilder::MakeFunction("child", {n_project, n_child_probe}, uint32());
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f1, table0_f2}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 1, 2, 3, 3, 5, 6, 6]",
                                                     "[1, 11, 2, 3, 13, 5, 6, 16]"};
  auto res_sch = arrow::schema({table1_f1, table0_f2});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 10, 12]", "[8, 10, 110, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestProjectFilterKeyInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint64());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_right_project_key = TreeExprBuilder::MakeFunction(
      "castBIGINT", {TreeExprBuilder::MakeField(table1_f0)}, uint64());
  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {n_right_project_key}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_add = TreeExprBuilder::MakeFunction(
      "add",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      uint64());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than", {n_add, TreeExprBuilder::MakeField(table0_f2)}, boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child_probe = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  ////////////////////////////////////////////////////////
  auto n_project_input = TreeExprBuilder::MakeFunction(
      "codegen_input_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_project_func = TreeExprBuilder::MakeFunction(
      "codegen_project",
      {TreeExprBuilder::MakeField(table1_f1), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_project = TreeExprBuilder::MakeFunction(
      "project", {n_project_input, n_project_func}, uint32());
  auto n_child_project =
      TreeExprBuilder::MakeFunction("child", {n_project, n_child_probe}, uint32());
  //////////////////////////////////////////////////////////////////////////
  auto n_filter_input = TreeExprBuilder::MakeFunction(
      "codegen_input_schema",
      {TreeExprBuilder::MakeField(table1_f1), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_filter_func = TreeExprBuilder::MakeFunction(
      "greater_than_or_equal_to",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeLiteral((uint32_t)10)},
      uint32());
  auto n_filter =
      TreeExprBuilder::MakeFunction("filter", {n_filter_input, n_filter_func}, uint32());
  //////////////////////////////////////////////////////////////////////////
  auto n_child =
      TreeExprBuilder::MakeFunction("child", {n_filter, n_child_project}, uint32());
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f1, table0_f2}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 3, 6]", "[11, 13, 16]"};
  auto res_sch = arrow::schema({table1_f1, table0_f2});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[10, 10, 12]", "[10, 110, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestStringInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key_func = TreeExprBuilder::MakeFunction(
      "upper", {TreeExprBuilder::MakeField(table1_f0)}, utf8());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {n_right_key_func}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_1, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"(["BJ", "SH", "HZ", "BH", "NY", "SH"])",
                                                R"(["A", "A", "C", "D", "C", "D"])",
                                                "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TK", "SH", "PH", "NJ", "NB", "SZ"])",
                       R"(["F", "F", "A", "B", "D", "C"])", "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["sh", "sz", "bj", null, "ny", "hz"])", "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["ph", null, "jh", "kk", "nj", "sz"])",
                         "[7, 8, 9, 10, null, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["SH", "SH", "SH", "SZ", "BJ", "NY", "HZ"])",
      R"(["A", "D", "F", "C", "A", "C", "C"])", "[3, 11, 12, 110, 10, 13, 1]",
      R"(["sh", "sh", "sh", "sz", "bj", "ny", "hz"])", "[1, 1, 1, 2, 3, 5, 6]"};
  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {R"(["PH", "NJ", "SZ"])", R"(["A", "B", "C"])", "[5, 8, 110]",
                            R"(["ph", "nj", "sz"])", "[7, null, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestTwoStringInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_1, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      R"(["l", "c", null, "b"])", R"(["L", "C", "A", "B"])", "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["f", "n", "e", "j"])", R"(["F", "N", "E", "J"])",
                       "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {R"([null, "b", "c", "d", "e", "f"])",
                                                  R"(["A", "B", "C", "D", "F", "F"])"};
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
      R"(["b", "c", "f"])", R"(["B", "C", "F"])", "[2, 3, 6]", R"(["b", "c", "f"])",
      R"(["B", "C", "F"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {R"(["j", "l", "n"])", R"(["J", "L", "N"])", "[8, 10, 12]",
                            R"(["j", "l", "n"])", R"(["J", "L", "N"])"};
  MakeInputBatch(expected_result_string, schema_table, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestOuterJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysOuter",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_1, {probeArrays_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 1, 2, 3, 3, null, 5, 6, 6]", "[1, 11, 2, 3, 13, null, 5, 6, 16]",
      "[1, 11, 2, 3, 13, null, 5, 6, 16]", "[1, 1, 2, 3, 3, 4, 5, 6, 6]",
      "[1, 1, 2, 3, 3, 4, 5, 6, 6]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[null, 8, null, 10, 10, null, 12]",
                            "[null, 8, null, 10, 110, null, 12]",
                            "[null, 8, null, 10, 110, null, 12]",
                            "[7, 8, 9, 10, 10, 11, 12]", "[7, 8, 9, 10, 10, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestAntiJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysAnti",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[4]", "[4]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 9, 11]", "[7, 9, 11]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestAntiJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysAnti",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[2, 4, 5]", "[2, 4, 5]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 8, 9, 11, 12]", "[7, 8, 9, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestSemiJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysSemi",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table = arrow::schema({table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 3, 5, 6]",
                                                     R"(["BJ", "TY", "SH", "HZ"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 12]", R"(["NY", "IT", "TL"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestSemiJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table0_f2)},
      arrow::boolean());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysSemi",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config, n_condition},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table = arrow::schema({table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 17, 2, 13, 11]", "[6, 12, 5, 8, 12, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 3, 5, 6]",
                                                     R"(["BJ", "TY", "SH", "HZ"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[8, 10, 12]", R"(["NY", "IT", "TL"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestExistenceJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto f_res = field("res", uint32());
  auto f_exist = field("res", arrow::boolean());
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(f_exist),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysExistence",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, f_exist, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, f_exist, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6]", "[true, true, true, false, true, true]",
      "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[7, 8, 9, 10, 11, 12]",
                            "[false, true, false, true, false, true]",
                            "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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

TEST(TestArrowComputeWSCG, WSCGTestSemiJoinWithCoalesce) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto func_node_0 = TreeExprBuilder::MakeFunction(
      "isnotnull", {TreeExprBuilder::MakeField(table0_f0)}, arrow::boolean());
  auto func_node_1 = TreeExprBuilder::MakeStringLiteral("");
  auto func_node_2 = TreeExprBuilder::MakeIf(
      func_node_0, TreeExprBuilder::MakeField(table0_f0), func_node_1, utf8());
  auto func_node_5 = TreeExprBuilder::MakeFunction(
      "isnull", {TreeExprBuilder::MakeField(table0_f0)}, arrow::boolean());

  auto func_node_3 = TreeExprBuilder::MakeFunction(
      "isnotnull", {TreeExprBuilder::MakeField(table1_f0)}, arrow::boolean());
  auto func_node_4 = TreeExprBuilder::MakeIf(
      func_node_3, TreeExprBuilder::MakeField(table1_f0), func_node_1, utf8());
  auto func_node_6 = TreeExprBuilder::MakeFunction(
      "isnull", {TreeExprBuilder::MakeField(table1_f0)}, arrow::boolean());

  auto n_left_key = TreeExprBuilder::MakeFunction("codegen_left_key_schema",
                                                  {func_node_2, func_node_5}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {func_node_4, func_node_6}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedProbeArraysSemi",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_hash_config}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table = arrow::schema({table1_f0, table1_f1});

  auto n_hash_kernel = TreeExprBuilder::MakeFunction(
      "HashRelation", {n_left_key, n_hash_config}, uint32());
  auto n_hash = TreeExprBuilder::MakeFunction("standalone", {n_hash_kernel}, uint32());
  auto hashRelation_expr = TreeExprBuilder::MakeExpression(n_hash, f_res);
  std::shared_ptr<CodeGenerator> expr_build;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_0,
                                    {hashRelation_expr}, {}, &expr_build, true));
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1, {probeArrays_expr},
                                    {table1_f0, table1_f1}, &expr_probe, true));
  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"(["l", "c", null, "b"])",
                                                "[10, 3, 1, 2]", "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["f", "n", "e", "j"])", "[6, 12, 5, 8]", "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {R"([null, "b", "c", "d", "e"])",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["i", "j", "k", "l", "m", "n"])",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {R"([null, "b", "c", "e"])",
                                                     R"(["BJ", "TY", "NY", "HZ"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {R"(["j", "l", "n"])", R"(["NY", "IT", "TL"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_build->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_build->finish(&build_result_iterator));
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator_base));

  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies({build_result_iterator});

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
/*
TEST(TestArrowComputeWSCG, WSCGTestStringInnerMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key_func = TreeExprBuilder::MakeFunction(
      "upper", {TreeExprBuilder::MakeField(table1_f0)}, utf8());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {n_right_key_func}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      boolean());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinInner",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_condition}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), arrow::schema({}), {mergeJoin_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"(["BJ", "SH", "HZ", "BH", "NY", "SH"])",
                                                R"(["A", "A", "C", "D", "C", "D"])",
                                                "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TK", "SH", "PH", "NJ", "NB", "SZ"])",
                       R"(["F", "F", "A", "B", "D", "C"])", "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["sh", "sz", "bj", null, "ny", "hz"])", "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["ph", null, "jh", "kk", "nj", "sz"])",
                         "[7, 8, 9, 10, 5, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["BJ", "NJ", "NY", "SH", "SH", "SH", "SZ", "SZ"])",
      R"(["A", "B", "C", "A", "D", "F", "C", "C"])", "[10, 8, 13, 3, 11, 12, 110, 110]",
      R"(["bj", "nj", "ny", "sh", "sh", "sh", "sz", "sz"])", "[3, 5, 5, 1, 1, 1, 2, 12]"};
  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestInnerMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinInner", {n_left, n_right, n_left_key, n_right_key, n_result},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0});
  auto schema_table = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), arrow::schema({}), {mergeJoin_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0}, &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"([12, 27, 34, 10, 39, 27])",
                                                R"(["A", "A", "C", "D", "C", "D"])",
                                                "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"([31, 27, 24, 24, 16, 45])", R"(["F", "F", "A", "B", "D", "C"])",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {R"([27, 45, 12, null, 39, 34])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"([24, null, 18, 22, 24, 45])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"([12,24,24,24,24,27,27,27,34,39,45,45])",
      R"(["A","A","B","A","B","A","D","F","C","C","C","C"])",
      "[10,5,8,5,8,3,11,12,1,13,110,110]", R"([12,24,24,24,24,27,27,27,34,39,45,45])"};
  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestStringOuterMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key_func = TreeExprBuilder::MakeFunction(
      "upper", {TreeExprBuilder::MakeField(table1_f0)}, utf8());
  auto n_right_key = TreeExprBuilder::MakeFunction("codegen_right_key_schema",
                                                   {n_right_key_func}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f1)},
      boolean());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinOuter",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_condition}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), arrow::schema({}), {mergeJoin_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"(["BJ", "SH", "HZ", "BH", "NY", "SH"])",
                                                R"(["A", "A", "C", "D", "C", "D"])",
                                                "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TK", "SH", "PH", "NJ", "NB", "SZ"])",
                       R"(["F", "F", "A", "B", "D", "C"])", "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["sh", "sz", "bj", null, "ny", "hz"])", "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["ph", null, "jh", "kk", "nj", "sz"])",
                         "[7, 8, 9, 10, 5, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"([null, null, "BJ", null, null, null, "NJ", "NY", null, "SH", "SH", "SH", "SZ",
"SZ"])", R"([null, null, "A", null, null, null, "B", "C", null, "A", "D", "F", "C",
"C"])",
      "[null, null, 10, null, null, null, 8, 13, null, 3, 11, 12, 110, 110]",
      R"([null, null, "bj", "hz", "jh", "kk", "nj", "ny", "ph", "sh", "sh", "sh", "sz",
"sz"])",
      "[4, 8, 3, 6, 9, 10, 5, 5, 7, 1, 1, 1, 2, 12]"};
  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestAntiMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinAnti", {n_left, n_right, n_left_key, n_right_key, n_result},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1}, &expr_join,
                                    true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[4, 7, 9, 11]", "[4, 7, 9, 11]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestAntiMergeJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinAnti",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_condition}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1}, &expr_join,
                                    true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[2, 4, 5, 7, 8, 9, 11, 12]",
                                                     "[2, 4, 5, 7, 8, 9, 11, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestSemiMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinSemi", {n_left, n_right, n_left_key, n_right_key, n_result},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1}, &expr_join,
                                    true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 5, 6, 8, 10, 12]", R"(["BJ", "TY", "SH", "HZ", "NY", "IT", "TL"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestSemiMergeJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table0_f2)},
      arrow::boolean());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinSemi",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_condition}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1}, &expr_join,
                                    true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 17, 2, 9, 11]", "[6, 12, 5, 8, 12, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[10, 13, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 5, 6, 8, 10]",
                                                     R"(["BJ", "SH", "HZ", "NY", "IT"])"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestExistenceMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());
  auto f_exist = field("res", arrow::boolean());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1),
       TreeExprBuilder::MakeField(f_exist)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinExistence",
      {n_left, n_right, n_left_key, n_right_key, n_result}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1, f_exist},
                                    &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1, f_exist});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      R"(["BJ", "TY", "NY", "SH", "HZ", "SH", "NY", "BJ", "IT", "BR", "TL"])",
      "[true, true, false, true, true, false, true, false, true, false, true]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestExistenceMergeJoinWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());
  auto f_exist = field("res", arrow::boolean());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1),
       TreeExprBuilder::MakeField(f_exist)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table0_f2)},
      arrow::boolean());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinExistence",
      {n_left, n_right, n_left_key, n_right_key, n_result, n_condition}, uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1, f_exist},
                                    &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 17, 2, 9, 11]", "[6, 12, 5, 8, 12, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[10, 13, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1, f_exist});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      R"(["BJ", "TY", "NY", "SH", "HZ", "SH", "NY", "BJ", "IT", "BR", "TL"])",
      "[true, false, false, true, true, false, true, false, true, false, false]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestTwoKeysOuterMergeJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", uint32());

  ///////////////////////////////////////////
  auto n_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto n_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_right_key_func = TreeExprBuilder::MakeFunction(
      "upper", {TreeExprBuilder::MakeField(table1_f0)}, utf8());
  auto n_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema",
      {n_right_key_func, TreeExprBuilder::MakeField(table1_f1)}, uint32());
  auto n_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0),
       TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinOuter", {n_left, n_right, n_left_key, n_right_key, n_result},
      uint32());
  auto n_child = TreeExprBuilder::MakeFunction("child", {n_probeArrays}, uint32());
  //////////////////////////////////////////////////////////////////
  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table =
      arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), arrow::schema({}), {mergeJoin_expr},
      {table0_f0, table0_f1, table0_f2, table1_f0, table1_f1}, &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions",
                                             {true_literal, true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {true_literal, true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {false_literal, false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {R"(["BJ", "SH", "HZ", "BH", "NY", "SH"])",
                                                R"(["A", "A", "C", "D", "C", "D"])",
                                                "[10, 13, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TK", "SH", "PH", "NJ", "NB", "SZ"])",
                       R"(["F", "F", "A", "B", "D", "C"])", "[6, 11, 5, 8, 16, 12]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["sh", "sz", "bj", null, "ny", "hz"])", "[11, 2, 10, 4, 13, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["ph", null, "jh", "kk", "nj", "sz"])",
                         "[7, 8, 9, 10, 8, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"([null, null, "BJ", null, null, null, "NJ", "NY", null, "SH", "SH", null, "SZ"])",
      R"([null, null, "A", null, null, null, "B", "C", null, "D", "F", null, "C"])",
      "[null, null, 10, null, null, null, 8, 13, null, 11, 11, null, 12]",
      R"([null, null, "bj", "hz", "jh", "kk", "nj", "ny", "ph", "sh", "sh", "sz", "sz"])",
      "[4, 8, 10, 6, 9, 10, 8, 13, 7, 11, 11, 2, 12]"};
  auto res_sch = arrow::schema({table0_f0, table0_f1, table0_f2, table1_f0, table1_f1});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeWSCG, WSCGTestContinuousMergeJoinSemiExistence) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());
  auto table2_f0 = field("table2_f0", uint32());

  auto f_exist = field("res", arrow::boolean());

  ///////////////////////////////////////////
  auto f_res = field("res", uint32());
  auto n_semi_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_semi_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());

  auto n_semi_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_semi_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_semi_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_semi_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinSemi",
      {n_semi_left, n_semi_right, n_semi_left_key, n_semi_right_key, n_semi_result},
      uint32());
  auto n_semi_child =
      TreeExprBuilder::MakeFunction("child", {n_semi_probeArrays}, uint32());

  //////////////////////////////////////////////////////////////////
  auto n_existence_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_existence_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());

  auto n_existence_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_existence_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_existence_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1),
       TreeExprBuilder::MakeField(f_exist)},
      uint32());
  auto n_existence_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinExistence",
      {n_existence_left, n_existence_right, n_existence_left_key, n_existence_right_key,
       n_existence_result},
      uint32());
  auto n_existence_child = TreeExprBuilder::MakeFunction(
      "child", {n_existence_probeArrays, n_semi_child}, uint32());
  //////////////////////////////////////////////////////////////
  auto n_wscg =
      TreeExprBuilder::MakeFunction("wholestagecodegen", {n_existence_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table_2 = arrow::schema({table2_f0});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1, f_exist},
                                    &expr_join, true));
  /////////////// Sort Kernel ///////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));
  ////////////////////////////////////////////////
  auto n_key_func_left_2 = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_key_field_left_2 = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_sort_to_indices_left_2 =
      TreeExprBuilder::MakeFunction("sortArraysToIndices",
                                    {n_key_func_left_2, n_key_field_left_2, n_dir,
                                     n_nulls_order, NaN_check, result_type},
                                    uint32());
  auto n_sort_left_2 =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left_2}, uint32());
  auto sortArrays_expr_left_2 = TreeExprBuilder::MakeExpression(n_sort_left_2, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left_2;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_2,
                                    {sortArrays_expr_left_2}, {table2_f0},
                                    &expr_sort_left_2, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_2;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_1_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_1_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_1_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_1_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[3, 7, 1, 2, 8, 10]"};
  MakeInputBatch(input_data_2_string, schema_table_2, &input_batch);
  table_2.push_back(input_batch);

  input_data_2_string = {"[9, 5, 4]"};
  MakeInputBatch(input_data_2_string, schema_table_2, &input_batch);
  table_2.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1, f_exist});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 5, 6, 8, 10]", R"(["BJ", "TY", "SH", "HZ", "NY", "IT"])",
      "[true, true, true, false, true, true]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_2) {
    ASSERT_NOT_OK(expr_sort_left_2->evaluate(batch, &dummy_result_batches));
  }
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_sort_left->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_right->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);
  ASSERT_NOT_OK(expr_sort_left_2->finish(&build_result_iterator));
  dependency_iterator_list.push_back(build_result_iterator);

  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;

    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
}
*/
TEST(TestArrowComputeWSCG, WSCGTestContinuousMergeJoinSemiExistenceWithCondition) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", utf8());
  auto table2_f0 = field("table2_f0", uint32());
  auto table2_f1 = field("table2_f1", utf8());

  auto f_exist = field("res", arrow::boolean());

  ///////////////////////////////////////////
  auto f_res = field("res", uint32());
  auto n_semi_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_semi_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());

  auto n_semi_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_semi_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_semi_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_semi_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinSemi",
      {n_semi_left, n_semi_right, n_semi_left_key, n_semi_right_key, n_semi_result},
      uint32());
  auto n_semi_child =
      TreeExprBuilder::MakeFunction("child", {n_semi_probeArrays}, uint32());

  //////////////////////////////////////////////////////////////////
  auto n_existence_left = TreeExprBuilder::MakeFunction(
      "codegen_left_schema",
      {TreeExprBuilder::MakeField(table2_f0), TreeExprBuilder::MakeField(table2_f1)},
      uint32());
  auto n_existence_right = TreeExprBuilder::MakeFunction(
      "codegen_right_schema",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());

  auto n_existence_left_key = TreeExprBuilder::MakeFunction(
      "codegen_left_key_schema", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_existence_right_key = TreeExprBuilder::MakeFunction(
      "codegen_right_key_schema", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_existence_result = TreeExprBuilder::MakeFunction(
      "result",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1),
       TreeExprBuilder::MakeField(f_exist)},
      uint32());
  auto n_condition = TreeExprBuilder::MakeFunction(
      "not",
      {TreeExprBuilder::MakeFunction(
          "equal",
          {TreeExprBuilder::MakeField(table1_f1), TreeExprBuilder::MakeField(table2_f1)},
          arrow::boolean())},
      arrow::boolean());
  auto n_existence_probeArrays = TreeExprBuilder::MakeFunction(
      "conditionedMergeJoinExistence",
      {n_existence_left, n_existence_right, n_existence_left_key, n_existence_right_key,
       n_existence_result, n_condition},
      uint32());
  auto n_existence_child = TreeExprBuilder::MakeFunction(
      "child", {n_existence_probeArrays, n_semi_child}, uint32());
  //////////////////////////////////////////////////////////////
  auto n_wscg =
      TreeExprBuilder::MakeFunction("wholestagecodegen", {n_existence_child}, uint32());
  auto mergeJoin_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  auto schema_table_2 = arrow::schema({table2_f0, table2_f1});
  std::shared_ptr<CodeGenerator> expr_join;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), arrow::schema({}),
                                    {mergeJoin_expr}, {table1_f0, table1_f1, f_exist},
                                    &expr_join, true));
  /////////////// Sort Kernel //////////////////////////
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto n_dir = TreeExprBuilder::MakeFunction("sort_directions", {true_literal}, uint32());
  auto n_nulls_order =
      TreeExprBuilder::MakeFunction("sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction("NaN_check", {false_literal}, uint32());
  auto result_type = TreeExprBuilder::MakeFunction(
      "result_type", {TreeExprBuilder::MakeLiteral((int)0)}, uint32());
  auto n_key_func_left = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_key_field_left = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table0_f0)}, uint32());
  auto n_sort_to_indices_left = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_left, n_key_field_left, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_left =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left}, uint32());
  auto sortArrays_expr_left = TreeExprBuilder::MakeExpression(n_sort_left, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left;
  ASSERT_NOT_OK(
      CreateCodeGenerator(ctx.memory_pool(), schema_table_0, {sortArrays_expr_left},
                          {table0_f0, table0_f1, table0_f2}, &expr_sort_left, true));
  auto n_cached_relation_left = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("CachedRelation", {n_key_field_left}, uint32())},
      uint32());
  auto cached_relation_expr_left =
      TreeExprBuilder::MakeExpression(n_cached_relation_left, f_res);
  std::shared_ptr<CodeGenerator> expr_cached_relation_left;
  ASSERT_NOT_OK(CreateCodeGenerator(
      ctx.memory_pool(), schema_table_0, {cached_relation_expr_left},
      {table0_f0, table0_f1, table0_f2}, &expr_cached_relation_left, true));
  ////////////////////////////////////////////////
  auto n_key_func_right = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_key_field_right = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table1_f0)}, uint32());
  auto n_sort_to_indices_right = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices",
      {n_key_func_right, n_key_field_right, n_dir, n_nulls_order, NaN_check, result_type},
      uint32());
  auto n_sort_right =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_right}, uint32());
  auto sortArrays_expr_right = TreeExprBuilder::MakeExpression(n_sort_right, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {sortArrays_expr_right}, {table1_f0, table1_f1},
                                    &expr_sort_right, true));
  auto n_cached_relation_right = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("CachedRelation", {n_key_field_right}, uint32())},
      uint32());
  auto cached_relation_expr_right =
      TreeExprBuilder::MakeExpression(n_cached_relation_right, f_res);
  std::shared_ptr<CodeGenerator> expr_cached_relation_right;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_1,
                                    {cached_relation_expr_right}, {table1_f0, table1_f1},
                                    &expr_cached_relation_right, true));
  ////////////////////////////////////////////////
  auto n_key_func_left_2 = TreeExprBuilder::MakeFunction(
      "key_function", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_key_field_left_2 = TreeExprBuilder::MakeFunction(
      "key_field", {TreeExprBuilder::MakeField(table2_f0)}, uint32());
  auto n_sort_to_indices_left_2 =
      TreeExprBuilder::MakeFunction("sortArraysToIndices",
                                    {n_key_func_left_2, n_key_field_left_2, n_dir,
                                     n_nulls_order, NaN_check, result_type},
                                    uint32());
  auto n_sort_left_2 =
      TreeExprBuilder::MakeFunction("standalone", {n_sort_to_indices_left_2}, uint32());
  auto sortArrays_expr_left_2 = TreeExprBuilder::MakeExpression(n_sort_left_2, f_res);
  std::shared_ptr<CodeGenerator> expr_sort_left_2;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_2,
                                    {sortArrays_expr_left_2}, {table2_f0, table2_f1},
                                    &expr_sort_left_2, true));
  auto n_cached_relation_left_2 = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("CachedRelation", {n_key_field_left_2}, uint32())},
      uint32());
  auto cached_relation_expr_left_2 =
      TreeExprBuilder::MakeExpression(n_cached_relation_left_2, f_res);
  std::shared_ptr<CodeGenerator> expr_cached_relation_left_2;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), schema_table_2,
                                    {cached_relation_expr_left_2}, {table2_f0, table2_f1},
                                    &expr_cached_relation_left_2, true));

  ///////////////////// Calculation //////////////////
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_2;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_1_string = {"[1, 3, 4, 5, 6]",
                                                  R"(["BJ", "TY", "NY", "SH", "HZ"])"};
  MakeInputBatch(input_data_1_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_1_string = {"[7, 8, 9, 10, 11, 12]",
                         R"(["SH", "NY", "BJ", "IT", "BR", "TL"])"};
  MakeInputBatch(input_data_1_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      "[3, 7, 1, 2, 8, 10]", R"(["XM", "KY", "BJ", "IT", "NY", "JP"])"};
  MakeInputBatch(input_data_2_string, schema_table_2, &input_batch);
  table_2.push_back(input_batch);

  input_data_2_string = {"[9, 5, 4]", R"(["XM", "KY", "BJ"])"};
  MakeInputBatch(input_data_2_string, schema_table_2, &input_batch);
  table_2.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  auto res_sch = arrow::schema({table1_f0, table1_f1, f_exist});
  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 3, 5, 6, 8, 10, 12]", R"(["BJ", "TY", "SH", "HZ", "NY", "IT", "TL"])",
      "[false, true, true, false, false, true, false]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_sort_left->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_1) {
    ASSERT_NOT_OK(expr_sort_right->evaluate(batch, &dummy_result_batches));
  }
  for (auto batch : table_2) {
    ASSERT_NOT_OK(expr_sort_left_2->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIteratorBase> build_result_iterator;
  std::vector<std::shared_ptr<CodeGenerator>> expr_list = {
      expr_sort_left, expr_sort_right, expr_sort_left_2};
  std::vector<std::shared_ptr<CodeGenerator>> cache_list = {
      expr_cached_relation_left, expr_cached_relation_right, expr_cached_relation_left_2};
  std::vector<std::shared_ptr<ResultIteratorBase>> dependency_iterator_list;
  for (int i = 0; i < expr_list.size(); i++) {
    auto expr = expr_list[i];
    auto cache = cache_list[i];
    ASSERT_NOT_OK(expr->finish(&build_result_iterator));
    auto rb_iter = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
        build_result_iterator);
    while (rb_iter->HasNext()) {
      std::shared_ptr<arrow::RecordBatch> result_batch;
      ASSERT_NOT_OK(rb_iter->Next(&result_batch));
      ASSERT_NOT_OK(cache->evaluate(result_batch, &dummy_result_batches));
    }
    ASSERT_NOT_OK(cache->finish(&build_result_iterator));
    dependency_iterator_list.push_back(build_result_iterator);
  }

  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;
  ASSERT_NOT_OK(expr_join->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);
  probe_result_iterator->SetDependencies(dependency_iterator_list);

  int i = 0;
  while (probe_result_iterator->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;
    ASSERT_NOT_OK(probe_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i++]).get(), *result_batch.get()));
  }
  std::shared_ptr<Metrics> metrics;
  probe_result_iterator_base->GetMetrics(&metrics);
  for (int i = 0; i < metrics->num_metrics; i++) {
    std::cout << "CodeGen " << i << ": Process time is " << metrics->process_time[i]
              << ", output length is " << metrics->output_length[i] << std::endl;
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin