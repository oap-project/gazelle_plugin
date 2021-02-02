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

TEST(TestArrowComputeSort, SortTestInPlaceNullsFirstAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, NaN, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, NaN, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 15, 17, 18, 19, 20, 21, "
      "23, 30, 32, 33, 35, 37, 42, 43, 50, 52, 59, 64, NaN, NaN, NaN]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestInplaceNullsLastAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, NaN, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, NaN, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 15, 17, 18, 19, 20, 21, "
      "23, 30, 32, 33, 35, 37, 42, 43, 50, 52, 59, 64, NaN, NaN, NaN, null, null]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestInplaceNullsFirstDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {false_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, NaN, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, NaN, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, NaN, NaN, NaN, 64, 59, 52, 50, 43, 42, 37, 35, 33, 32, 30, 23, "
      "21, 20, 19, 18, 17, 15, 13, 12, 11, 10, 9, 8, 7, 6, 4, 3, 2, 1]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestInplaceNullsLastDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {false_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, 12, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, NaN, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, NaN, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[NaN, NaN, NaN, 64, 59, 52, 50, 43, 42, 37, 35, 33, 32, 30, 23, 21, 20, "
      "19, 18, 17, 15, 13, 12, 11, 10, 9, 8, 7, 6, 4, 3, 2, 1, null, null]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestInplaceAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, 45, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, NaN, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, 12, 22, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 18, 19, 20, 21, 22, 23, "
      "30, 32, 33, 35, 37, 41, 42, 43, 45, 50, 52, 59, 64, NaN, NaN]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestInplaceDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {false_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0});
  std::vector<std::shared_ptr<Field>> ret_types = {f0};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 43, 42, 6, 45, 2]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, NaN, 7, 9, 19, 33]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, 12, 22, 13, 8, 59, 21]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[NaN, NaN, 64, 59, 52, 50, 45, 43, 42, 41, 37, 35, 33, 32, 30, 23, "
      "22, 21, 20, 19, 18, 17, 14, 13, 12, 11, 10, 9, 8, 7, 6, 4, 3, 2, 1]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOnekeyNullsFirstAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, NaN, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 15, 17, 18, 19, 21, "
      "22, 23, 30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64, NaN, NaN, NaN]",
      "[34, 67, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 14, 16, 18, 19, 20, 22, "
      "23, 24, "
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65, 21, null, 13]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOnekeyNullsLastAsc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {TreeExprBuilder::MakeLiteral(false)}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, NaN, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 15, 17, 18, 19, 21, 22, 23, 30, "
      "32, 33, 35, 37, 41, 42, 43, 50, 52, 59, 64, NaN, NaN, NaN, null, null]",
      "[2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 14, 16, 18, 19, 20, 22, 23, 24,"
      "31, 33, 34, 36, 38, 42, 43, 44, 51, null, 60, 65, 21, null, 13, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOnekeyNullsFirstDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {TreeExprBuilder::MakeLiteral(false)}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, NaN, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null ,null , NaN, NaN, NaN, 64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 "
      ",23 ,22 ,21 ,19 ,18 ,17 ,15 ,13 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1]",
      "[34 ,67 ,13, null, 21, 65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 "
      ",23 ,22 , 20 ,19 ,18 ,16 ,14 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOnekeyNullsLastDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {TreeExprBuilder::MakeLiteral(false)}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {TreeExprBuilder::MakeLiteral(false)}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {TreeExprBuilder::MakeLiteral(true)}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[10, NaN, 4, 50, 52, 32, 11]",
                                                "[11, 13, 5, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, NaN, 43, 42, 6, null, 2]",
                                                  "[2, null, 44, 43, 7, 34, 3]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 15, 7, 9, 19, 33]",
                                                  "[4, 65, 16, 8, 10, 20, 34]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, NaN, 35, 30]",
                                                  "[24, 18, 42, 19, 21, 36, 31]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  "[38, 67, 23, 14, 9, 60, 22]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[NaN, NaN, NaN, 64 ,59 ,52 ,50 ,43 ,42 ,41 ,37 ,35 ,33 ,32 ,30 ,23 ,22 ,21 "
      ",19 ,18 ,17 ,15 ,13 , 11 ,10 ,9 ,8 ,7 ,6 ,4 ,3 ,2 ,1, null, null]",
      "[13, null, 21, 65 ,60 ,null ,51 ,44 ,43 ,42 ,38 ,36 ,34 ,33 ,31 ,24 ,23 ,22 "
      ", 20 ,19 ,18 ,16 ,14 ,12 ,11 ,10 ,9 ,8 ,7 ,5 ,4 ,3 ,2, 34, 67]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOnekeyBooleanDesc) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", boolean());
  auto f1 = field("f1", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {false_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;

  std::vector<std::string> input_data_string = {"[true, false, false, false, true, true, false]", 
                                                "[1, 2, 3, 4, 5, 6, 7]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[true, true, false, false, false, true, false]",
                                                  "[4, 2, 6, 0, 1, 4, 12]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[true, true, false, false, false, true, false]",
                                                  "[6, 12, 16, 10, 11, 41, 2]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[true, true, false, false, false, true, false]",
                                                  "[8, 22, 45, 12, 78, 12, 32]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[true, true, false, false, false, true, false]",
                                                  "[18, 5, 6, 78, 11, 2, 12]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, "
      "false, false, false, false, false, false, false, false, false, false, false, false, false, "
      "false, false, false, false, false, false, false]",
      "[18, 22, 8, 41, 1, 12, 12, 6, 4, 5, 2, 4, 6, 5, 2, 6, 10, 32, 78, 78, 11, 12, 12, 45, 2, "
      "11, 16, 12, 1, 0, 6, 7, 4, 3, 2]"};
  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));

  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOneKeyStr) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", utf8());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);

  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  std::vector<std::string> input_data_string = {
    R"(["b", "q", "s", "t", null, null, "a"])",
    R"(["a", "c", "e", "f", "g", null, "h"])"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_2 = {
    R"([null, "f", "q", "d", "r", null, "g"])",
    R"(["a", "c", "e", "f", null, "j", "h"])"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_3 = {
    R"(["p", "q", "o", "e", null, null, "l"])",
    R"(["a", "c", "e", "f", "g","j", null])"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_4 = {
    R"(["q", "w", "z", "x", "y", null, "u"])",
    R"(["a", "c", "e", "f", "g","j", "h"])"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_5 = {
    R"(["a", "c", "b", "d", null, null, null])",
    R"(["a", null, "e", "f", "g","j", "h"])"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a","a","b","b","c","d","d","e","f","g","l","o","p","q","q","q","q","r","s","t","u","w","x","y","z",null,null,null,null,null,null,null,null,null,null])",
      R"(["h","a","e","a",null,"f","f","f","c","h",null,"e","a","c","a","e","c",null,"e","f","h","c","f","g","e","g",null,"a","j","g","j","j","g","j","h"])"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);
  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;
  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestOneKeyWithProjection) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", utf8());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);

  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_projection = TreeExprBuilder::MakeFunction(
      "upper", {arg_0}, utf8());
  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {n_projection}, uint32());    
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  std::vector<std::string> input_data_string = {
    R"(["B", "q", "s", "T", null, null, "a"])",
    R"(["a", "c", "e", "f", "g", null, "h"])"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_2 = {
    R"([null, "F", "Q", "d", "r", null, "g"])",
    R"(["a", "c", "e", "f", null, "j", "h"])"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_3 = {
    R"(["p", "q", "o", "E", null, null, "l"])",
    R"(["a", "c", "e", "f", "g","j", null])"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_4 = {
    R"(["q", "W", "Z", "x", "y", null, "u"])",
    R"(["a", "c", "e", "f", "g","j", "h"])"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  std::vector<std::string> input_data_string_5 = {
    R"(["a", "C", "b", "D", null, null, null])",
    R"(["a", null, "e", "f", "g","j", "h"])"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);
  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      R"(["a","a","b","B","C","D","d","E","F","g","l","o","p","q","q","Q","q","r","s","T","u","W","x","y","Z",null,null,null,null,null,null,null,null,null,null])",
      R"(["h","a","e","a",null,"f","f","f","c","h",null,"e","a","c","a","e","c",null,"e","f","h","c","f","g","e","g",null,"a","j","g","j","j","g","j","h"])"};
  MakeInputBatch(expected_result_string, sch, &expected_result);
  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  auto sort_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          sort_result_iterator_base);
  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;
  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestMultipleKeysNaN) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", float64());
  auto f1 = field("f1", utf8());
  auto f2 = field("f2", float64());
  auto f3 = field("f3", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto f_res = field("res", uint32());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {arg_0, arg_1, arg_2}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0, arg_1, arg_2}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal, false_literal, true_literal}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal, true_literal, true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1, f2, f3});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1, f2, f3};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[8, NaN, 4, 50, 52, 32, 11]",
                                                R"([null, "a", "a", "b", "b","b", "b"])",
                                                "[11, NaN, 5, 51, null, 33, 12]",
                                                "[1, 3, 5, 10, null, 13, 2]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, NaN, 42, 6, null, 2]",
                                                  R"(["a", "a", null, "b", "b", "a", "b"])",
                                                  "[2, null, 44, 43, 7, 34, 3]",
                                                  "[9, 7, 5, 1, 5, null, 17]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 8, 7, 9, 8, NaN]",
                                                  R"(["a", "a", "b", "b", "b","b", "b"])",
                                                  "[4, 65, 16, 8, 10, 20, 34]",
                                                  "[8, 6, 2, 3, 10, 12, 15]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[24, 18, 42, NaN, 21, 36, 31]",
                                                  "[15, 16, 2, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  R"(["a", "b", "a", "b", "b","b", "b"])",
                                                  "[38, 67, 23, 14, null, 60, 22]",
                                                  "[16, 17, 5, 15, 9, null, 19]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 6, 7, 8, 8, 8, 8, 9, 11, 13, 14, 17, 18, 20, 21, "
      "22, 23, 30, 32, 35, 37, 41, 42, 50, 52, 59, 64, NaN, NaN, NaN, null, null]",
      R"(["a","b","a","a","b","b", null,"b","b","b","b","b","b","a","a","b","b","b","a","a","b","b","b","a","a","b","b","b","b","a",null,"b","a","b","a"])",
      "[2, 3, 4, 5, 7, 8, 11, null, 16, 20, 10, 12, 14, null, 18, NaN, 21, 22, 23, "
      "24, 31, 33, 36, 38, 42, 43, 51, null, 60, 65, 44, 34, NaN, 67, 34]",
      "[9, 17, 8, 5, 5, 3, 1, 9, 2, 12, 10, 2, 15, 7, 16, 51, null, 19, 5, "
      "15, 12, 13, 33, 16, 2, 1, 10, null, null, 6, 5, 15, 3, 17, null]"};

  MakeInputBatch(expected_result_string, sch, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}

TEST(TestArrowComputeSort, SortTestMultipleKeysWithProjection) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", utf8());
  auto f2 = field("f2", uint32());
  auto f3 = field("f3", uint32());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto arg_2 = TreeExprBuilder::MakeField(f2);
  auto true_literal = TreeExprBuilder::MakeLiteral(true);
  auto false_literal = TreeExprBuilder::MakeLiteral(false);
  auto f_res = field("res", uint32());
  auto f_bool = field("res", arrow::boolean());
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  auto f_indices = field("indices", indices_type);

  auto uint32_node = TreeExprBuilder::MakeLiteral((uint32_t)0);
  auto str_node = TreeExprBuilder::MakeStringLiteral("");

  auto isnotnull_0 = TreeExprBuilder::MakeFunction(
      "isnotnull", {TreeExprBuilder::MakeField(f0)}, arrow::boolean());
  auto coalesce_0 = TreeExprBuilder::MakeIf(
      isnotnull_0, TreeExprBuilder::MakeField(f0), uint32_node, uint32());
  auto isnull_0 = TreeExprBuilder::MakeFunction(
      "isnull", {arg_0}, arrow::boolean());

  auto isnotnull_1 = TreeExprBuilder::MakeFunction(
      "isnotnull", {TreeExprBuilder::MakeField(f1)}, arrow::boolean());
  auto coalesce_1 = TreeExprBuilder::MakeIf(
      isnotnull_1, TreeExprBuilder::MakeField(f1), str_node, utf8());
  auto isnull_1 = TreeExprBuilder::MakeFunction(
      "isnull", {arg_1}, arrow::boolean());
  
  auto isnotnull_2 = TreeExprBuilder::MakeFunction(
      "isnotnull", {TreeExprBuilder::MakeField(f2)}, arrow::boolean());
  auto coalesce_2 = TreeExprBuilder::MakeIf(
      isnotnull_2, TreeExprBuilder::MakeField(f2), uint32_node, uint32());
  auto isnull_2 = TreeExprBuilder::MakeFunction(
      "isnull", {arg_2}, arrow::boolean());

  auto n_key_func = TreeExprBuilder::MakeFunction(
      "key_function", {coalesce_0, isnull_0, coalesce_1, isnull_1, coalesce_2, isnull_2}, uint32());
  auto n_key_field = TreeExprBuilder::MakeFunction(
      "key_field", {arg_0, arg_0, arg_1, arg_1, arg_2, arg_2}, uint32());
  auto n_dir = TreeExprBuilder::MakeFunction(
      "sort_directions", {true_literal, true_literal, false_literal, false_literal, 
                          true_literal, true_literal,}, uint32());
  auto n_nulls_order = TreeExprBuilder::MakeFunction(
      "sort_nulls_order", {false_literal, false_literal, true_literal, true_literal, 
                           true_literal, true_literal}, uint32());
  auto NaN_check = TreeExprBuilder::MakeFunction(
      "NaN_check", {true_literal}, uint32());
  auto n_sort_to_indices = TreeExprBuilder::MakeFunction(
      "sortArraysToIndices", {n_key_func, n_key_field, n_dir, n_nulls_order, NaN_check}, uint32());
  auto n_sort = TreeExprBuilder::MakeFunction(
      "standalone", {n_sort_to_indices}, uint32());
  auto sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort, f_res);

  auto sch = arrow::schema({f0, f1, f2, f3});
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1, f2, f3};
  auto ret_schema = arrow::schema(ret_types);
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> sort_expr;
  arrow::compute::FunctionContext ctx;
  ASSERT_NOT_OK(CreateCodeGenerator(ctx.memory_pool(), sch, {sortArrays_expr}, ret_types, &sort_expr, true));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;

  std::vector<std::string> input_data_string = {"[8, 8, 4, 50, 52, 32, 11]",
                                                R"([null, "b", "a", "b", "b","b", "b"])",
                                                "[11, 10, 5, 51, null, 33, 12]",
                                                "[1, 3, 5, 10, null, 13, 2]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_2 = {"[1, 14, 8, 42, 6, null, 2]",
                                                  R"(["a", "a", null, "b", "b","b", "b"])",
                                                  "[2, null, 44, 43, 7, 34, 3]",
                                                  "[9, 7, 5, 1, 5, null, 17]"};
  MakeInputBatch(input_data_string_2, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_3 = {"[3, 64, 8, 7, 9, 8, 33]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[4, 65, 16, 8, 10, 20, 34]",
                                                  "[8, 6, 2, 3, 10, 12, 15]"};
  MakeInputBatch(input_data_string_3, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_4 = {"[23, 17, 41, 18, 20, 35, 30]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[24, 18, 42, 19, 21, 36, 31]",
                                                  "[15, 16, 2, 51, null, 33, 12]"};
  MakeInputBatch(input_data_string_4, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  std::vector<std::string> input_data_string_5 = {"[37, null, 22, 13, 8, 59, 21]",
                                                  R"(["a", "a", "a", "b", "b","b", "b"])",
                                                  "[38, 67, 23, 14, null, 60, 22]",
                                                  "[16, 17, 5, 15, 9, null, 19]"};
  MakeInputBatch(input_data_string_5, sch, &input_batch);
  input_batch_list.push_back(input_batch);

  ////////////////////////////////// calculation ///////////////////////////////////
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[null, null, 1, 2, 3, 4, 6, 7, 8, 8, 8, 8, 8, 8, 9, 11, 13, 14, 17, 18, 20, 21, "
      "22, 23, 30, 32, 33, 35, 37, 41, 42, 50, 52, 59, 64]",
      R"(["b","a","a","b","a","a","b","b","b","b","b","a", null, null,"b","b","b","a","a","b","b","b","a","a","b","b","b","b","a","a","b","b","b","b","a"])",
      "[34, 67, 2, 3, 4, 5, 7, 8, null, 10, 20, 16, 11, 44, 10, 12, 14, null, 18, 19, 21, 22, 23, "
      "24, 31, 33, 34, 36, 38, 42, 43, 51, null, 60, 65]",
      "[null, 17, 9, 17, 8, 5, 5, 3, 9, 3, 12, 2, 1, 5, 10, 2, 15, 7, 16, 51, null, 19, 5, "
      "15, 12, 13, 15, 33, 16, 2, 1, 10, null, null, 6]"};

  MakeInputBatch(expected_result_string, ret_schema, &expected_result);

  for (auto batch : input_batch_list) {
    ASSERT_NOT_OK(sort_expr->evaluate(batch, &dummy_result_batches));
  }
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;
  std::shared_ptr<ResultIteratorBase> sort_result_iterator_base;
  ASSERT_NOT_OK(sort_expr->finish(&sort_result_iterator_base));
  sort_result_iterator = std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
      sort_result_iterator_base);

  std::shared_ptr<arrow::RecordBatch> dummy_result_batch;
  std::shared_ptr<arrow::RecordBatch> result_batch;

  if (sort_result_iterator->HasNext()) {
    ASSERT_NOT_OK(sort_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*expected_result.get(), *result_batch.get()));
  }
}


}  // namespace codegen
}  // namespace sparkcolumnarplugin
