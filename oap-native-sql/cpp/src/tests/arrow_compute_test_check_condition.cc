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
#include <dlfcn.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <memory>

#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

arrow::Status ExecFunction(std::string signature,
                           std::vector<std::shared_ptr<arrow::RecordBatch>> table_0,
                           std::shared_ptr<arrow::RecordBatch> table_1,
                           std::vector<bool>* res) {
  std::string outpath = GetTempPath() + "/tmp";
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string libfile = outpath + prefix + signature + ".so";
  // load dynamic library
  void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
  if (!dynlib) {
    std::stringstream ss;
    ss << "LoadLibrary " << libfile
       << " failed. \nCur dir has contents "
          "as below."
       << std::endl;
    auto cmd = "ls -l " + GetTempPath() + ";";
    ss << exec(cmd.c_str()) << std::endl;
    return arrow::Status::Invalid(libfile,
                                  " is not generated, failed msg as below: ", ss.str());
  }

  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>, std::vector<bool>*);
  *(void**)(&Function) = dlsym(dynlib, "DoTest");
  const char* dlsym_error = dlerror();
  if (dlsym_error != NULL) {
    std::stringstream ss;
    ss << "error loading symbol:\n" << dlsym_error << std::endl;
    return arrow::Status::Invalid(ss.str());
  }

  Function(table_0, table_1, res);

  return arrow::Status::OK();
}

std::string ProduceCodes(std::string condition_check_str, std::string var_define,
                         std::string var_prepare) {
  std::stringstream ss;
  ss << "#include \"precompile/array.h\"" << std::endl;
  ss << "#include <arrow/record_batch.h>" << std::endl;
  ss << "#include <vector>" << std::endl;
  ss << "#include \"codegen/arrow_compute/ext/array_item_index.h\"" << std::endl;
  ss << "using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;" << std::endl;
  ss << "using namespace sparkcolumnarplugin::precompile;" << std::endl;
  ss << "class TESTCONDITION {" << std::endl;
  ss << " public:" << std::endl;
  ss << "  TESTCONDITION(std::vector<std::shared_ptr<arrow::RecordBatch>> table_0,"
     << std::endl;
  ss << "                std::shared_ptr<arrow::RecordBatch> table_1) {" << std::endl;
  ss << var_prepare << std::endl;
  ss << "}" << std::endl;
  ss << condition_check_str << std::endl;
  ss << var_define << std::endl;
  ss << "};" << std::endl;
  ss << "extern \"C\" void DoTest(" << std::endl
     << "    std::vector<std::shared_ptr<arrow::RecordBatch>> table_0,"
     << "    std::shared_ptr<arrow::RecordBatch> table_1," << std::endl;
  ss << "    std::vector<bool>* res) {" << std::endl;
  ss << "  int x = 0;" << std::endl;
  ss << "  int y = 0;" << std::endl;
  ss << "  auto test = TESTCONDITION(table_0, table_1);" << std::endl;
  ss << "  for(int i = 0; i < table_1->num_rows(); i++) { " << std::endl;
  ss << "    (*res).push_back(test.ConditionCheck({x, y}, i));" << std::endl;
  ss << "    if (++y >= table_0[x]->num_rows()) {" << std::endl;
  ss << "      x++;" << std::endl;
  ss << "      y = 0;" << std::endl;
  ss << "    }" << std::endl;
  ss << "  }" << std::endl;
  ss << "}" << std::endl;
  return ss.str();
}

void ProduceVars(std::shared_ptr<arrow::Schema> schema_table_0,
                 std::shared_ptr<arrow::Schema> schema_table_1, std::string* prepare_str,
                 std::string* define_str) {
  std::stringstream prepare_ss;
  std::stringstream define_ss;
  prepare_ss << "for (int i = 0; i < table_0.size(); i++) {" << std::endl;
  for (int i = 0; i < schema_table_0->num_fields(); i++) {
    auto array_type = GetTypeString(schema_table_0->field(i)->type(), "Array");
    prepare_ss << "cached_0_" << i << "_.push_back(std::make_shared<" << array_type
               << ">(table_0[i]->column(" << i << ")));" << std::endl;
    define_ss << "std::vector<std::shared_ptr<" << array_type << ">> cached_0_" << i
              << "_;" << std::endl;
  }
  prepare_ss << "}" << std::endl;
  for (int i = 0; i < schema_table_1->num_fields(); i++) {
    auto array_type = GetTypeString(schema_table_1->field(i)->type(), "Array");
    prepare_ss << "cached_1_" << i << "_ = std::make_shared<" << array_type
               << ">(table_1->column(" << i << "));" << std::endl;
    define_ss << "std::shared_ptr<"
              << GetTypeString(schema_table_1->field(i)->type(), "Array") << "> cached_1_"
              << i << "_;" << std::endl;
  }
  *prepare_str = prepare_ss.str();
  *define_str = define_ss.str();
}

void ASSERT_NOT_EQUAL(std::vector<bool> expected_res, std::vector<bool> res) {
  bool check = true;
  for (int i = 0; i < res.size(); i++) {
    if (res[i] != expected_res[i]) {
      check = false;
      break;
    }
  }
  if (!check) {
    std::stringstream err_ss;
    err_ss << "Incorrect result, check below" << std::endl;
    for (int i = 0; i < res.size(); i++) {
      err_ss << i << ": expect is " << (expected_res[i] ? "true" : "false")
             << ", actual is " << (res[i] ? "true" : "false") << std::endl;
    }
    throw std::runtime_error(err_ss.str());
  }
}

TEST(TestArrowComputeCondition, check0) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto func_node = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::shared_ptr<arrow::RecordBatch> table_1;
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &table_1);

  //////////////////////// data prepared /////////////////////////

  std::vector<int> left_out_index_list;
  std::vector<int> right_out_index_list;
  std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
  int func_count = 0;
  std::vector<std::string> input_list;
  MakeCodeGenNodeVisitor(func_node, {schema_table_0->fields(), schema_table_1->fields()},
                         &func_count, &input_list, &left_out_index_list,
                         &right_out_index_list, &func_node_visitor);

  auto func_str = R"(
inline bool ConditionCheck(ArrayItemIndex x, int y) {
)" + func_node_visitor->GetPrepare() +
                  R"(
return )" + func_node_visitor->GetResult() +
                  R"(;
})";

  std::string prepare_str;
  std::string define_str;
  ProduceVars(schema_table_0, schema_table_1, &prepare_str, &define_str);
  auto codes = ProduceCodes(func_str, define_str, prepare_str);
  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>);
  ASSERT_NOT_OK(CompileCodes(codes, "condition_check_0"));
  std::vector<bool> res;
  ASSERT_NOT_OK(ExecFunction("condition_check_0", table_0, table_1, &res));
  std::vector<bool> expected_res = {true,  true, false, false, true, true,
                                    false, true, false, false, true, true};
  ASSERT_NOT_EQUAL(expected_res, res);
}

TEST(TestArrowComputeCondition, check1) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto func_node_0 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto func_node_1 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table1_f0)},
      arrow::boolean());
  auto func_node_2 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0)},
      arrow::boolean());
  auto func_node = TreeExprBuilder::MakeAnd(
      {TreeExprBuilder::MakeOr({func_node_0, func_node_1}), func_node_2});
  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::shared_ptr<arrow::RecordBatch> table_1;
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[null, 3, 8, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      "[1, 2, 3, null, 5, 6, 7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &table_1);

  //////////////////////// data prepared /////////////////////////

  std::vector<int> left_out_index_list;
  std::vector<int> right_out_index_list;
  std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
  int func_count = 0;
  std::vector<std::string> input_list;
  MakeCodeGenNodeVisitor(func_node, {schema_table_0->fields(), schema_table_1->fields()},
                         &func_count, &input_list, &left_out_index_list,
                         &right_out_index_list, &func_node_visitor);

  auto func_str = R"(
inline bool ConditionCheck(ArrayItemIndex x, int y) {
)" + func_node_visitor->GetPrepare() +
                  R"(
return )" + func_node_visitor->GetResult() +
                  R"(;
})";

  std::string prepare_str;
  std::string define_str;
  ProduceVars(schema_table_0, schema_table_1, &prepare_str, &define_str);
  auto codes = ProduceCodes(func_str, define_str, prepare_str);
  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>);
  ASSERT_NOT_OK(CompileCodes(codes, "condition_check_1"));
  std::vector<bool> res;
  ASSERT_NOT_OK(ExecFunction("condition_check_1", table_0, table_1, &res));
  std::vector<bool> expected_res = {true,  true, false, false, true, true,
                                    false, true, false, false, true, true};
  ASSERT_NOT_EQUAL(expected_res, res);
}

TEST(TestArrowComputeCondition, check2) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", utf8());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto func_node_0 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto func_node_1 = TreeExprBuilder::MakeInExpressionString(
      TreeExprBuilder::MakeField(table0_f0), {"BJ", "SH", "WH", "HZ"});
  auto func_node_2 = TreeExprBuilder::MakeFunction(
      "equal",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeStringLiteral("F")},
      arrow::boolean());
  auto func_node = TreeExprBuilder::MakeAnd(
      {TreeExprBuilder::MakeAnd({func_node_0, func_node_1}), func_node_2});
  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::shared_ptr<arrow::RecordBatch> table_1;
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::string> input_data_string = {R"(["BJ", "SH", "SZ", "HZ", "NB", "AU"])",
                                                "[null, 3, 8, 2, 13, 11]",
                                                R"(["F", "F", "M", "F", "F", "M"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TY", "LA", "SZ", "HZ", "BJ", "HZ"])",
                       "[6, 12, 5, 8, 16, 110]", R"(["F", "F", "M", "F", "F", "F"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      "[1, 2, 3, null, 5, 6, 7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &table_1);

  //////////////////////// data prepared /////////////////////////

  std::vector<int> left_out_index_list;
  std::vector<int> right_out_index_list;
  std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
  int func_count = 0;
  std::vector<std::string> input_list;
  MakeCodeGenNodeVisitor(func_node, {schema_table_0->fields(), schema_table_1->fields()},
                         &func_count, &input_list, &left_out_index_list,
                         &right_out_index_list, &func_node_visitor);

  auto func_str = R"(
inline bool ConditionCheck(ArrayItemIndex x, int y) {
)" + func_node_visitor->GetPrepare() +
                  R"(
return )" + func_node_visitor->GetResult() +
                  R"(;
})";

  std::string prepare_str;
  std::string define_str;
  ProduceVars(schema_table_0, schema_table_1, &prepare_str, &define_str);
  auto codes = ProduceCodes(func_str, define_str, prepare_str);
  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>);
  ASSERT_NOT_OK(CompileCodes(codes, "condition_check_2"));
  std::vector<bool> res;
  ASSERT_NOT_OK(ExecFunction("condition_check_2", table_0, table_1, &res));
  std::vector<bool> expected_res = {false, true,  false, false, false, false,
                                    false, false, false, false, true,  true};
  ASSERT_NOT_EQUAL(expected_res, res);
}

TEST(TestArrowComputeCondition, check3) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", utf8());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto func_node_0 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto func_node_1 = TreeExprBuilder::MakeInExpressionString(
      TreeExprBuilder::MakeField(table0_f0), {"BJ", "SH", "WH", "HZ"});
  auto func_node_2 = TreeExprBuilder::MakeFunction(
      "substr",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeLiteral((long)0),
       TreeExprBuilder::MakeLiteral((long)1)},
      utf8());
  auto func_node_3 = TreeExprBuilder::MakeFunction(
      "equal", {func_node_2, TreeExprBuilder::MakeStringLiteral("F")}, arrow::boolean());
  auto func_node = TreeExprBuilder::MakeAnd(
      {TreeExprBuilder::MakeAnd({func_node_0, func_node_1}), func_node_3});
  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::shared_ptr<arrow::RecordBatch> table_1;
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::string> input_data_string = {
      R"(["BJ", "SH", "SZ", "HZ", "NB", "AU"])", "[null, 3, 8, 2, 13, 11]",
      R"(["Female", "Female", "Male", "Female", "Female", "Male"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TY", "LA", "SZ", "HZ", "BJ", "HZ"])",
                       "[6, 12, 5, 8, 16, 110]",
                       R"(["Female", "Female", "Male", "Female", "Female", "Female"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
      "[1, 2, 3, null, 5, 6, 7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &table_1);

  //////////////////////// data prepared /////////////////////////

  std::vector<int> left_out_index_list;
  std::vector<int> right_out_index_list;
  std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
  int func_count = 0;
  std::vector<std::string> input_list;
  MakeCodeGenNodeVisitor(func_node, {schema_table_0->fields(), schema_table_1->fields()},
                         &func_count, &input_list, &left_out_index_list,
                         &right_out_index_list, &func_node_visitor);

  auto func_str = R"(
inline bool ConditionCheck(ArrayItemIndex x, int y) {
)" + func_node_visitor->GetPrepare() +
                  R"(
return )" + func_node_visitor->GetResult() +
                  R"(;
})";

  std::string prepare_str;
  std::string define_str;
  ProduceVars(schema_table_0, schema_table_1, &prepare_str, &define_str);
  auto codes = ProduceCodes(func_str, define_str, prepare_str);
  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>);
  ASSERT_NOT_OK(CompileCodes(codes, "condition_check_3"));
  std::vector<bool> res;
  ASSERT_NOT_OK(ExecFunction("condition_check_3", table_0, table_1, &res));
  std::vector<bool> expected_res = {false, true,  false, false, false, false,
                                    false, false, false, false, true,  true};
  ASSERT_NOT_EQUAL(expected_res, res);
}

TEST(TestArrowComputeCondition, check4) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", utf8());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", uint32());

  auto func_node_0 = TreeExprBuilder::MakeFunction(
      "greater_than",
      {TreeExprBuilder::MakeField(table0_f1), TreeExprBuilder::MakeField(table1_f1)},
      arrow::boolean());
  auto func_node_1 = TreeExprBuilder::MakeInExpressionString(
      TreeExprBuilder::MakeField(table0_f0), {"BJ", "SH", "WH", "HZ"});
  auto func_node_2 = TreeExprBuilder::MakeFunction(
      "equal",
      {TreeExprBuilder::MakeField(table0_f2), TreeExprBuilder::MakeField(table1_f0)},
      arrow::boolean());
  auto func_node_3 =
      TreeExprBuilder::MakeFunction("not", {func_node_2}, arrow::boolean());
  auto func_node = TreeExprBuilder::MakeAnd(
      {TreeExprBuilder::MakeAnd({func_node_0, func_node_1}), func_node_3});
  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::shared_ptr<arrow::RecordBatch> table_1;
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::string> input_data_string = {
      R"(["BJ", "SH", "SZ", "HZ", "NB", "AU"])", "[null, 3, 8, 2, 13, 11]",
      R"(["Female", "Female", "Male", "Female", "Female", "Male"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["TY", "LA", "SZ", "HZ", "BJ", "HZ"])",
                       "[6, 12, 5, 8, 16, 110]",
                       R"(["Female", "Female", "Male", "Female", "Female", "Female"])"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {
      R"(["Female", "Male", "Male", "Female", "Female", "Male", "Female", "Female", "Male", "Female", "Male", "Male"])",
      "[1, 2, 3, null, 5, 6, 7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &table_1);

  //////////////////////// data prepared /////////////////////////

  std::vector<int> left_out_index_list;
  std::vector<int> right_out_index_list;
  std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
  int func_count = 0;
  std::vector<std::string> input_list;
  MakeCodeGenNodeVisitor(func_node, {schema_table_0->fields(), schema_table_1->fields()},
                         &func_count, &input_list, &left_out_index_list,
                         &right_out_index_list, &func_node_visitor);

  auto func_str = R"(
inline bool ConditionCheck(ArrayItemIndex x, int y) {
)" + func_node_visitor->GetPrepare() +
                  R"(
return )" + func_node_visitor->GetResult() +
                  R"(;
})";

  std::string prepare_str;
  std::string define_str;
  ProduceVars(schema_table_0, schema_table_1, &prepare_str, &define_str);
  auto codes = ProduceCodes(func_str, define_str, prepare_str);
  void (*Function)(std::vector<std::shared_ptr<arrow::RecordBatch>>,
                   std::shared_ptr<arrow::RecordBatch>);
  ASSERT_NOT_OK(CompileCodes(codes, "condition_check_4"));
  std::vector<bool> res;
  ASSERT_NOT_OK(ExecFunction("condition_check_4", table_0, table_1, &res));
  std::vector<bool> expected_res = {false, true,  false, false, false, false,
                                    false, false, false, false, true,  true};
  ASSERT_NOT_EQUAL(expected_res, res);
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
