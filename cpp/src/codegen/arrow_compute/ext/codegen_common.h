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
#include <arrow/compute/context.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include <sstream>
#include <string>

#include "codegen/arrow_compute/ext/code_generator_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

std::string BaseCodes();

int FileSpinLock();

void FileSpinUnLock(int fd);

int GetBatchSize();
bool GetEnableTimeMetrics();
std::string exec(const char* cmd);
std::string GetTempPath();
std::string GetArrowTypeDefString(std::shared_ptr<arrow::DataType> type);
std::string GetCTypeString(std::shared_ptr<arrow::DataType> type);
std::string GetTypeString(std::shared_ptr<arrow::DataType> type,
                          std::string tail = "Type");
std::string GetTemplateString(std::shared_ptr<arrow::DataType> type,
                              std::string template_name, std::string tail = "",
                              std::string prefix = "");
gandiva::ExpressionPtr GetConcatedKernel(std::vector<gandiva::NodePtr> key_list);
gandiva::ExpressionPtr GetHash32Kernel(std::vector<gandiva::NodePtr> key_list);
gandiva::ExpressionVector GetGandivaKernel(std::vector<gandiva::NodePtr> key_list);
template <typename T>
std::string GetStringFromList(std::vector<T> list) {
  std::stringstream ss;
  for (auto i : list) {
    ss << i << std::endl;
  }
  return ss.str();
}
std::string GetParameterList(std::vector<std::string> parameter_list,
                             bool comma_ahead = true, std::string split = ", ");
arrow::Status GetIndexList(const std::vector<std::shared_ptr<arrow::Field>>& target_list,
                           const std::vector<std::shared_ptr<arrow::Field>>& source_list,
                           std::vector<int>* out);
arrow::Status GetIndexList(
    const std::vector<std::shared_ptr<arrow::Field>>& target_list,
    const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
    const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
    const bool isExistJoin, int* exist_index,
    std::vector<std::pair<int, int>>* result_schema_index_list);
arrow::Status GetIndexListFromSchema(
    const std::shared_ptr<arrow::Schema>& result_schema,
    const std::vector<std::shared_ptr<arrow::Field>>& field_list,
    std::vector<int>* index_list);
std::pair<int, int> GetFieldIndex(gandiva::FieldPtr target_field,
                                  std::vector<gandiva::FieldVector> field_list_v);

arrow::Status CompileCodes(std::string codes, std::string signature);

arrow::Status LoadLibrary(std::string signature, arrow::compute::FunctionContext* ctx,
                          std::shared_ptr<CodeGenBase>* out);
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
