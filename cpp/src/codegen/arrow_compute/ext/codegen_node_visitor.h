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

#include <sstream>

#include "codegen/arrow_compute/ext/typed_action_codegen_impl.h"
#include "codegen/common/visitor_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class CodeGenNodeVisitor : public VisitorBase {
 public:
  CodeGenNodeVisitor(std::shared_ptr<gandiva::Node> func,
                     std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v,
                     int* func_count, std::vector<std::string>* input_list,
                     std::vector<int>* left_indices, std::vector<int>* right_indices,
                     std::vector<gandiva::ExpressionPtr>* project_list)
      : func_(func),
        field_list_v_(field_list_v),
        func_count_(func_count),
        input_list_(input_list),
        left_indices_(left_indices),
        right_indices_(right_indices),
        project_list_(project_list) {}
  CodeGenNodeVisitor(std::shared_ptr<gandiva::Node> func,
                     std::vector<std::shared_ptr<arrow::Field>> field_list,
                     std::vector<std::string>* input_list)
      : func_(func), field_list_v_({field_list}), input_list_(input_list) {
    action_impl_ = std::make_shared<TypedActionCodeGenImpl>();
    func_count_ = action_impl_->GetFuncCountRef();
    left_indices_ = action_impl_->GetInputIndexListRef();
    left_field_ = action_impl_->GetInputFieldsListRef();
  }
  CodeGenNodeVisitor(std::shared_ptr<gandiva::Node> func,
                     std::vector<std::shared_ptr<arrow::Field>> field_list,
                     std::shared_ptr<TypedActionCodeGenImpl> action_impl,
                     std::vector<std::string>* input_list)
      : func_(func),
        field_list_v_({field_list}),
        action_impl_(action_impl),
        input_list_(input_list) {
    func_count_ = action_impl_->GetFuncCountRef();
    left_indices_ = action_impl_->GetInputIndexListRef();
    left_field_ = action_impl_->GetInputFieldsListRef();
  }
  enum FieldType { left, right, literal, mixed, unknown };

  arrow::Status Eval() {
    RETURN_NOT_OK(func_->Accept(*this));
    return arrow::Status::OK();
  }

  arrow::Status ProduceCodes(std::shared_ptr<ActionCodeGen>* action_codegen) {
    action_impl_->ProduceCodes(action_codegen);
    return arrow::Status::OK();
  }

  std::string GetInput();
  std::string GetResult();
  std::string GetResultValidity();
  std::string GetPreCheck();
  std::string GetPrepare();
  std::string GetRealResult();
  std::string GetRealValidity();
  FieldType GetFieldType();
  gandiva::ExpressionPtr GetProjectExpr();
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::FieldNode& node) override;
  arrow::Status Visit(const gandiva::IfNode& node) override;
  arrow::Status Visit(const gandiva::LiteralNode& node) override;
  arrow::Status Visit(const gandiva::BooleanNode& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<long int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override;

 private:
  std::shared_ptr<gandiva::Node> func_;
  std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v_;
  int* func_count_;
  FieldType field_type_ = unknown;
  // output
  std::shared_ptr<TypedActionCodeGenImpl> action_impl_;
  std::vector<std::string>* input_list_;
  std::string real_codes_str_;
  std::string real_validity_str_;
  std::string codes_str_;
  std::string codes_validity_str_;
  std::string prepare_str_;
  std::string input_codes_str_;
  std::string check_str_;
  gandiva::ExpressionPtr project_;
  std::vector<int>* left_indices_ = nullptr;
  std::vector<std::shared_ptr<arrow::Field>>* left_field_ = nullptr;
  std::vector<int>* right_indices_ = nullptr;
  std::vector<std::shared_ptr<arrow::Field>>* right_field_ = nullptr;
  std::vector<gandiva::ExpressionPtr>* project_list_;
  arrow::Status InsertToIndices(int index, int arg_id,
                                std::shared_ptr<arrow::Field> field);
  arrow::Status ProduceGandivaFunction();
  arrow::Status AppendProjectList(
      const std::vector<std::shared_ptr<CodeGenNodeVisitor>>& child_visitor_list, int i);
  std::string GetNaNCheckStr(std::string left, std::string right, std::string func);
};
static arrow::Status MakeCodeGenNodeVisitor(
    std::shared_ptr<gandiva::Node> func,
    std::vector<std::shared_ptr<arrow::Field>> field_list,
    std::shared_ptr<TypedActionCodeGenImpl> action_impl,
    std::shared_ptr<CodeGenNodeVisitor>* out) {
  std::vector<std::string> input_list;
  auto visitor =
      std::make_shared<CodeGenNodeVisitor>(func, field_list, action_impl, &input_list);
  RETURN_NOT_OK(visitor->Eval());
  *out = visitor;
  return arrow::Status::OK();
}
static arrow::Status MakeCodeGenNodeVisitor(
    std::shared_ptr<gandiva::Node> func,
    std::vector<std::shared_ptr<arrow::Field>> field_list,
    std::shared_ptr<ActionCodeGen>* out_action_impl,
    std::shared_ptr<CodeGenNodeVisitor>* out) {
  std::vector<std::string> input_list;
  auto visitor = std::make_shared<CodeGenNodeVisitor>(func, field_list, &input_list);
  RETURN_NOT_OK(visitor->Eval());
  RETURN_NOT_OK(visitor->ProduceCodes(out_action_impl));
  *out = visitor;
  return arrow::Status::OK();
}
static arrow::Status MakeCodeGenNodeVisitor(
    std::shared_ptr<gandiva::Node> func,
    std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v, int* func_count,
    std::vector<std::string>* input_list, std::vector<int>* left_indices,
    std::vector<int>* right_indices, std::vector<gandiva::ExpressionPtr>* project_list,
    std::shared_ptr<CodeGenNodeVisitor>* out) {
  auto visitor =
      std::make_shared<CodeGenNodeVisitor>(func, field_list_v, func_count, input_list,
                                           left_indices, right_indices, project_list);
  RETURN_NOT_OK(visitor->Eval());
  *out = visitor;
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
