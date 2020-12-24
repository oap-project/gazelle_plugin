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

#include "codegen/common/visitor_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class ExpressionCodegenVisitor : public VisitorBase {
 public:
  ExpressionCodegenVisitor(
      std::shared_ptr<gandiva::Node> func, std::vector<std::string> input_list,
      std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v,
      int hash_relation_id, int* func_count, std::vector<std::string>* prepared_list,
      bool is_smj)
      : func_(func),
        field_list_v_(field_list_v),
        func_count_(func_count),
        input_list_(input_list),
        prepared_list_(prepared_list),
        hash_relation_id_(hash_relation_id),
        is_smj_(is_smj) {}

  enum FieldType { left, right, sort_relation, literal, mixed, unknown };

  arrow::Status Eval() {
    RETURN_NOT_OK(func_->Accept(*this));
    return arrow::Status::OK();
  }

  std::string GetInput();
  std::string GetResult();
  std::string GetResultValidity();
  std::string GetPreCheck();
  std::string GetPrepare();
  std::string GetRealResult();
  std::string GetRealValidity();
  std::vector<std::string> GetHeaders();
  FieldType GetFieldType();
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::FieldNode& node) override;
  arrow::Status Visit(const gandiva::IfNode& node) override;
  arrow::Status Visit(const gandiva::LiteralNode& node) override;
  arrow::Status Visit(const gandiva::BooleanNode& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<long int>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override;
  std::string decimal_scale_;

 private:
  std::shared_ptr<gandiva::Node> func_;
  std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v_;
  int hash_relation_id_;
  std::vector<std::string> input_list_;
  int* func_count_;
  FieldType field_type_ = unknown;
  bool is_smj_;
  // output
  std::vector<std::string>* prepared_list_;
  std::vector<std::string> header_list_;
  std::string real_codes_str_;
  std::string real_validity_str_;
  std::string codes_str_;
  std::string codes_validity_str_;
  std::string prepare_str_;
  std::string input_codes_str_;
  std::string check_str_;

  std::string CombineValidity(std::vector<std::string> validity_list);
  std::string GetValidityName(std::string name);
  std::string GetNaNCheckStr(std::string left, std::string right, std::string func);
};

static arrow::Status MakeExpressionCodegenVisitor(
    std::shared_ptr<gandiva::Node> func, std::vector<std::string> input_list,
    std::vector<std::vector<std::shared_ptr<arrow::Field>>> field_list_v,
    int hash_relation_id, int* func_count, std::vector<std::string>* prepared_list,
    std::shared_ptr<ExpressionCodegenVisitor>* out, bool is_smj = false) {
  auto visitor = std::make_shared<ExpressionCodegenVisitor>(
      func, input_list, field_list_v, hash_relation_id, func_count, prepared_list,
      is_smj);
  RETURN_NOT_OK(visitor->Eval());
  *out = visitor;
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
