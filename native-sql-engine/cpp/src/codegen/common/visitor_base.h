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

#include <arrow/status.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>

namespace sparkcolumnarplugin {
namespace codegen {
class VisitorBase : public gandiva::NodeVisitor {
  arrow::Status Visit(const gandiva::FunctionNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::FieldNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::IfNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::LiteralNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::BooleanNode& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<int>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<long int>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<float>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<double>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override {
    return arrow::Status::OK();
  }
  arrow::Status Visit(
      const gandiva::InExpressionNode<gandiva::DecimalScalar128>& node) override {
    return arrow::Status::OK();
  }
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
