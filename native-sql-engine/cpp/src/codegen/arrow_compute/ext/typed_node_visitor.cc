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

#include "codegen/arrow_compute/ext/typed_node_visitor.h"

#include <gandiva/node.h>
#include <gandiva/node_visitor.h>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

TypedNodeVisitor::ResultType TypedNodeVisitor::GetResultType() {
  return res_type_;
}
arrow::Status TypedNodeVisitor::GetTypedNode(
    std::shared_ptr<gandiva::FunctionNode>* out) {
  *out = function_node_;
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::GetTypedNode(
    std::shared_ptr<gandiva::FieldNode>* out) {
  *out = field_node_;
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::GetTypedNode(
    std::shared_ptr<gandiva::IfNode>* out) {
  *out = if_node_;
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::GetTypedNode(
    std::shared_ptr<gandiva::LiteralNode>* out) {
  *out = literal_node_;
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::GetTypedNode(
    std::shared_ptr<gandiva::BooleanNode>* out) {
  *out = boolean_node_;
  return arrow::Status::OK();
}

arrow::Status TypedNodeVisitor::Visit(const gandiva::FunctionNode& node) {
  res_type_ = FunctionNode;
  function_node_ = std::dynamic_pointer_cast<gandiva::FunctionNode>(func_);
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::Visit(const gandiva::FieldNode& node) {
  res_type_ = FieldNode;
  field_node_ = std::dynamic_pointer_cast<gandiva::FieldNode>(func_);
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::Visit(const gandiva::IfNode& node) {
  res_type_ = IfNode;
  if_node_ = std::dynamic_pointer_cast<gandiva::IfNode>(func_);
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::Visit(const gandiva::LiteralNode& node) {
  res_type_ = LiteralNode;
  literal_node_ = std::dynamic_pointer_cast<gandiva::LiteralNode>(func_);
  return arrow::Status::OK();
}
arrow::Status TypedNodeVisitor::Visit(const gandiva::BooleanNode& node) {
  res_type_ = BooleanNode;
  boolean_node_ = std::dynamic_pointer_cast<gandiva::BooleanNode>(func_);
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
