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

#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include <gandiva/node.h>
#include <iostream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
std::string CodeGenNodeVisitor::GetResult() { return codes_str_; }
std::string CodeGenNodeVisitor::GetPreCheck() { return check_str_; }
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FunctionNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, codes_ss_,
                                         left_indices_, right_indices_, &child_visitor));
    child_visitor_list.push_back(child_visitor);
  }

  auto func_name = node.descriptor()->name();
  std::stringstream ss;
  if (func_name.compare("less_than") == 0) {
    ss << "(" << child_visitor_list[0]->GetResult() << " < "
       << child_visitor_list[1]->GetResult() << ")";
  } else if (func_name.compare("greater_than") == 0) {
    ss << "(" << child_visitor_list[0]->GetResult() << " > "
       << child_visitor_list[1]->GetResult() << ")";
  } else if (func_name.compare("less_than_or_equal_to") == 0) {
    ss << "(" << child_visitor_list[0]->GetResult()
       << " <= " << child_visitor_list[1]->GetResult() << ")";
  } else if (func_name.compare("greater_than_or_equal_to") == 0) {
    ss << "(" << child_visitor_list[0]->GetResult()
       << " >= " << child_visitor_list[1]->GetResult() << ")";
  } else if (func_name.compare("equal") == 0) {
    ss << "(" << child_visitor_list[0]->GetResult()
       << " == " << child_visitor_list[1]->GetResult() << ")";
  } else if (func_name.compare("not") == 0) {
    ss << "!(" << child_visitor_list[0]->GetResult() << ")";
  } else {
    ss << child_visitor_list[0]->GetResult();
  }
  if (cur_func_id == 0) {
    codes_str_ = ss.str();
  } else {
    codes_str_ = ss.str();
  }
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FieldNode& node) {
  auto cur_func_id = *func_count_;
  auto this_field = node.field();
  int arg_id = 0;
  bool found = false;
  int index = 0;
  for (auto field_list : field_list_v_) {
    arg_id = 0;
    for (auto field : field_list) {
      if (field->name() == this_field->name()) {
        found = true;
        InsertToIndices(index, arg_id);
        break;
      }
      arg_id++;
    }
    if (found) {
      break;
    }
    index = 1;
  }
  if (index == 0) {
    *codes_ss_ << "if (cached_0_" << arg_id
               << "_[x.array_id]->IsNull(x.id)) {return false;}" << std::endl;
    *codes_ss_ << "auto input_field_" << cur_func_id << " = cached_0_" << arg_id
               << "_[x.array_id]->GetView(x.id);" << std::endl;

  } else {
    *codes_ss_ << "if (cached_1_" << arg_id << "_->IsNull(y)) {return false;}"
               << std::endl;
    *codes_ss_ << "auto input_field_" << cur_func_id << " = cached_1_" << arg_id
               << "_->GetView(y);" << std::endl;
  }

  std::stringstream ss;
  ss << "input_field_" << cur_func_id;
  codes_str_ = ss.str();

  check_str_ = "";
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::IfNode& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::LiteralNode& node) {
  auto cur_func_id = *func_count_;
  if (node.return_type()->id() == arrow::Type::STRING) {
    *codes_ss_ << "auto input_field_" << cur_func_id << R"( = ")"
               << gandiva::ToString(node.holder()) << R"(";)" << std::endl;

  } else {
    *codes_ss_ << "auto input_field_" << cur_func_id << " = "
               << gandiva::ToString(node.holder()) << ";" << std::endl;
  }

  std::stringstream ss;
  ss << "input_field_" << cur_func_id;
  codes_str_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::BooleanNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, codes_ss_,
                                         left_indices_, right_indices_, &child_visitor));
    child_visitor_list.push_back(child_visitor);
  }

  std::stringstream ss;
  if (node.expr_type() == gandiva::BooleanNode::AND) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") && ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  if (node.expr_type() == gandiva::BooleanNode::OR) {
    ss << "(" << child_visitor_list[0]->GetResult() << ") || ("
       << child_visitor_list[1]->GetResult() << ")";
  }
  codes_str_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::InExpressionNode<int>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<CodeGenNodeVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;
  RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                       codes_ss_, left_indices_, right_indices_,
                                       &child_visitor));
  *codes_ss_ << "std::vector<int> input_field_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      *codes_ss_ << ", ";
    }
    // add type in the front to differentiate
    *codes_ss_ << value;
    add_comma = true;
  }
  *codes_ss_ << "};" << std::endl;

  std::stringstream ss;
  ss << "std::find(input_field_" << cur_func_id << ".begin(), input_field_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "input_field_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  check_str_ = child_visitor->GetPreCheck();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::InExpressionNode<long int>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<CodeGenNodeVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;
  RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                       codes_ss_, left_indices_, right_indices_,
                                       &child_visitor));
  *codes_ss_ << "std::vector<long int> input_field_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      *codes_ss_ << ", ";
    }
    // add type in the front to differentiate
    *codes_ss_ << value;
    add_comma = true;
  }
  *codes_ss_ << "};" << std::endl;

  std::stringstream ss;
  ss << "std::find(input_field_" << cur_func_id << ".begin(), input_field_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "input_field_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  check_str_ = child_visitor->GetPreCheck();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(
    const gandiva::InExpressionNode<std::string>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<CodeGenNodeVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;
  RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                       codes_ss_, left_indices_, right_indices_,
                                       &child_visitor));
  *codes_ss_ << "std::vector<std::string> input_field_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      *codes_ss_ << ", ";
    }
    // add type in the front to differentiate
    *codes_ss_ << R"(")" << value << R"(")";
    add_comma = true;
  }
  *codes_ss_ << "};" << std::endl;

  std::stringstream ss;
  ss << "std::find(input_field_" << cur_func_id << ".begin(), input_field_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "input_field_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  check_str_ = child_visitor->GetPreCheck();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::InsertToIndices(int index, int arg_id) {
  if (index == 0) {
    if (std::find((*left_indices_).begin(), (*left_indices_).end(), arg_id) ==
        (*left_indices_).end()) {
      (*left_indices_).push_back(arg_id);
    }
  }
  if (index == 1) {
    if (std::find((*right_indices_).begin(), (*right_indices_).end(), arg_id) ==
        (*right_indices_).end()) {
      (*right_indices_).push_back(arg_id);
    }
  }

  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
