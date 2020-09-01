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
std::string CodeGenNodeVisitor::GetInput() { return input_codes_str_; }
std::string CodeGenNodeVisitor::GetResult() { return codes_str_; }
std::string CodeGenNodeVisitor::GetPrepare() { return prepare_str_; }
std::string CodeGenNodeVisitor::GetPreCheck() { return check_str_; }
std::string CodeGenNodeVisitor::GetRealResult() { return real_codes_str_; }
std::string CodeGenNodeVisitor::GetRealValidity() { return real_validity_str_; }
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FunctionNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    if (action_impl_) {
      RETURN_NOT_OK(
          MakeCodeGenNodeVisitor(child, field_list_v_[0], action_impl_, &child_visitor));
    } else {
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, input_list_,
                                           left_indices_, right_indices_,
                                           &child_visitor));
    }
    prepare_str_ += child_visitor->GetPrepare();
    child_visitor_list.push_back(child_visitor);
  }

  auto func_name = node.descriptor()->name();
  std::stringstream ss;
  if (action_impl_) {
    if (func_name.compare(0, 7, "action_") == 0) {
      action_impl_->SetActionName(func_name);
      std::vector<std::string> child_res;
      for (auto child : child_visitor_list) {
        child_res.push_back(child->GetResult());
      }
      action_impl_->SetChildList(child_res);
      std::vector<std::string> input_list;
      for (auto child : child_visitor_list) {
        input_list.push_back(child->GetInput());
      }
      action_impl_->SetInputList(input_list);
    } else {
      // do projection when matches none.
      RETURN_NOT_OK(action_impl_->MakeGandivaProjection(func_, field_list_v_[0]));
    }
  } else {
    if (func_name.compare("less_than") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " < " +
                        child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
    } else if (func_name.compare("greater_than") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " > " +
                        child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
    } else if (func_name.compare("less_than_or_equal_to") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " <= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
    } else if (func_name.compare("greater_than_or_equal_to") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " >= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
    } else if (func_name.compare("equal") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " == " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
    } else if (func_name.compare("not") == 0) {
      std::string check_validity;
      if (child_visitor_list[0]->GetPreCheck() != "") {
        check_validity = child_visitor_list[0]->GetPreCheck() + " && ";
      }
      ss << check_validity << child_visitor_list[0]->GetRealValidity() << " && !"
         << child_visitor_list[0]->GetRealResult();
    } else if (func_name.compare("substr") == 0) {
      ss << child_visitor_list[0]->GetResult() << ".substr("
         << "((" << child_visitor_list[1]->GetResult() << " - 1) < 0 ? 0 : ("
         << child_visitor_list[1]->GetResult() << " - 1)), "
         << child_visitor_list[2]->GetResult() << ")";
      check_str_ = child_visitor_list[0]->GetPreCheck();
    } else if (func_name.find("cast") != std::string::npos) {
      ss << child_visitor_list[0]->GetResult();
      check_str_ = child_visitor_list[0]->GetPreCheck();
    } else {
      return arrow::Status::NotImplemented(func_name +
                                           " is not currently support inside condition.");
    }
  }
  codes_str_ = ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FieldNode& node) {
  auto cur_func_id = *func_count_;
  auto this_field = node.field();
  int arg_id = 0;
  bool found = false;
  int index = 0;
  std::stringstream prepare_ss;
  for (auto field_list : field_list_v_) {
    arg_id = 0;
    for (auto field : field_list) {
      if (field->name() == this_field->name()) {
        found = true;
        InsertToIndices(index, arg_id, field);
        break;
      }
      arg_id++;
    }
    if (found) {
      break;
    }
    index = 1;
  }
  if (field_list_v_.size() == 1) {
    input_codes_str_ = "in[" + std::to_string(arg_id) + "]";
    codes_str_ = "input_field_" + std::to_string(arg_id);
    auto typed_input_codes_str = "typed_in_" + std::to_string(arg_id);
    prepare_ss << "if (" << typed_input_codes_str << "->IsNull(cur_id_)) {return;}"
               << std::endl;
    if (this_field->type()->id() != arrow::Type::STRING) {
      prepare_ss << "auto " << codes_str_ << " = " << typed_input_codes_str
                 << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_ss << "auto " << codes_str_ << " = " << typed_input_codes_str
                 << "->GetString(cur_id_);" << std::endl;
    }
  } else {
    if (index == 0) {
      codes_str_ = "input_field_0_" + std::to_string(arg_id);
      codes_validity_str_ = "input_field_validity_0_" + std::to_string(arg_id);
      input_codes_str_ = "cached_0_" + std::to_string(arg_id) + "_";
      prepare_ss << "  bool " << codes_validity_str_ << " = true;" << std::endl;
      prepare_ss << "  " << GetCTypeString(this_field->type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "  if (" << input_codes_str_ << "[x.array_id]->IsNull(x.id)) {"
                 << std::endl;
      prepare_ss << "    " << codes_validity_str_ << " = false;" << std::endl;
      prepare_ss << "  } else {" << std::endl;
      if (this_field->type()->id() != arrow::Type::STRING) {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "[x.array_id]->GetView(x.id);" << std::endl;
      } else {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "[x.array_id]->GetString(x.id);" << std::endl;
      }
      prepare_ss << "  }" << std::endl;

    } else {
      codes_str_ = "input_field_1_" + std::to_string(arg_id);
      codes_validity_str_ = "input_field_validity_1_" + std::to_string(arg_id);
      input_codes_str_ = "cached_1_" + std::to_string(arg_id) + "_";
      prepare_ss << "  bool " << codes_validity_str_ << " = true;" << std::endl;
      prepare_ss << "  " << GetCTypeString(this_field->type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "  if (" << input_codes_str_ << "->IsNull(y)) {" << std::endl;
      prepare_ss << "    " << codes_validity_str_ << " = false;" << std::endl;
      prepare_ss << "  } else {" << std::endl;
      if (this_field->type()->id() != arrow::Type::STRING) {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_ << "->GetView(y);"
                   << std::endl;
      } else {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "->GetString(y);" << std::endl;
      }
      prepare_ss << "  }" << std::endl;
    }
  }

  check_str_ = codes_validity_str_;
  if (std::find((*input_list_).begin(), (*input_list_).end(), codes_str_) ==
      (*input_list_).end()) {
    (*input_list_).push_back(codes_str_);
    prepare_str_ = prepare_ss.str();
  }
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::IfNode& node) {
  if (action_impl_) {
    RETURN_NOT_OK(action_impl_->MakeGandivaProjection(func_, field_list_v_[0]));
  }
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::LiteralNode& node) {
  auto cur_func_id = *func_count_;
  std::stringstream prepare_ss;
  if (node.return_type()->id() == arrow::Type::STRING) {
    prepare_ss << "auto literal_" << cur_func_id << R"( = ")"
               << gandiva::ToString(node.holder()) << R"(";)" << std::endl;

  } else {
    prepare_ss << "auto literal_" << cur_func_id << " = "
               << gandiva::ToString(node.holder()) << ";" << std::endl;
  }

  std::stringstream ss;
  ss << "literal_" << cur_func_id;
  codes_str_ = ss.str();
  prepare_str_ = prepare_ss.str();
  check_str_ = "true";
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::BooleanNode& node) {
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    if (action_impl_) {
      RETURN_NOT_OK(
          MakeCodeGenNodeVisitor(child, field_list_v_[0], action_impl_, &child_visitor));
    } else {
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, input_list_,
                                           left_indices_, right_indices_,
                                           &child_visitor));
    }
    prepare_str_ += child_visitor->GetPrepare();
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
  if (action_impl_) {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_[0], action_impl_,
                                         &child_visitor));
  } else {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                         input_list_, left_indices_, right_indices_,
                                         &child_visitor));
  }
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<int> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << value;
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = child_visitor->GetPrepare() + prepare_ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::InExpressionNode<long int>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<CodeGenNodeVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;
  if (action_impl_) {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_[0], action_impl_,
                                         &child_visitor));
  } else {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                         input_list_, left_indices_, right_indices_,
                                         &child_visitor));
  }
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<long int> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << value;
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = child_visitor->GetPrepare() + prepare_ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(
    const gandiva::InExpressionNode<std::string>& node) {
  auto cur_func_id = *func_count_;
  std::shared_ptr<CodeGenNodeVisitor> child_visitor;
  *func_count_ = *func_count_ + 1;
  if (action_impl_) {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_[0], action_impl_,
                                         &child_visitor));
  } else {
    RETURN_NOT_OK(MakeCodeGenNodeVisitor(node.eval_expr(), field_list_v_, func_count_,
                                         input_list_, left_indices_, right_indices_,
                                         &child_visitor));
  }
  std::stringstream prepare_ss;
  prepare_ss << "std::vector<std::string> in_list_" << cur_func_id << " = {";
  bool add_comma = false;
  for (auto& value : node.values()) {
    if (add_comma) {
      prepare_ss << ", ";
    }
    // add type in the front to differentiate
    prepare_ss << R"(")" << value << R"(")";
    add_comma = true;
  }
  prepare_ss << "};" << std::endl;

  std::stringstream ss;
  ss << child_visitor->GetPreCheck() << " && "
     << "std::find(in_list_" << cur_func_id << ".begin(), in_list_" << cur_func_id
     << ".end(), " << child_visitor->GetResult() << ") != "
     << "in_list_" << cur_func_id << ".end()";
  codes_str_ = ss.str();
  prepare_str_ = child_visitor->GetPrepare() + prepare_ss.str();
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::InsertToIndices(int index, int arg_id,
                                                  std::shared_ptr<arrow::Field> field) {
  if (index == 0) {
    if (std::find((*left_indices_).begin(), (*left_indices_).end(), arg_id) ==
        (*left_indices_).end()) {
      (*left_indices_).push_back(arg_id);
      if (left_field_ != nullptr) {
        (*left_field_).push_back(field);
      }
    }
  }
  if (index == 1) {
    if (std::find((*right_indices_).begin(), (*right_indices_).end(), arg_id) ==
        (*right_indices_).end()) {
      (*right_indices_).push_back(arg_id);
      if (right_field_ != nullptr) {
        (*right_field_).push_back(field);
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
