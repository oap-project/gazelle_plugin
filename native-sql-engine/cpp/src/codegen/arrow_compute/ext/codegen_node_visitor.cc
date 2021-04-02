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
gandiva::ExpressionPtr CodeGenNodeVisitor::GetProjectExpr() { return project_; }
CodeGenNodeVisitor::FieldType CodeGenNodeVisitor::GetFieldType() { return field_type_; }
arrow::Status CodeGenNodeVisitor::ProduceGandivaFunction() {
  std::stringstream prepare_ss;
  auto return_type = func_->return_type();
  std::stringstream signature_ss;
  signature_ss << std::hash<std::string>{}(func_->ToString());
  auto arg_id = signature_ss.str();
  switch (field_type_) {
    case left: {
      codes_str_ = "projected_batch_0_" + arg_id;
      check_str_ = "projected_batch_validity_0_" + arg_id;
      input_codes_str_ = "projected_batch_left_" + arg_id + "_";
      prepare_ss << "  bool " << check_str_ << " = true;" << std::endl;
      prepare_ss << "  " << GetCTypeString(return_type) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "  if (" << input_codes_str_ << "[x.array_id]->IsNull(x.id)) {"
                 << std::endl;
      prepare_ss << "    " << check_str_ << " = false;" << std::endl;
      prepare_ss << "  } else {" << std::endl;
      if (return_type->id() != arrow::Type::STRING) {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "[x.array_id]->GetView(x.id);" << std::endl;
      } else {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "[x.array_id]->GetString(x.id);" << std::endl;
      }
      prepare_ss << "  }" << std::endl;
    } break;
    case right: {
      codes_str_ = "projected_batch_1_" + arg_id;
      check_str_ = "projected_batch_validity_1_" + arg_id;
      input_codes_str_ = "projected_batch_right_" + arg_id + "_";
      prepare_ss << "  bool " << check_str_ << " = true;" << std::endl;
      prepare_ss << "  " << GetCTypeString(return_type) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "  if (" << input_codes_str_ << "->IsNull(y)) {" << std::endl;
      prepare_ss << "    " << check_str_ << " = false;" << std::endl;
      prepare_ss << "  } else {" << std::endl;
      if (return_type->id() != arrow::Type::STRING) {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_ << "->GetView(y);"
                   << std::endl;
      } else {
        prepare_ss << "    " << codes_str_ << " = " << input_codes_str_
                   << "->GetString(y);" << std::endl;
      }
      prepare_ss << "  }" << std::endl;
    } break;
    default:
      return arrow::Status::NotImplemented(
          "Unable to support function whose chidren both from left and right "
          "Or "
          "Unknown.");
  }
  prepare_str_ = prepare_ss.str();
  auto res_field = arrow::field(input_codes_str_, return_type);
  project_ = gandiva::TreeExprBuilder::MakeExpression(func_, res_field);
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::AppendProjectList(
    const std::vector<std::shared_ptr<CodeGenNodeVisitor>>& child_visitor_list, int i) {
  auto project = child_visitor_list[i]->GetProjectExpr();
  if (project) {
    for (auto p : *project_list_) {
      if (p->result()->name() == project->result()->name()) return arrow::Status::OK();
    }
    (*project_list_).push_back(project);
  }
  prepare_str_ += child_visitor_list[i]->GetPrepare();
  return arrow::Status::OK();
}
arrow::Status CodeGenNodeVisitor::Visit(const gandiva::FunctionNode& node) {
  std::vector<std::string> non_gandiva_func_list = {"less_than",
                                                    "less_than_with_nan",
                                                    "greater_than",
                                                    "greater_than_with_nan",
                                                    "less_than_or_equal_to",
                                                    "less_than_or_equal_to_with_nan",
                                                    "greater_than_or_equal_to",
                                                    "greater_than_or_equal_to_with_nan",
                                                    "equal",
                                                    "equal_with_nan",
                                                    "not",
                                                    "substr",
                                                    "add",
                                                    "subtract",
                                                    "multiply",
                                                    "divide",
                                                    "isnotnull"};
  auto func_name = node.descriptor()->name();
  auto input_list = input_list_;
  if (func_name.compare(0, 7, "action_") != 0 &&
      func_name.find("cast") == std::string::npos &&
      std::find(non_gandiva_func_list.begin(), non_gandiva_func_list.end(), func_name) ==
          non_gandiva_func_list.end()) {
    input_list = nullptr;
  }
  std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
  auto cur_func_id = *func_count_;
  for (auto child : node.children()) {
    std::shared_ptr<CodeGenNodeVisitor> child_visitor;
    *func_count_ = *func_count_ + 1;
    if (action_impl_) {
      RETURN_NOT_OK(
          MakeCodeGenNodeVisitor(child, field_list_v_[0], action_impl_, &child_visitor));
    } else {
      // When set input_list as nullptr, MakeCodeGenNodeVisitor only check its
      // children's field_type won't add codes.
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, input_list,
                                           left_indices_, right_indices_, project_list_,
                                           &child_visitor));
    }
    child_visitor_list.push_back(child_visitor);
    if (field_type_ == unknown || field_type_ == literal) {
      field_type_ = child_visitor->GetFieldType();
    } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
               child_visitor->GetFieldType() != literal) {
      field_type_ = mixed;
    }
  }

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
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("less_than_with_nan") == 0) {
      // comparison for NaN is not really supported here
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " < " +
                        child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("greater_than") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " > " +
                        child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("greater_than_with_nan") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() + " > " +
                        child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("less_than_or_equal_to") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " <= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("less_than_or_equal_to_with_nan") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " <= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("greater_than_or_equal_to") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " >= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("greater_than_or_equal_to_with_nan") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " >= " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("equal") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " == " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("equal_with_nan") == 0) {
      real_codes_str_ = "(" + child_visitor_list[0]->GetResult() +
                        " == " + child_visitor_list[1]->GetResult() + ")";
      real_validity_str_ = child_visitor_list[0]->GetPreCheck() + " && " +
                           child_visitor_list[1]->GetPreCheck();
      ss << real_validity_str_ << " && " << real_codes_str_;
      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("not") == 0) {
      std::string check_validity;
      if (child_visitor_list[0]->GetPreCheck() != "") {
        check_validity = child_visitor_list[0]->GetPreCheck() + " && ";
      }
      ss << check_validity << child_visitor_list[0]->GetRealValidity() << " && !"
         << child_visitor_list[0]->GetRealResult();
      for (int i = 0; i < 1; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("isnotnull") == 0) {
      for (int i = 0; i < 1; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = child_visitor_list[0]->GetPreCheck();
    } else if (func_name.compare("substr") == 0) {
      ss << child_visitor_list[0]->GetResult() << ".substr("
         << "((" << child_visitor_list[1]->GetResult() << " - 1) < 0 ? 0 : ("
         << child_visitor_list[1]->GetResult() << " - 1)), "
         << child_visitor_list[2]->GetResult() << ")";
      check_str_ = child_visitor_list[0]->GetPreCheck();
      for (int i = 0; i < 3; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.find("cast") != std::string::npos &&
               func_name.compare("castDATE") != 0 &&
               func_name.compare("castDECIMAL") != 0 &&
               func_name.compare("castDECIMALNullOnOverflow") != 0) {
      ss << child_visitor_list[0]->GetResult();
      check_str_ = child_visitor_list[0]->GetPreCheck();
      for (int i = 0; i < 1; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      codes_str_ = ss.str();
    } else if (func_name.compare("castDECIMAL") == 0) {
      codes_str_ = func_name + "_" + std::to_string(cur_func_id);
      auto validity = codes_str_ + "_validity";
      std::stringstream fix_ss;
      auto decimal_type =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(node.return_type());
      auto childNode = node.children().at(0);
      if (childNode->return_type()->id() != arrow::Type::DECIMAL) {
        // if not casting from Decimal
        fix_ss << ", " << decimal_type->precision() << ", " << decimal_type->scale();
      } else {
        // if casting from Decimal
        auto childType =
            std::dynamic_pointer_cast<arrow::Decimal128Type>(childNode->return_type());
        fix_ss << ", " << childType->precision() << ", " << childType->scale() << ", "
               << decimal_type->precision() << ", " << decimal_type->scale();
      }
      std::stringstream prepare_ss;
      prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "bool " << validity << " = " << child_visitor_list[0]->GetPreCheck()
                 << ";" << std::endl;
      prepare_ss << "if (" << validity << ") {" << std::endl;
      prepare_ss << codes_str_ << " = " << func_name << "("
                 << child_visitor_list[0]->GetResult() << fix_ss.str() << ");"
                 << std::endl;
      prepare_ss << "}" << std::endl;

      for (int i = 0; i < 1; i++) {
        prepare_str_ += child_visitor_list[i]->GetPrepare();
      }
      prepare_str_ += prepare_ss.str();
      check_str_ = validity;

    } else if (func_name.compare("add") == 0) {
      codes_str_ = "add_" + std::to_string(cur_func_id);
      auto validity = "add_validity_" + std::to_string(cur_func_id);
      std::stringstream prepare_ss;
      prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "bool " << validity << " = (" << child_visitor_list[0]->GetPreCheck()
                 << " && " << child_visitor_list[1]->GetPreCheck() << ");" << std::endl;
      prepare_ss << "if (" << validity << ") {" << std::endl;
      prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " + "
                 << child_visitor_list[1]->GetResult() << ";" << std::endl;
      prepare_ss << "}" << std::endl;

      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      prepare_str_ += prepare_ss.str();
      check_str_ = validity;
    } else if (func_name.compare("subtract") == 0) {
      codes_str_ = "subtract_" + std::to_string(cur_func_id);
      auto validity = "subtract_validity_" + std::to_string(cur_func_id);
      std::stringstream prepare_ss;
      prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "bool " << validity << " = (" << child_visitor_list[0]->GetPreCheck()
                 << " && " << child_visitor_list[1]->GetPreCheck() << ");" << std::endl;
      prepare_ss << "if (" << validity << ") {" << std::endl;
      prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " - "
                 << child_visitor_list[1]->GetResult() << ";" << std::endl;
      prepare_ss << "}" << std::endl;

      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      prepare_str_ += prepare_ss.str();
      check_str_ = validity;
    } else if (func_name.compare("multiply") == 0) {
      codes_str_ = "multiply_" + std::to_string(cur_func_id);
      auto validity = "multiply_validity_" + std::to_string(cur_func_id);
      std::stringstream prepare_ss;
      prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "bool " << validity << " = (" << child_visitor_list[0]->GetPreCheck()
                 << " && " << child_visitor_list[1]->GetPreCheck() << ");" << std::endl;
      prepare_ss << "if (" << validity << ") {" << std::endl;
      prepare_ss << codes_str_ << " = " << child_visitor_list[0]->GetResult() << " * "
                 << child_visitor_list[1]->GetResult() << ";" << std::endl;
      prepare_ss << "}" << std::endl;

      for (int i = 0; i < 2; i++) {
        RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
      }
      prepare_str_ += prepare_ss.str();
      check_str_ = validity;
    } else if (func_name.compare("divide") == 0) {
      codes_str_ = "divide_" + std::to_string(cur_func_id);
      auto validity = codes_str_ + "_validity";
      std::stringstream fix_ss;
      if (node.return_type()->id() != arrow::Type::DECIMAL) {
        fix_ss << child_visitor_list[0]->GetResult() << " / "
               << child_visitor_list[1]->GetResult();
      } else {
        auto leftNode = node.children().at(0);
        auto rightNode = node.children().at(1);
        auto leftType =
            std::dynamic_pointer_cast<arrow::Decimal128Type>(leftNode->return_type());
        auto rightType =
            std::dynamic_pointer_cast<arrow::Decimal128Type>(rightNode->return_type());
        auto resType =
            std::dynamic_pointer_cast<arrow::Decimal128Type>(node.return_type());
        fix_ss << "divide(" << child_visitor_list[0]->GetResult() << ", "
               << leftType->precision() << ", " << leftType->scale() << ", "
               << child_visitor_list[1]->GetResult() << ", " << rightType->precision()
               << ", " << rightType->scale() << ", " << resType->precision() << ", "
               << resType->scale() << ", &overflow)";
      }
      std::stringstream prepare_ss;
      prepare_ss << GetCTypeString(node.return_type()) << " " << codes_str_ << ";"
                 << std::endl;
      prepare_ss << "bool " << validity << " = ("
                 << CombineValidity({child_visitor_list[0]->GetPreCheck(),
                                     child_visitor_list[1]->GetPreCheck()})
                 << ");" << std::endl;
      prepare_ss << "if (" << validity << ") {" << std::endl;
      if (node.return_type()->id() == arrow::Type::DECIMAL) {
        prepare_ss << "bool overflow = false;" << std::endl;
      }
      prepare_ss << codes_str_ << " = " << fix_ss.str() << ";" << std::endl;
      if (node.return_type()->id() == arrow::Type::DECIMAL) {
        prepare_ss << "if (overflow) {\n" << validity << " = false;}" << std::endl;
      }
      prepare_ss << "}" << std::endl;

      for (int i = 0; i < 2; i++) {
        prepare_str_ += child_visitor_list[i]->GetPrepare();
      }
      prepare_str_ += prepare_ss.str();
      check_str_ = validity;
    } else {
      RETURN_NOT_OK(ProduceGandivaFunction());
    }
  }
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
      field_type_ = left;

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
      field_type_ = right;
    }
  }

  check_str_ = codes_validity_str_;
  if (input_list_ != nullptr) {
    if (std::find((*input_list_).begin(), (*input_list_).end(), codes_str_) ==
        (*input_list_).end()) {
      (*input_list_).push_back(codes_str_);
      prepare_str_ = prepare_ss.str();
    }
  }
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::IfNode& node) {
  if (action_impl_) {
    RETURN_NOT_OK(action_impl_->MakeGandivaProjection(func_, field_list_v_[0]));
  } else {
    std::stringstream prepare_ss;
    auto cur_func_id = *func_count_;

    std::vector<gandiva::NodePtr> children = {node.condition(), node.then_node(),
                                              node.else_node()};
    std::vector<std::shared_ptr<CodeGenNodeVisitor>> child_visitor_list;
    for (auto child : children) {
      std::shared_ptr<CodeGenNodeVisitor> child_visitor;
      *func_count_ = *func_count_ + 1;
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(child, field_list_v_, func_count_, input_list_,
                                           left_indices_, right_indices_, project_list_,
                                           &child_visitor));
      child_visitor_list.push_back(child_visitor);
      if (field_type_ == unknown || field_type_ == literal) {
        field_type_ = child_visitor->GetFieldType();
      } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
                 child_visitor->GetFieldType() != literal) {
        field_type_ = mixed;
      }
    }
    for (int i = 0; i < 3; i++) {
      RETURN_NOT_OK(AppendProjectList(child_visitor_list, i));
    }
    auto condition_name = "condition_" + std::to_string(cur_func_id);
    auto condition_validity = "condition_validity_" + std::to_string(cur_func_id);
    prepare_ss << GetCTypeString(node.return_type()) << " " << condition_name << ";"
               << std::endl;
    prepare_ss << "bool " << condition_validity << ";" << std::endl;
    prepare_ss << "if (" << child_visitor_list[0]->GetResult() << ") {" << std::endl;
    prepare_ss << condition_name << " = " << child_visitor_list[1]->GetResult() << ";"
               << std::endl;
    prepare_ss << condition_validity << " = " << child_visitor_list[1]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "} else {" << std::endl;
    prepare_ss << condition_name << " = " << child_visitor_list[2]->GetResult() << ";"
               << std::endl;
    prepare_ss << condition_validity << " = " << child_visitor_list[2]->GetPreCheck()
               << ";" << std::endl;
    prepare_ss << "}" << std::endl;
    codes_str_ = condition_name;
    prepare_str_ += prepare_ss.str();
    check_str_ = condition_validity;
    // RETURN_NOT_OK(ProduceGandivaFunction());
  }
  return arrow::Status::OK();
}

arrow::Status CodeGenNodeVisitor::Visit(const gandiva::LiteralNode& node) {
  auto cur_func_id = *func_count_;
  std::stringstream prepare_ss;
  if (node.return_type()->id() == arrow::Type::STRING) {
    prepare_ss << "auto literal_" << cur_func_id << R"( = ")"
               << gandiva::ToString(node.holder()) << R"(";)" << std::endl;

  } else if (node.return_type()->id() == arrow::Type::DECIMAL) {
    auto scalar = arrow::util::get<gandiva::DecimalScalar128>(node.holder());
    auto decimal = arrow::Decimal128(scalar.value());
    prepare_ss << "auto literal_" << cur_func_id << " = "
               << "arrow::Decimal128(\"" << decimal.ToString(scalar.scale()) << "\");"
               << std::endl;
    decimal_scale_ = std::to_string(scalar.scale());
  } else {
    prepare_ss << "auto literal_" << cur_func_id << " = "
               << gandiva::ToString(node.holder()) << ";" << std::endl;
  }

  std::stringstream ss;
  ss << "literal_" << cur_func_id;
  codes_str_ = ss.str();
  prepare_str_ = prepare_ss.str();
  check_str_ = node.is_null() ? "false" : "true";
  field_type_ = literal;
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
                                           left_indices_, right_indices_, project_list_,
                                           &child_visitor));
    }
    prepare_str_ += child_visitor->GetPrepare();
    child_visitor_list.push_back(child_visitor);
    if (field_type_ == unknown || field_type_ == literal) {
      field_type_ = child_visitor->GetFieldType();
    } else if (field_type_ != child_visitor->GetFieldType() && field_type_ != literal &&
               child_visitor->GetFieldType() != literal) {
      field_type_ = mixed;
    }
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
                                         project_list_, &child_visitor));
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
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  RETURN_NOT_OK(AppendProjectList({child_visitor}, 0));
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
                                         project_list_, &child_visitor));
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
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  RETURN_NOT_OK(AppendProjectList({child_visitor}, 0));
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
                                         project_list_, &child_visitor));
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
  prepare_str_ = prepare_ss.str();
  field_type_ = child_visitor->GetFieldType();
  RETURN_NOT_OK(AppendProjectList({child_visitor}, 0));
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

std::string CodeGenNodeVisitor::GetNaNCheckStr(std::string left, std::string right,
                                               std::string func) {
  std::stringstream ss;
  func = " " + func + " ";
  ss << "((std::isnan(" << left << ") && std::isnan(" << right << ")) ? (1.0 / 0.0"
     << func << "1.0 / 0.0) : "
     << "(std::isnan(" << left << ")) ? (1.0 / 0.0" << func << right << ") : "
     << "(std::isnan(" << right << ")) ? (" << left << func << "1.0 / 0.0) : "
     << "(" << left << func << right << "))";
  return ss.str();
}

std::string CodeGenNodeVisitor::CombineValidity(std::vector<std::string> validity_list) {
  bool first = true;
  std::stringstream out;
  for (int i = 0; i < validity_list.size(); i++) {
    auto validity = validity_list[i];
    if (first) {
      if (validity.compare("true") != 0) {
        out << validity;
        first = false;
      }
    } else {
      if (validity.compare("true") != 0) {
        out << " && " << validity;
      }
    }
  }
  return out.str();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
