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
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <iostream>
#include <sstream>

#include "codegen/arrow_compute/ext/codegen_common.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};
class ActionCodeGen {
 public:
  bool IsGroupBy() { return is_key_; }
  bool IsPreProjected() {
    if (projector_expr_) {
      return true;
    } else {
      return false;
    }
  }
  virtual arrow::Status WithProjectIndex(int index) { return arrow::Status::OK(); }
  std::shared_ptr<gandiva::Expression> GetProjectorExpr() { return projector_expr_; }
  std::vector<gandiva::NodePtr> GetInputFieldList() { return input_expr_list_; }
  std::vector<std::string> GetInputDataNameList() { return input_data_list_; }
  std::vector<std::string> GetVariablesList() { return func_sig_list_; }
  std::vector<std::string> GetVariablesDefineList() {
    return func_sig_define_codes_list_;
  }
  std::vector<std::pair<std::string, std::string>> GetTypedInputList() {
    return typed_input_and_prepare_list_;
  }
  std::vector<std::string> GetOnExistsPrepareCodesList() {
    return on_exists_prepare_codes_list_;
  }
  std::vector<std::string> GetOnNewPrepareCodesList() {
    return on_new_prepare_codes_list_;
  }
  std::vector<std::string> GetOnExistsCodesList() { return on_exists_codes_list_; }
  std::vector<std::string> GetOnNewCodesList() { return on_new_codes_list_; }
  std::vector<std::string> GetOnFinishCodesList() { return on_finish_codes_list_; }
  std::vector<std::string> GetFinishVariablesList() { return finish_variable_list_; }
  std::vector<std::string> GetFinishVariablesDefineList() {
    return finish_var_define_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesParameterList() {
    return finish_var_parameter_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesPrepareList() {
    return finish_var_prepare_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesToBuilderList() {
    return finish_var_to_builder_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesToArrayList() {
    return finish_var_to_array_codes_list_;
  }
  std::vector<std::string> GetFinishVariablesArrayList() {
    return finish_var_array_codes_list_;
  }

 protected:
  bool is_key_ = false;
  std::shared_ptr<gandiva::Expression> projector_expr_;
  std::vector<gandiva::NodePtr> input_expr_list_;
  std::vector<std::string> input_data_list_;
  std::vector<std::pair<std::string, std::string>> typed_input_and_prepare_list_;
  std::vector<std::string> func_sig_list_;
  std::vector<std::string> func_sig_define_codes_list_;
  std::vector<std::string> on_exists_prepare_codes_list_;
  std::vector<std::string> on_new_prepare_codes_list_;
  std::vector<std::string> on_exists_codes_list_;
  std::vector<std::string> on_new_codes_list_;
  std::vector<std::string> on_finish_codes_list_;
  std::vector<std::string> finish_variable_list_;
  std::vector<std::string> finish_var_define_codes_list_;
  std::vector<std::string> finish_var_prepare_codes_list_;
  std::vector<std::string> finish_var_parameter_codes_list_;
  std::vector<std::string> finish_var_to_builder_codes_list_;
  std::vector<std::string> finish_var_to_array_codes_list_;
  std::vector<std::string> finish_var_array_codes_list_;
  std::string Replace(std::string& str_input, const std::string& oldStr,
                      const std::string& newStr) {
    auto str = str_input;
    std::string::size_type pos = 0u;
    while ((pos = str.find(oldStr, pos)) != std::string::npos) {
      str.replace(pos, oldStr.length(), newStr);
      pos += newStr.length();
    }
    return str;
  }
  std::string GetTypedVectorDefineString(std::shared_ptr<arrow::DataType> type,
                                         std::string name, bool use_ref = false) {
    if (use_ref) {
      return "const std::vector<" + GetCTypeString(type) + ">& " + name;
    } else {
      return "std::vector<" + GetCTypeString(type) + "> " + name;
    }
  }

  void GetTypedArrayCastString(std::shared_ptr<arrow::DataType> type, std::string name) {
    std::stringstream ss;
    auto cached_name = Replace(name, "[", "_");
    cached_name = "typed_" + Replace(cached_name, "]", "");
    ss << "auto " << cached_name << " = std::make_shared<" << GetTypeString(type, "Array")
       << ">(" << name << ");";
    typed_input_and_prepare_list_.push_back(std::make_pair(cached_name, ss.str()));
    input_data_list_.push_back(name);
  }

  void GetTypedArrayCastFromProjectedString(std::shared_ptr<arrow::DataType> type,
                                            std::string name) {
    std::stringstream ss;
    auto cached_name = "typed_projected_" + name;
    auto array_name = "projected_batch->column(" + name + ")";
    ss << "auto " << cached_name << " = std::make_shared<" << GetTypeString(type, "Array")
       << ">(" << array_name << ");";
    typed_input_and_prepare_list_.push_back(std::make_pair(cached_name, ss.str()));
    input_data_list_.push_back(array_name);
  }

  void GetTypedArrayCastByNameString(std::shared_ptr<arrow::DataType> type,
                                     std::string name) {
    std::stringstream ss;
    auto cached_name = "typed_" + name;
    ss << "auto " << cached_name << " = std::make_shared<" << GetTypeString(type, "Array")
       << ">(projected_batch->GetColumnByName(\"" << name << "\"));";
    typed_input_and_prepare_list_.push_back(std::make_pair(cached_name, ss.str()));
  }

  std::string GetTypedVectorAndBuilderDefineString(std::shared_ptr<arrow::DataType> type,
                                                   std::string name,
                                                   bool validity = false) {
    std::stringstream ss;
    auto cache_name = name + "_vector_";
    auto builder_name = name + "_builder_";
    if (validity) {
      auto validity_name = name + "validity__vector_";
      ss << "std::vector<bool> " << validity_name << ";" << std::endl;
    }
    ss << "std::vector<" << GetCTypeString(type) << "> " << cache_name << ";"
       << std::endl;
    ss << "std::shared_ptr<" << GetTypeString(type, "Builder") << "> " << builder_name
       << ";" << std::endl;
    return ss.str();
  }

  std::string GetTypedVectorAndBuilderPrepareString(std::shared_ptr<arrow::DataType> type,
                                                    std::string name,
                                                    bool validity = false) {
    std::stringstream ss;
    auto cache_name_tmp = name + "_vector_tmp";
    auto cache_name = name + "_vector_";
    ss << cache_name << " = " << cache_name_tmp << ";" << std::endl;
    if (validity) {
      auto validity_name = name + "validity__vector_";
      auto validity_name_tmp = name + "validity__vector_tmp";
      ss << validity_name << " = " << validity_name_tmp << ";" << std::endl;
    }
    auto builder_name_tmp = name + "_builder";
    auto builder_name = name + "_builder_";
    ss << builder_name << " = std::make_shared<" << GetTypeString(type, "Builder")
       << ">(ctx_->memory_pool());" << std::endl;
    return ss.str();
  }

  std::string GetTypedVectorToBuilderString(std::shared_ptr<arrow::DataType> type,
                                            std::string name, bool validity = false) {
    std::stringstream ss;
    auto cache_name = name + "_vector_";
    auto builder_name = name + "_builder_";
    if (validity) {
      auto validity_name = name + "validity__vector_";
      ss << "if (" << validity_name << "[offset_ + count]) {" << std::endl;
      ss << "RETURN_NOT_OK(" << builder_name << "->Append(" << cache_name
         << "[offset_ + count]));" << std::endl;
      ss << "} else {" << std::endl;
      ss << "RETURN_NOT_OK(" << builder_name << "->AppendNull());" << std::endl;
      ss << "}" << std::endl;
    } else {
      ss << "RETURN_NOT_OK(" << builder_name << "->Append(" << cache_name
         << "[offset_ + count]));" << std::endl;
    }
    return ss.str();
  }

  std::string GetTypedResultToArrayString(std::shared_ptr<arrow::DataType> type,
                                          std::string name) {
    std::stringstream ss;
    auto builder_name = name + "_builder_";
    auto out_name = name + "_out";
    ss << "std::shared_ptr<arrow::Array> " << out_name << ";" << std::endl;
    ss << "RETURN_NOT_OK(" << builder_name << "->Finish(&" << out_name << "));"
       << std::endl;
    ss << builder_name << "->Reset();" << std::endl;
    return ss.str();
  }

  std::string GetTypedResultArrayString(std::shared_ptr<arrow::DataType> type,
                                        std::string name) {
    return name + "_out";
  }
};

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

class GroupByActionCodeGen : public ActionCodeGen {
 public:
  GroupByActionCodeGen(std::string name, bool keep, std::vector<std::string> child_list,
                       std::vector<std::string> input_list,
                       std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                       std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = true;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }

    produce_ = [this, in_data_type, input_list, input_fields_list,
                keep](std::string name) {
      auto data_type = in_data_type;
      std::string sig_name;
      std::string validity_name;
      if (projector_expr_) {
        sig_name = "action_groupby_projected_" + name + "_";
        validity_name = "action_groupby_projected_" + name + "_validity_";
        GetTypedArrayCastFromProjectedString(data_type, name);
        input_expr_list_.push_back(
            projector_expr_->root());  // this line is used to gen hash for multiple keys

      } else {
        sig_name = "action_groupby_" + name + "_";
        validity_name = "action_groupby_" + name + "_validity_";
        GetTypedArrayCastString(data_type, input_list[0]);
        input_expr_list_.push_back(gandiva::TreeExprBuilder::MakeField(
            input_fields_list[0]));  // this line is used to gen hash for multiple keys
      }
      typed_input_and_prepare_list_.push_back(std::make_pair(
          "", ""));  // when there is two name in sig list, we need to make others aligned

      if (keep == false) {
        return;
      }
      func_sig_list_.push_back(sig_name);
      func_sig_list_.push_back(validity_name);
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";

      std::stringstream prepare_codes_ss;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << sig_name << ".push_back("
                         << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_));" << std::endl;
        prepare_codes_ss << validity_name << ".push_back(true);" << std::endl;
      } else {
        prepare_codes_ss << sig_name << ".push_back("
                         << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_));" << std::endl;
        prepare_codes_ss << validity_name << ".push_back(true);" << std::endl;
      }
      prepare_codes_ss << "} else {" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << sig_name << ".push_back(0);" << std::endl;
        prepare_codes_ss << validity_name << ".push_back(false);" << std::endl;
      } else {
        prepare_codes_ss << sig_name << ".push_back(\"\");" << std::endl;
        prepare_codes_ss << validity_name << ".push_back(false);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_variable_list_.push_back(validity_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
      finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
          arrow::boolean(), validity_name + "_vector_tmp", true));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name, true));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      finish_var_define_codes_list_.push_back("");
      finish_var_prepare_codes_list_.push_back("");
      finish_var_to_builder_codes_list_.push_back("");
      finish_var_to_array_codes_list_.push_back("");
      finish_var_array_codes_list_.push_back("");
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class SumActionCodeGen : public ActionCodeGen {
 public:
  SumActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      data_type = _type;
      projector_expr_ = projector;
    } else {
      data_type = input_fields_list[0]->type();
    }

    // Since sum may overflow original data type, we need to calculate its
    // accumulateType here
    std::shared_ptr<arrow::DataType> res_data_type;
    switch (data_type->id()) {
#define PROCESS(InType)                                            \
  case InType::type_id: {                                          \
    using AggrType = typename FindAccumulatorType<InType>::Type;   \
    res_data_type = arrow::TypeTraits<AggrType>::type_singleton(); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "Not Found " << data_type->ToString() << ", type id is "
                  << data_type->id() << std::endl;
      } break;
    }

    produce_ = [this, data_type, res_data_type, input_list](std::string name) {
      std::string sig_name;
      std::string validity_name;
      if (projector_expr_) {
        sig_name = "action_sum_projected_" + name + "_";
        validity_name = "action_sum_projected_" + name + "_validity_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_sum_" + name + "_";
        validity_name = "action_sum_" + name + "_validity_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }
      typed_input_and_prepare_list_.push_back(std::make_pair(
          "", ""));  // when there is two name in sig list, we need to make others aligned
      func_sig_list_.push_back(sig_name);
      func_sig_list_.push_back(validity_name);
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;

      } else {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;

      std::stringstream on_exists_codes_ss;
      on_exists_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
      on_exists_codes_ss << validity_name << "[i] = true;" << std::endl;
      on_exists_codes_ss << "}" << std::endl;
      std::stringstream on_new_codes_ss;
      on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
      on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
      on_new_codes_ss << "} else {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
      on_new_codes_ss << "}" << std::endl;

      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(res_data_type, sig_name) + ";\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
      on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
      // round sum result to 10 decimal places
      on_finish_codes_list_.push_back(sig_name + "[i] = round(" + sig_name 
                                      + "[i] * 10000000000) / 10000000000;\n");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_variable_list_.push_back(validity_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(res_data_type, sig_name + "_vector_tmp", true));
      finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
          arrow::boolean(), validity_name + "_vector_tmp", true));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(res_data_type, sig_name, true));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(res_data_type, sig_name, true));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(res_data_type, sig_name, true));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(res_data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(res_data_type, sig_name));
      finish_var_define_codes_list_.push_back("");
      finish_var_prepare_codes_list_.push_back("");
      finish_var_to_builder_codes_list_.push_back("");
      finish_var_to_array_codes_list_.push_back("");
      finish_var_array_codes_list_.push_back("");
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class CountActionCodeGen : public ActionCodeGen {
 public:
  CountActionCodeGen(std::string name, std::vector<std::string> child_list,
                     std::vector<std::string> input_list,
                     std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                     std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      data_type = _type;
      projector_expr_ = projector;
    } else {
      data_type = input_fields_list[0]->type();
    }

    // Calculate accumulateType here
    std::shared_ptr<arrow::DataType> res_data_type;
    switch (data_type->id()) {
#define PROCESS(InType)                                            \
  case InType::type_id: {                                          \
    using AggrType = typename FindAccumulatorType<InType>::Type;   \
    res_data_type = arrow::TypeTraits<AggrType>::type_singleton(); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "Not Found " << data_type->ToString() << ", type id is "
                  << data_type->id() << std::endl;
      } break;
    }

    produce_ = [this, data_type, res_data_type, input_list](std::string name) {
      std::string sig_name;
      if (projector_expr_) {
        sig_name = "action_count_projected_" + name + "_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_count_" + name + "_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }
      func_sig_list_.push_back(sig_name);
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
      std::stringstream prepare_codes_ss;
      auto count_name_tmp = tmp_name + "_count";
      prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                       << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
      prepare_codes_ss << "}" << std::endl;

      std::stringstream on_exists_codes_ss;
      on_exists_codes_ss << sig_name << "[i] += " << count_name_tmp << ";" << std::endl;
      std::stringstream on_new_codes_ss;
      on_new_codes_ss << sig_name << ".push_back(" << count_name_tmp << ");" << std::endl;

      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(res_data_type, sig_name) + ";\n");
      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back("");
      on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" +
                                      on_exists_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + on_new_codes_ss.str() +
                                   "\n");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(res_data_type, sig_name + "_vector_tmp", true));

      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(res_data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(res_data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(res_data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(res_data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(res_data_type, sig_name));
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class CountLiteralActionCodeGen : public ActionCodeGen {
 public:
  CountLiteralActionCodeGen(std::string name, int arg,
                            std::vector<std::string> child_list,
                            std::vector<std::string> input_list,
                            std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                            std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_countLiteral_" + name + "_";
    auto data_type = arrow::int64();
    auto tmp_name = sig_name + "_tmp";
    func_sig_list_.push_back(sig_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back(sig_name + "[i] += " + std::to_string(arg) + ";");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + std::to_string(arg) + ");");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
  }
};

class SumCountActionCodeGen : public ActionCodeGen {
 public:
  SumCountActionCodeGen(std::string name, std::vector<std::string> child_list,
                        std::vector<std::string> input_list,
                        std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                        std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }

    produce_ = [this, in_data_type, input_list](std::string name) {
      auto data_type = in_data_type;
      std::string sig_name;
      std::string validity_name;
      if (projector_expr_) {
        sig_name = "action_sum_projected_" + name + "_";
        validity_name = "action_sum_projected_" + name + "_validity_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_sum_" + name + "_";
        validity_name = "action_sum_" + name + "_validity_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_list_.push_back(sig_name);
      func_sig_list_.push_back(validity_name);
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;

      } else {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;
      std::stringstream on_exists_codes_ss;
      on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
      std::stringstream on_new_codes_ss;
      on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
      on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
      on_new_codes_ss << "} else {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
      on_new_codes_ss << "}" << std::endl;

      // FIXME: spark expect double as sum result type, quick fix here
      data_type = arrow::float64();
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
      on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
      on_finish_codes_list_.push_back("");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_variable_list_.push_back(validity_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
      finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
          arrow::boolean(), validity_name + "_vector_tmp", true));

      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      finish_var_define_codes_list_.push_back("");
      finish_var_prepare_codes_list_.push_back("");
      finish_var_to_builder_codes_list_.push_back("");
      finish_var_to_array_codes_list_.push_back("");
      finish_var_array_codes_list_.push_back("");

      sig_name = "action_count_" + name + "_";
      data_type = arrow::int64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      prepare_codes_ss.str("");
      auto count_name_tmp = tmp_name + "_count";
      prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                       << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
      prepare_codes_ss << "}" << std::endl;

      std::stringstream on_exists_count_codes_ss;
      on_exists_count_codes_ss << sig_name << "[i] += " << count_name_tmp << ";"
                               << std::endl;
      std::stringstream on_new_count_codes_ss;
      on_new_count_codes_ss << sig_name << ".push_back(" << count_name_tmp << ");"
                            << std::endl;

      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");

      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back("");
      on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" +
                                      on_exists_count_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" +
                                   on_new_count_codes_ss.str() + "\n");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
    };
    if (!projector) {
      produce_(name);
    }
  }
  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class AvgByCountActionCodeGen : public ActionCodeGen {
 public:
  AvgByCountActionCodeGen(std::string name, std::vector<std::string> child_list,
                          std::vector<std::string> input_list,
                          std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                          std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_sum_" + name + "_";
    auto validity_name = "action_sum_" + name + "_validity_";

    auto data_type = input_fields_list[0]->type();
    GetTypedArrayCastString(data_type, input_list[0]);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));

    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);

    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;
    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    std::stringstream on_exists_codes_ss;
    on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
    std::stringstream on_new_codes_ss;
    on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
    on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
    on_new_codes_ss << "} else {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
    on_new_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
    on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");

    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");

    sig_name = "action_count_" + name + "_";
    validity_name = "action_count_" + name + "_validity_";
    data_type = input_fields_list[1]->type();
    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);
    GetTypedArrayCastString(data_type, input_list[1]);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));

    tmp_name = typed_input_and_prepare_list_[2].first + "_tmp";
    prepare_codes_ss.str("");
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[2].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    on_exists_codes_ss.str("");
    on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
    on_new_codes_ss.str("");
    on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
    on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
    on_new_codes_ss << "} else {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
    on_new_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
    on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");

    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");

    sig_name = "action_avg_" + name + "_";
    validity_name = "action_avg_" + name + "_validity_";
    auto sum_name = "action_sum_" + name + "_";
    auto sum_name_validity = "action_sum_" + name + "_validity_";
    auto count_name = "action_count_" + name + "_";
    auto count_name_validity = "action_count_" + name + "_validity_";
    data_type = arrow::float64();
    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");

    std::stringstream on_finish_codes_ss;
    on_finish_codes_ss << "if ( " << count_name << "[i] > 0 ) {" << std::endl;
    on_finish_codes_ss << sig_name << ".push_back( " << sum_name << "[i] / " << count_name
                       << "[i]);" << std::endl;
    on_finish_codes_ss << validity_name << ".push_back(true);" << std::endl;
    on_finish_codes_ss << "} else {" << std::endl;
    on_finish_codes_ss << sig_name << ".push_back(0);" << std::endl;
    on_finish_codes_ss << validity_name << ".push_back(false);" << std::endl;
    on_finish_codes_ss << "}" << std::endl;
    on_finish_codes_list_.push_back(on_finish_codes_ss.str());
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_variable_list_.push_back(validity_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
    finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
        arrow::boolean(), validity_name + "_vector_tmp", true));

    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name, true));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
    finish_var_define_codes_list_.push_back("");
    finish_var_prepare_codes_list_.push_back("");
    finish_var_to_builder_codes_list_.push_back("");
    finish_var_to_array_codes_list_.push_back("");
    finish_var_array_codes_list_.push_back("");
  }
};

class MaxActionCodeGen : public ActionCodeGen {
 public:
  MaxActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }

    produce_ = [this, in_data_type, input_list](std::string name) {
      auto data_type = in_data_type;
      std::string sig_name;
      std::string validity_name;
      if (projector_expr_) {
        sig_name = "action_max_projected_" + name + "_";
        validity_name = "action_max_projected_" + name + "_validity_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_max_" + name + "_";
        validity_name = "action_max_" + name + "_validity_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }

      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_list_.push_back(sig_name);
      func_sig_list_.push_back(validity_name);
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;

      } else {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;

      std::stringstream on_exists_codes_ss;
      on_exists_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_exists_codes_ss << sig_name << "[i] = " << sig_name << "[i] > " << tmp_name
                         << "?" << sig_name + "[i]:" << tmp_name << ";" << std::endl;
      on_exists_codes_ss << validity_name << "[i] = true;" << std::endl;
      on_exists_codes_ss << "}" << std::endl;
      std::stringstream on_new_codes_ss;
      on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
      on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
      on_new_codes_ss << "} else {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
      on_new_codes_ss << "}" << std::endl;

      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
      on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
      on_finish_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_variable_list_.push_back(validity_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
      finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
          arrow::boolean(), validity_name + "_vector_tmp", true));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name, true));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      finish_var_define_codes_list_.push_back("");
      finish_var_prepare_codes_list_.push_back("");
      finish_var_to_builder_codes_list_.push_back("");
      finish_var_to_array_codes_list_.push_back("");
      finish_var_array_codes_list_.push_back("");
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class MinActionCodeGen : public ActionCodeGen {
 public:
  MinActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }

    produce_ = [this, in_data_type, input_list](std::string name) {
      auto data_type = in_data_type;
      std::string sig_name;
      std::string validity_name;
      if (projector_expr_) {
        sig_name = "action_min_projected_" + name + "_";
        validity_name = "action_min_projected_" + name + "_validity_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_min_" + name + "_";
        validity_name = "action_min_" + name + "_validity_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }

      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_list_.push_back(sig_name);
      func_sig_list_.push_back(validity_name);

      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;

      } else {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;

      std::stringstream on_exists_codes_ss;
      on_exists_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_exists_codes_ss << sig_name << "[i] = " << sig_name << "[i] < " << tmp_name
                         << "?" << sig_name << "[i]:" << tmp_name << ";" << std::endl;
      on_exists_codes_ss << validity_name << "[i] = true;" << std::endl;
      on_exists_codes_ss << "}" << std::endl;
      std::stringstream on_new_codes_ss;
      on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
      on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
      on_new_codes_ss << "} else {" << std::endl;
      on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
      on_new_codes_ss << "}" << std::endl;

      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");

      on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
      on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
      on_finish_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_variable_list_.push_back(validity_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
      finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
          arrow::boolean(), validity_name + "_vector_tmp", true));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name, true));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      finish_var_define_codes_list_.push_back("");
      finish_var_prepare_codes_list_.push_back("");
      finish_var_to_builder_codes_list_.push_back("");
      finish_var_to_array_codes_list_.push_back("");
      finish_var_array_codes_list_.push_back("");
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class AvgActionCodeGen : public ActionCodeGen {
 public:
  AvgActionCodeGen(std::string name, std::vector<std::string> child_list,
                   std::vector<std::string> input_list,
                   std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                   std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }

    produce_ = [this, in_data_type, input_list](std::string name) {
      auto data_type = in_data_type;
      std::string sig_name;
      if (projector_expr_) {
        sig_name = "action_sum_projected_" + name + "_";
        GetTypedArrayCastFromProjectedString(data_type, name);

      } else {
        sig_name = "action_sum_" + name + "_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }
      func_sig_list_.push_back(sig_name);
      auto tmp_name = sig_name + "_tmp";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;

      } else {
        prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "}" << std::endl;

      on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_codes_list_.push_back(sig_name + "[i] += " + tmp_name + ";");
      on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");

      sig_name = "action_count_" + name + "_";
      data_type = arrow::int64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      prepare_codes_ss.str("");
      auto count_name_tmp = tmp_name + "_count";
      prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                       << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
      prepare_codes_ss << "}" << std::endl;
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back("");
      on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                      "[i] += " + count_name_tmp + ";");
      on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                   ".push_back(" + count_name_tmp + ");");

      sig_name = "action_avg_" + name + "_";
      auto sum_name = "action_sum_" + name + "_";
      auto count_name = "action_count_" + name + "_";
      data_type = arrow::float64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back(sig_name + ".push_back(" + sum_name + "[i] / " +
                                      count_name + "[i]);");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
    };
    if (!projector) {
      produce_(name);
    }
  }

  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class SumCountMergeActionCodeGen : public ActionCodeGen {
 public:
  SumCountMergeActionCodeGen(std::string name, std::vector<std::string> child_list,
                             std::vector<std::string> input_list,
                             std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
                             std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    auto sig_name = "action_sum_" + name + "_";
    auto validity_name = "action_sum_" + name + "_validity_";
    auto data_type = input_fields_list[0]->type();
    auto sum_data_type = data_type;

    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);

    GetTypedArrayCastString(data_type, input_list[0]);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));

    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    std::stringstream on_exists_codes_ss;
    on_exists_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
    on_exists_codes_ss << validity_name << "[i] = true;" << std::endl;
    on_exists_codes_ss << "}" << std::endl;
    std::stringstream on_new_codes_ss;
    on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
    on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
    on_new_codes_ss << "} else {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
    on_new_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
    on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
    on_finish_codes_list_.push_back("");

    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_variable_list_.push_back(validity_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
    finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
        arrow::boolean(), validity_name + "_vector_tmp", true));

    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name, true));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
    finish_var_define_codes_list_.push_back("");
    finish_var_prepare_codes_list_.push_back("");
    finish_var_to_builder_codes_list_.push_back("");
    finish_var_to_array_codes_list_.push_back("");
    finish_var_array_codes_list_.push_back("");

    sig_name = "action_count_" + name + "_";
    validity_name = "action_count_" + name + "_validity_";
    data_type = input_fields_list[1]->type();
    auto count_data_type = data_type;

    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);

    GetTypedArrayCastString(data_type, input_list[1]);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));

    tmp_name = typed_input_and_prepare_list_[2].first + "_tmp";
    prepare_codes_ss.str("");
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool " << tmp_name << "_validity = false;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[2].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    prepare_codes_ss << tmp_name << "_validity = true;" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetView(cur_id_);" << std::endl;

    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "}" << std::endl;

    on_exists_codes_ss.str("");
    on_exists_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_exists_codes_ss << sig_name << "[i] += " << tmp_name << ";" << std::endl;
    on_exists_codes_ss << validity_name << "[i] = true;" << std::endl;
    on_exists_codes_ss << "}" << std::endl;
    on_new_codes_ss.str("");
    on_new_codes_ss << sig_name << ".push_back(" << tmp_name << ");" << std::endl;
    on_new_codes_ss << "if ( " << tmp_name << "_validity ) {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(true);" << std::endl;
    on_new_codes_ss << "} else {" << std::endl;
    on_new_codes_ss << validity_name << ".push_back(false);" << std::endl;
    on_new_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(on_exists_codes_ss.str() + "\n");
    on_new_codes_list_.push_back(on_new_codes_ss.str() + "\n");
    on_finish_codes_list_.push_back("");
    on_exists_prepare_codes_list_.push_back("");
    on_new_prepare_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_variable_list_.push_back(validity_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
    finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
        arrow::boolean(), validity_name + "_vector_tmp", true));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name, true));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
    finish_var_define_codes_list_.push_back("");
    finish_var_prepare_codes_list_.push_back("");
    finish_var_to_builder_codes_list_.push_back("");
    finish_var_to_array_codes_list_.push_back("");
    finish_var_array_codes_list_.push_back("");
  }
};

class StddevSampPartialActionCodeGen : public ActionCodeGen {
 public:
  StddevSampPartialActionCodeGen(
      std::string name, std::vector<std::string> child_list,
      std::vector<std::string> input_list,
      std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
      std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::shared_ptr<arrow::DataType> in_data_type;
    if (projector) {
      // if projection pre-defined, use projection input
      auto _type = projector->result()->type();
      in_data_type = _type;
      projector_expr_ = projector;
    } else {
      in_data_type = input_fields_list[0]->type();
    }
    produce_ = [this, in_data_type, input_list](std::string name) {
      auto data_type = in_data_type;
      std::string std_name;
      std::string sig_name;
      if (projector_expr_) {
        std_name = "action_std_projected_" + name + "_";
        GetTypedArrayCastFromProjectedString(data_type, name);
      } else {
        std_name = "action_std_" + name + "_";
        GetTypedArrayCastString(data_type, input_list[0]);
      }
      auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp" + name;
      ///////////////////////////// Std //////////////////////////////////
      sig_name = std_name;
      auto count_name = "action_n_" + name + "_";
      auto avg_name = "action_avg_" + name + "_";
      auto m2_name = "action_m2_" + name + "_";
      func_sig_list_.push_back(sig_name);
      auto sum_tmp_name = tmp_name + "_sum";
      std::stringstream prepare_codes_ss;
      prepare_codes_ss << GetCTypeString(data_type) << " " << sum_tmp_name << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool is_null_" << sum_tmp_name << " = true;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      if (data_type->id() != arrow::Type::STRING) {
        prepare_codes_ss << sum_tmp_name << " = "
                         << typed_input_and_prepare_list_[0].first
                         << "->GetView(cur_id_);" << std::endl;
      } else {
        prepare_codes_ss << sum_tmp_name << " = "
                         << typed_input_and_prepare_list_[0].first
                         << "->GetString(cur_id_);" << std::endl;
      }
      prepare_codes_ss << "is_null_" << sum_tmp_name << " = false;" << std::endl;
      prepare_codes_ss << "}" << std::endl;
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back("");
      on_exists_codes_list_.push_back(
          prepare_codes_ss.str() + "\n" + "double pre_avg_" + name + " = " + sig_name +
          "[i] * 1.0 / (" + count_name + "[i] > 0 ? " + count_name + "[i] : 1);\n" +
          "double delta_" + name + " = " + sum_tmp_name + " * 1.0 - pre_avg_" + name +
          ";\n double deltaN_" + name + " = delta_" + name + " / (" + count_name +
          "[i] + 1);\n" + "if (!is_null_" + sum_tmp_name + ") {\n" + m2_name +
          "[i] += delta_" + name + "* deltaN_" + name + " * " + count_name + "[i];\n" +
          sig_name + "[i] += " + sum_tmp_name + ";\n }\n");
      on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                   ".push_back(" + sum_tmp_name + ");\n" +
                                   "double stddev_" + name + " = 0;\n" + m2_name +
                                   ".push_back(stddev_" + name + ");\n");
      ///////////////////////////// Count //////////////////////////////////
      sig_name = count_name;
      data_type = arrow::float64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      prepare_codes_ss.str("");
      auto count_name_tmp = tmp_name + "_n";
      prepare_codes_ss << GetCTypeString(data_type) << " " << count_name_tmp << " = 0;"
                       << std::endl;
      prepare_codes_ss << "bool is_null_" << count_name_tmp << " = true;" << std::endl;
      prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                       << "->IsNull(cur_id_)) {" << std::endl;
      prepare_codes_ss << count_name_tmp << " = 1;" << std::endl;
      prepare_codes_ss << "is_null_" << count_name_tmp << " = false;" << std::endl;
      prepare_codes_ss << "}" << std::endl;
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_prepare_codes_list_.push_back("");
      on_new_prepare_codes_list_.push_back("");
      on_exists_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                      "[i] += " + count_name_tmp + ";");
      on_new_codes_list_.push_back(prepare_codes_ss.str() + "\n" + sig_name +
                                   ".push_back(" + count_name_tmp + ");");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      ///////////////////////////// Avg //////////////////////////////////
      sig_name = avg_name;
      data_type = arrow::float64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back(sig_name + ".push_back(" + count_name +
                                      "[i] > 0 ?" + std_name + "[i] * 1.0 / " +
                                      count_name + "[i] : 0.0);\n");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
      ///////////////////////////// M2 //////////////////////////////////
      sig_name = m2_name;
      data_type = arrow::float64();
      func_sig_list_.push_back(sig_name);
      typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
      func_sig_define_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name) + ";\n");
      on_exists_codes_list_.push_back("");
      on_new_codes_list_.push_back("");
      on_finish_codes_list_.push_back("");

      finish_variable_list_.push_back(sig_name);
      finish_var_parameter_codes_list_.push_back(
          GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp"));
      finish_var_define_codes_list_.push_back(
          GetTypedVectorAndBuilderDefineString(data_type, sig_name));
      finish_var_prepare_codes_list_.push_back(
          GetTypedVectorAndBuilderPrepareString(data_type, sig_name));
      finish_var_to_builder_codes_list_.push_back(
          GetTypedVectorToBuilderString(data_type, sig_name));
      finish_var_to_array_codes_list_.push_back(
          GetTypedResultToArrayString(data_type, sig_name));
      finish_var_array_codes_list_.push_back(
          GetTypedResultArrayString(data_type, sig_name));
    };
    if (!projector) {
      produce_(name);
    }
  }
  arrow::Status WithProjectIndex(int index) override {
    produce_(std::to_string(index));
    return arrow::Status::OK();
  }

 private:
  std::function<void(std::string)> produce_;
};

class StddevSampFinalActionCodeGen : public ActionCodeGen {
 public:
  StddevSampFinalActionCodeGen(
      std::string name, std::vector<std::string> child_list,
      std::vector<std::string> input_list,
      std::vector<std::shared_ptr<arrow::Field>> input_fields_list,
      std::shared_ptr<gandiva::Expression> projector) {
    is_key_ = false;
    std::string avg_name = "action_avg_" + name + "_";
    std::string m2_name = "action_m2_" + name + "_";
    std::string count_name = "action_n_" + name + "_";
    ///////////////////////////// Count //////////////////////////////////
    std::string sig_name = count_name;
    auto data_type = input_fields_list[0]->type();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[0]);
    auto tmp_name = typed_input_and_prepare_list_[0].first + "_tmp";
    std::stringstream prepare_codes_ss;
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool is_null_" << tmp_name << " = true;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[0].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetView(cur_id_);" << std::endl;
    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[0].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "is_null_" << tmp_name << " = false;" << std::endl;
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back("double pre_count_" + name + " = " + sig_name +
                                    "[i] * 1.0;\n" + "double new_count_" + name + " = " +
                                    tmp_name + " * 1.0;\n" + "if(!is_null_" + tmp_name +
                                    ") {\n" + sig_name + "[i] += " + tmp_name + ";\n}\n");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    ///////////////////////////// Avg //////////////////////////////////
    sig_name = avg_name;
    data_type = input_fields_list[1]->type();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[1]);
    tmp_name = typed_input_and_prepare_list_[1].first + "_tmp";
    prepare_codes_ss.str("");
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool is_null_" << tmp_name << " = true;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[1].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[1].first
                       << "->GetView(cur_id_);" << std::endl;
    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[1].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "is_null_" << tmp_name << " = false;" << std::endl;
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back(
        "double delta_" + name + " = " + tmp_name + " - " + sig_name + "[i];\n" +
        "double deltaN_" + name + " = " + count_name + "[i] > 0 ? delta_" + name + " / " +
        count_name + "[i] : 0;\n" + "if(!is_null_" + tmp_name + ") {\n" + sig_name +
        "[i] += deltaN_" + name + " * new_count_" + name + ";\n}\n");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    ///////////////////////////// M2 //////////////////////////////////
    sig_name = m2_name;
    data_type = input_fields_list[2]->type();
    func_sig_list_.push_back(sig_name);
    GetTypedArrayCastString(data_type, input_list[2]);
    tmp_name = typed_input_and_prepare_list_[2].first + "_tmp";
    prepare_codes_ss.str("");
    prepare_codes_ss << GetCTypeString(data_type) << " " << tmp_name << " = 0;"
                     << std::endl;
    prepare_codes_ss << "bool is_null_" << tmp_name << " = true;" << std::endl;
    prepare_codes_ss << "if (!" << typed_input_and_prepare_list_[2].first
                     << "->IsNull(cur_id_)) {" << std::endl;
    if (data_type->id() != arrow::Type::STRING) {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetView(cur_id_);" << std::endl;
    } else {
      prepare_codes_ss << tmp_name << " = " << typed_input_and_prepare_list_[2].first
                       << "->GetString(cur_id_);" << std::endl;
    }
    prepare_codes_ss << "is_null_" << tmp_name << " = false;" << std::endl;
    prepare_codes_ss << "}" << std::endl;

    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    on_exists_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_new_prepare_codes_list_.push_back(prepare_codes_ss.str() + "\n");
    on_exists_codes_list_.push_back("if(!is_null_" + tmp_name + ") {\n" + sig_name +
                                    "[i] += (" + tmp_name + " + delta_" + name +
                                    " * deltaN_" + name + " * pre_count_" + name +
                                    " * new_count_" + name + ");\n}\n");
    on_new_codes_list_.push_back(sig_name + ".push_back(" + tmp_name + ");");
    ///////////////////////////// Stddev //////////////////////////////////
    sig_name = "action_stddev_" + name + "_";
    auto validity_name = "action_stddev_" + name + "_validity_";
    data_type = arrow::float64();
    func_sig_list_.push_back(sig_name);
    func_sig_list_.push_back(validity_name);
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    typed_input_and_prepare_list_.push_back(std::make_pair("", ""));
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name) + ";\n");
    func_sig_define_codes_list_.push_back(
        GetTypedVectorDefineString(arrow::boolean(), validity_name) + ";\n");
    on_exists_codes_list_.push_back("");
    on_exists_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_new_codes_list_.push_back("");
    on_finish_codes_list_.push_back(
        "if (" + count_name + "[i] - 1 < 0.00001) {\n" + validity_name +
        ".push_back(true);\n"
        // + sig_name + ".push_back(std::numeric_limits<double>::quiet_NaN());}\n"
        + sig_name + ".push_back(std::numeric_limits<double>::infinity());}\n" +
        "else if (" + count_name + "[i] < 0.00001) {\n" + validity_name +
        ".push_back(false);\n" + sig_name + ".push_back(0);}\n" + "else {\n" +
        validity_name + ".push_back(true);\n" + sig_name + ".push_back(" + "sqrt(" +
        m2_name + "[i] / (" + count_name + "[i] - 1)));}\n");
    on_finish_codes_list_.push_back("");

    finish_variable_list_.push_back(sig_name);
    finish_variable_list_.push_back(validity_name);
    finish_var_parameter_codes_list_.push_back(
        GetTypedVectorDefineString(data_type, sig_name + "_vector_tmp", true));
    finish_var_parameter_codes_list_.push_back(GetTypedVectorDefineString(
        arrow::boolean(), validity_name + "_vector_tmp", true));
    finish_var_define_codes_list_.push_back(
        GetTypedVectorAndBuilderDefineString(data_type, sig_name, true));
    finish_var_prepare_codes_list_.push_back(
        GetTypedVectorAndBuilderPrepareString(data_type, sig_name, true));
    finish_var_to_builder_codes_list_.push_back(
        GetTypedVectorToBuilderString(data_type, sig_name, true));
    finish_var_to_array_codes_list_.push_back(
        GetTypedResultToArrayString(data_type, sig_name));
    finish_var_array_codes_list_.push_back(
        GetTypedResultArrayString(data_type, sig_name));
    finish_var_define_codes_list_.push_back("");
    finish_var_prepare_codes_list_.push_back("");
    finish_var_to_builder_codes_list_.push_back("");
    finish_var_to_array_codes_list_.push_back("");
    finish_var_array_codes_list_.push_back("");
  }
};

#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin