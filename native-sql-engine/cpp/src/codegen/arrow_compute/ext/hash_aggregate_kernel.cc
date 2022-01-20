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

#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/actions_impl.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/codegen_register.h"
#include "codegen/arrow_compute/ext/expression_codegen_visitor.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_action_codegen_impl.h"
#include "precompile/gandiva_projector.h"
#include "precompile/hash_map.h"
#include "precompile/sparse_hash_map.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using precompile::StringHashMap;

///////////////  HashAgg Kernel  ////////////////
class HashAggregateKernel::Impl {
 public:
  Impl(arrow::compute::ExecContext* ctx,
       std::vector<std::shared_ptr<gandiva::Node>> input_field_list,
       std::vector<std::shared_ptr<gandiva::Node>> action_list,
       std::vector<std::shared_ptr<gandiva::Node>> result_field_node_list,
       std::vector<std::shared_ptr<gandiva::Node>> result_expr_node_list)
      : ctx_(ctx), action_list_(action_list) {
    // if there is projection inside aggregate, we need to extract them into
    // projector_list
    for (auto node : input_field_list) {
      input_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }

    // map action_list nodes to keys or other aggregate actions
    for (auto node : action_list_) {
      auto func_node = std::dynamic_pointer_cast<gandiva::FunctionNode>(node);
      auto func_name = func_node->descriptor()->name();
      std::shared_ptr<arrow::DataType> type;
      if (func_node->children().size() > 0) {
        if (func_name.compare(0, 24, "action_stddev_samp_final") == 0) {
          type = func_node->children()[1]->return_type();
        } else {
          type = func_node->children()[0]->return_type();
        }
      } else {
        type = func_node->return_type();
      }
      if (func_name.compare(0, 7, "action_") == 0) {
        action_name_list_.push_back(std::make_pair(func_name, type));
        std::vector<int> child_prepare_idxs;
        if (func_name.compare(0, 20, "action_countLiteral_") == 0) {
          action_prepare_index_list_.push_back(child_prepare_idxs);
          continue;
        }
        for (auto child_node : func_node->children()) {
          bool found = false;
          for (int i = 0; i < prepare_function_list_.size(); i++) {
            auto tmp_node = prepare_function_list_[i];
            if (tmp_node->ToString() == child_node->ToString()) {
              child_prepare_idxs.push_back(i);
              if (func_name == "action_groupby") {
                key_index_list_.push_back(i);
                key_node_list_.push_back(child_node);
              }
              found = true;
              break;
            }
          }
          if (!found) {
            if (func_name == "action_groupby") {
              key_index_list_.push_back(prepare_function_list_.size());
              key_node_list_.push_back(child_node);
            }
            child_prepare_idxs.push_back(prepare_function_list_.size());
            prepare_function_list_.push_back(child_node);
          }
        }
        action_prepare_index_list_.push_back(child_prepare_idxs);
      } else {
        THROW_NOT_OK(arrow::Status::Invalid("Expected some with action_ prefix."));
      }
    }

    for (auto node : result_field_node_list) {
      result_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }
    if (result_field_node_list.size() == result_expr_node_list.size()) {
      auto tmp_size = result_field_node_list.size();
      bool no_result_project = true;
      for (int i = 0; i < tmp_size; i++) {
        if (result_field_node_list[i]->ToString() !=
            result_expr_node_list[i]->ToString()) {
          no_result_project = false;
          break;
        }
      }
      if (no_result_project) return;
    }
    result_expr_list_ = result_expr_node_list;
    pool_ = nullptr;
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    // 1. create pre project
    std::shared_ptr<GandivaProjector> pre_process_projector;
    if (!prepare_function_list_.empty()) {
      auto pre_process_expr_list = GetGandivaKernel(prepare_function_list_);
      pre_process_projector = std::make_shared<GandivaProjector>(
          ctx_, arrow::schema(input_field_list_), pre_process_expr_list);
    }

    // 2. action_impl_list
    std::vector<std::shared_ptr<ActionBase>> action_impl_list;
    gandiva::DataTypeVector res_type_list;
    for (auto field : result_field_list_) {
      res_type_list.push_back(field->type());
    }
    RETURN_NOT_OK(PrepareActionList(action_name_list_, res_type_list, &action_impl_list));

    // 3. create post project
    std::shared_ptr<GandivaProjector> post_process_projector;
    if (!result_expr_list_.empty()) {
      auto post_process_expr_list = GetGandivaKernel(result_expr_list_);
      post_process_projector = std::make_shared<GandivaProjector>(
          ctx_, arrow::schema(result_field_list_), post_process_expr_list);
    }

    auto result_schema = schema;
    // 4. create ResultIterator
    if (key_node_list_.size() > 1 ||
        (key_node_list_.size() > 0 &&
         key_node_list_[0]->return_type()->id() == arrow::Type::STRING)) {
      *out = std::make_shared<HashAggregateResultIterator<arrow::StringType>>(
          ctx_, result_schema, key_index_list_, action_prepare_index_list_,
          pre_process_projector, post_process_projector, action_impl_list);
    } else {
      auto type = arrow::int32();
      if (key_node_list_.size() > 0) type = key_node_list_[0]->return_type();
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::Decimal128Type)
      switch (type->id()) {
#define PROCESS(InType)                                                   \
  case TypeTraits<InType>::type_id: {                                     \
    *out = std::make_shared<HashAggregateResultIterator<InType>>(         \
        ctx_, result_schema, key_index_list_, action_prepare_index_list_, \
        pre_process_projector, post_process_projector, action_impl_list); \
  } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
        default: {
          return arrow::Status::NotImplemented(
              "HashAggregateResultIterator doesn't support type ", type->ToString());
        } break;
      }
#undef PROCESS_SUPPORTED_TYPES
    }

    return arrow::Status::OK();
  }

  std::string GetSignature() { return ""; }

  arrow::Status DoCodeGen(
      int level,
      std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
          input,
      std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
    auto codegen_ctx = std::make_shared<CodeGenContext>();

    std::vector<std::string> prepare_list;
    // 1.0 prepare aggregate input expressions
    std::stringstream prepare_ss;
    std::stringstream define_ss;
    std::stringstream aggr_prepare_ss;
    std::stringstream finish_ss;
    std::stringstream finish_condition_ss;
    std::stringstream process_ss;
    std::stringstream action_list_define_function_ss;
    std::vector<std::pair<std::string, gandiva::DataTypePtr>> action_name_list =
        action_name_list_;
    std::vector<std::vector<int>> action_prepare_index_list = action_prepare_index_list_;

    std::vector<int> key_index_list = key_index_list_;
    std::vector<gandiva::NodePtr> prepare_function_list = prepare_function_list_;
    std::vector<gandiva::NodePtr> key_node_list = key_node_list_;
    std::vector<gandiva::FieldPtr> key_hash_field_list;
    std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
        project_output_list;

    // 1. Get action list and action_prepare_project_list
    if (key_node_list.size() > 0 &&
        key_node_list[0]->return_type()->id() == arrow::Type::DECIMAL128) {
      aggr_prepare_ss << "aggr_hash_table_" << level << " = std::make_shared<"
                      << GetTypeString(key_node_list[0]->return_type(), "")
                      << "HashMap>(ctx_->memory_pool());" << std::endl;
      define_ss << "std::shared_ptr<"
                << GetTypeString(key_node_list[0]->return_type(), "")
                << "HashMap> aggr_hash_table_" << level << ";" << std::endl;

    } else if (key_node_list.size() > 1 ||
               (key_node_list.size() > 0 &&
                key_node_list[0]->return_type()->id() == arrow::Type::STRING)) {
      aggr_prepare_ss << "aggr_hash_table_" << level << " = std::make_shared<"
                      << GetTypeString(arrow::utf8(), "")
                      << "HashMap>(ctx_->memory_pool());" << std::endl;
      define_ss << "std::shared_ptr<" << GetTypeString(arrow::utf8(), "")
                << "HashMap> aggr_hash_table_" << level << ";" << std::endl;

    } else if (key_node_list.size() > 0) {
      auto type = key_node_list[0]->return_type();

      aggr_prepare_ss << "aggr_hash_table_" << level << " = std::make_shared<"
                      << "SparseHashMap<" << GetCTypeString(type)
                      << ">>(ctx_->memory_pool());" << std::endl;
      define_ss << "std::shared_ptr<SparseHashMap<" << GetCTypeString(type)
                << ">> aggr_hash_table_" << level << ";" << std::endl;
    }
    // 2. create key_hash_project_node and prepare_gandiva_project_node_list

    // 3. create cpp codes prepare_project
    int idx = 0;
    for (auto node : prepare_function_list) {
      std::vector<std::string> input_list;
      std::shared_ptr<ExpressionCodegenVisitor> project_node_visitor;
      auto is_local = false;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(node, &input, {input_field_list_}, level,
                                                 var_id, is_local, &input_list,
                                                 &project_node_visitor, false));
      auto key_name = project_node_visitor->GetResult();
      auto validity_name = project_node_visitor->GetPreCheck();

      project_output_list.push_back(std::make_pair(
          std::make_pair(key_name, project_node_visitor->GetPrepare()), nullptr));
      for (auto header : project_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                      header) == codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }
      idx++;
    }

    // 4. create cpp codes for key_hash_project
    if (key_index_list.size() > 0) {
      auto unsafe_row_name = "aggr_key_" + std::to_string(level);
      auto unsafe_row_name_validity = unsafe_row_name + "_validity";
      if (key_index_list.size() == 1) {
        auto i = key_index_list[0];
        prepare_ss << project_output_list[i].first.second << std::endl;
        project_output_list[i].first.second = "";
        prepare_ss << "auto " << unsafe_row_name << " = "
                   << project_output_list[i].first.first << ";" << std::endl;
        prepare_ss << "auto " << unsafe_row_name_validity << " = "
                   << project_output_list[i].first.first << "_validity;" << std::endl;
      } else {
        std::stringstream unsafe_row_define_ss;
        unsafe_row_define_ss << "std::shared_ptr<UnsafeRow> " << unsafe_row_name
                             << "_unsafe_row = std::make_shared<UnsafeRow>("
                             << key_index_list.size() << ");" << std::endl;
        codegen_ctx->unsafe_row_prepare_codes = unsafe_row_define_ss.str();
        prepare_ss << "auto " << unsafe_row_name_validity << " = "
                   << "true;" << std::endl;
        prepare_ss << unsafe_row_name << "_unsafe_row->reset();" << std::endl;
        int idx = 0;
        for (auto i : key_index_list) {
          prepare_ss << project_output_list[i].first.second << std::endl;
          project_output_list[i].first.second = "";
          auto key_name = project_output_list[i].first.first;
          auto validity_name = key_name + "_validity";
          prepare_ss << "if (" << validity_name << ") {" << std::endl;
          prepare_ss << "appendToUnsafeRow(" << unsafe_row_name << "_unsafe_row.get(), "
                     << idx << ", " << key_name << ");" << std::endl;
          prepare_ss << "} else {" << std::endl;
          prepare_ss << "setNullAt(" << unsafe_row_name << "_unsafe_row.get(), " << idx
                     << ");" << std::endl;
          prepare_ss << "}" << std::endl;
          idx++;
        }
        prepare_ss << "auto " << unsafe_row_name << " = arrow::util::string_view("
                   << unsafe_row_name << "_unsafe_row->data, " << unsafe_row_name
                   << "_unsafe_row->cursor);" << std::endl;
      }
    }

    // 5. create codes for hash aggregate GetOrInsert
    std::vector<std::string> action_name_str_list;
    std::vector<std::string> action_type_str_list;
    for (auto action_pair : action_name_list) {
      action_name_str_list.push_back("\"" + action_pair.first + "\"");
      action_type_str_list.push_back("arrow::" +
                                     GetArrowTypeDefString(action_pair.second));
    }

    std::vector<std::string> res_type_str_list;
    for (auto field : result_field_list_) {
      res_type_str_list.push_back("arrow::" + GetArrowTypeDefString(field->type()));
    }
    define_ss << "std::vector<std::shared_ptr<ActionBase>> aggr_action_list_" << level
              << ";" << std::endl;
    define_ss << "bool do_hash_aggr_finish_" << level << " = false;" << std::endl;
    define_ss << "uint64_t do_hash_aggr_finish_" << level << "_offset = 0;" << std::endl;
    define_ss << "int do_hash_aggr_finish_" << level << "_num_groups = -1;" << std::endl;
    aggr_prepare_ss << "std::vector<std::string> action_name_list_" << level << " = {"
                    << GetParameterList(action_name_str_list, false) << "};" << std::endl;
    aggr_prepare_ss << "auto action_type_list_" << level << " = {"
                    << GetParameterList(action_type_str_list, false) << "};" << std::endl;
    aggr_prepare_ss << "auto res_type_list_" << level << " = {"
                    << GetParameterList(res_type_str_list, false) << "};" << std::endl;
    aggr_prepare_ss << "PrepareActionList(action_name_list_" << level
                    << ", action_type_list_" << level << ", res_type_list_" << level
                    << ", &aggr_action_list_" << level << ");" << std::endl;
    std::stringstream action_codes_ss;
    int action_idx = 0;
    for (auto idx_v : action_prepare_index_list) {
      for (auto i : idx_v) {
        action_codes_ss << project_output_list[i].first.second << std::endl;
        project_output_list[i].first.second = "";
      }
      if (idx_v.size() > 0) {
        if (action_name_str_list[action_idx] != "\"action_count\"") {
          action_codes_ss << "if (" << project_output_list[idx_v[0]].first.first
                          << "_validity) {" << std::endl;
        } else {
          // For action_count with mutiple-col input, will check the validity
          // of all the input cols.
          action_codes_ss << "if (" << project_output_list[idx_v[0]].first.first
                          << "_validity";
          for (int i = 1; i < idx_v.size() - 1; i++) {
            action_codes_ss << " && " << project_output_list[idx_v[i]].first.first
                            << "_validity";
          }
          action_codes_ss << " && "
                          << project_output_list[idx_v[idx_v.size() - 1]].first.first
                          << "_validity) {" << std::endl;
        }
      }
      std::vector<std::string> parameter_list;
      if (action_name_str_list[action_idx] != "\"action_count\"") {
        for (auto i : idx_v) {
          parameter_list.push_back("(void*)&" + project_output_list[i].first.first);
        }
      } else {
        // For action_count, only the first col will be used as input to Evaluate
        // function, in which it will not be used.
        parameter_list.push_back("(void*)&" + project_output_list[idx_v[0]].first.first);
      }
      action_codes_ss << "RETURN_NOT_OK(aggr_action_list_" << level << "[" << action_idx
                      << "]->Evaluate(memo_index" << GetParameterList(parameter_list)
                      << "));" << std::endl;
      if (idx_v.size() > 0) {
        action_codes_ss << "} else {" << std::endl;
        action_codes_ss << "RETURN_NOT_OK(aggr_action_list_" << level << "[" << action_idx
                        << "]->EvaluateNull(memo_index));" << std::endl;
        action_codes_ss << "}" << std::endl;
      }
      action_idx++;
    }
    process_ss << "int memo_index = 0;" << std::endl;

    if (key_index_list.size() > 0) {
      process_ss << "if (!aggr_key_" << level << "_validity) {" << std::endl;
      process_ss << "  memo_index = aggr_hash_table_" << level
                 << "->GetOrInsertNull([](int){}, [](int){});" << std::endl;
      process_ss << " } else {" << std::endl;
      process_ss << "   aggr_hash_table_" << level << "->GetOrInsert(aggr_key_" << level
                 << ",[](int){}, [](int){}, &memo_index);" << std::endl;
      process_ss << " }" << std::endl;
      process_ss << action_codes_ss.str() << std::endl;
      process_ss << "if (memo_index > do_hash_aggr_finish_" << level << "_num_groups) {"
                 << std::endl;
      process_ss << "do_hash_aggr_finish_" << level << "_num_groups = memo_index;"
                 << std::endl;
      process_ss << "}" << std::endl;
    } else {
      process_ss << action_codes_ss.str() << std::endl;
    }

    action_list_define_function_ss
        << "bool getActionOption(std::string action_name, std::string "
           "action_name_prefix) {\n"
        << "int prefix_size = action_name_prefix.size();\n"
        << "bool option = false;\n"
        << "if (action_name.size() > prefix_size &&\n"
        << "action_name.compare(0, prefix_size + 1, action_name_prefix + \"_\") == 0) {\n"
        << "auto lit = action_name.substr(prefix_size + 1);\n"
        << "option = (lit == \"true\") ? true : false;\n"
        << "}\n"
        << "return option;\n"
        << "}" << std::endl;
    action_list_define_function_ss
        << "arrow::Status PrepareActionList(std::vector<std::string> "
           "action_name_list, "
           "std::vector<std::shared_ptr<arrow::DataType>> type_list,"
           "std::vector<std::shared_ptr<arrow::DataType>> result_field_list,"
           "std::vector<std::shared_ptr<ActionBase>> *action_list) {"
        << std::endl;
    action_list_define_function_ss << R"(
    int type_id = 0;
    int result_id = 0;
    for (int action_id = 0; action_id < action_name_list.size(); action_id++) {
      std::shared_ptr<ActionBase> action;
      if (action_name_list[action_id].compare("action_groupby") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeUniqueAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_count") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeCountAction(ctx_, res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_sum") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeSumAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_sum_partial") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        if (result_field_list[result_id]->id() == arrow::Decimal128Type::type_id) {
          result_id += 2;
        } else {
          result_id += 1;
        }
        RETURN_NOT_OK(MakeSumActionPartial(ctx_, type_list[type_id], res_type_list, &action));
      }else if (action_name_list[action_id].compare("action_avg") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeAvgAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare(0, ACTION_MIN.size(), ACTION_MIN) == 0) {
        auto res_type_list = {result_field_list[result_id]};
        bool NaN_check = getActionOption(action_name_list[action_id], ACTION_MIN);
        result_id += 1;
        RETURN_NOT_OK(MakeMinAction(ctx_, type_list[type_id], res_type_list, &action, NaN_check));
      } else if (action_name_list[action_id].compare(0, ACTION_MAX.size(), ACTION_MAX) == 0) {
        auto res_type_list = {result_field_list[result_id]};
        bool NaN_check = getActionOption(action_name_list[action_id], ACTION_MAX);
        result_id += 1;
        RETURN_NOT_OK(MakeMaxAction(ctx_, type_list[type_id], res_type_list, &action, NaN_check));
      } else if (action_name_list[action_id].compare("action_sum_count") == 0) {
        auto res_type_list = {result_field_list[result_id], result_field_list[result_id + 1]};
        result_id += 2;
        RETURN_NOT_OK(MakeSumCountAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_sum_count_merge") == 0) {
        auto res_type_list = {result_field_list[result_id], result_field_list[result_id + 1]};
        result_id += 2;
        RETURN_NOT_OK(MakeSumCountMergeAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_avgByCount") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeAvgByCountAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare(0, 20, "action_countLiteral_") ==
                 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        int arg = std::stol(action_name_list[action_id].substr(20));
        RETURN_NOT_OK(MakeCountLiteralAction(ctx_, arg, res_type_list, &action));
      } else if (action_name_list[action_id].compare("action_stddev_samp_partial") ==
                 0) {
        auto res_type_list = {result_field_list[result_id], result_field_list[result_id + 1], result_field_list[result_id + 2]};
        result_id += 3;
        RETURN_NOT_OK(MakeStddevSampPartialAction(ctx_, type_list[type_id], res_type_list, &action));
      } else if (action_name_list[action_id].compare(0, 24, "action_stddev_samp_final") == 0) {
        bool null_on_divide_by_zero = false;
        if (action_name_list[action_id].length() > 25) {
          null_on_divide_by_zero = (action_name_list[action_id].substr(25) == "true" ? true : false);
        }
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeStddevSampFinalAction(ctx_, type_list[type_id], res_type_list, null_on_divide_by_zero, &action));
      } else {
        return arrow::Status::NotImplemented(action_name_list[action_id],
                                             " is not implementetd.");
      }
      type_id += 1;
      (*action_list).push_back(action);
    }
    return arrow::Status::OK();
    )" << std::endl;
    action_list_define_function_ss << "}" << std::endl;

    // 6. create all input evaluated finish codes
    finish_ss << "do_hash_aggr_finish_" << level << " = true;" << std::endl;
    finish_ss << "should_stop_ = false;" << std::endl;
    finish_ss << "std::vector<std::shared_ptr<arrow::Array>> do_hash_aggr_finish_"
              << level << "_out;" << std::endl;
    finish_ss << "if(do_hash_aggr_finish_" << level << ") {";
    for (int i = 0; i < action_idx; i++) {
      finish_ss << "aggr_action_list_" << level << "[" << i
                << "]->Finish(do_hash_aggr_finish_" << level << "_offset,"
                << GetBatchSize() << ", &do_hash_aggr_finish_" << level << "_out);"
                << std::endl;
    }
    finish_ss << "if (do_hash_aggr_finish_" << level << "_out.size() > 0) {" << std::endl;
    finish_ss << "auto tmp_arr = std::make_shared<Array>(do_hash_aggr_finish_" << level
              << "_out[0]);" << std::endl;
    finish_ss << "out_length += tmp_arr->length();" << std::endl;
    finish_ss << "do_hash_aggr_finish_" << level << "_offset += tmp_arr->length();"
              << std::endl;
    finish_ss << "}" << std::endl;
    finish_ss << "if (out_length == 0 || do_hash_aggr_finish_" << level
              << "_num_groups < do_hash_aggr_finish_" << level << "_offset) {"
              << std::endl;
    finish_ss << "should_stop_ = true;" << std::endl;
    finish_ss << "}" << std::endl;
    finish_ss << "}" << std::endl;

    // 7. Do GandivaProjector if result_expr_list is not empty
    if (!result_expr_list_.empty()) {
      codegen_ctx->gandiva_projector = std::make_shared<GandivaProjector>(
          ctx_, arrow::schema(result_field_list_), GetGandivaKernel(result_expr_list_));

      finish_ss << "RETURN_NOT_OK(gandiva_projector_list_[gp_idx++]->Evaluate(&"
                   "do_hash_"
                   "aggr_finish_"
                << level << "_out));" << std::endl;
    }

    finish_condition_ss << "do_hash_aggr_finish_" << level;

    codegen_ctx->function_list.push_back(action_list_define_function_ss.str());
    codegen_ctx->prepare_codes += prepare_ss.str();
    codegen_ctx->process_codes += process_ss.str();
    codegen_ctx->definition_codes += define_ss.str();
    codegen_ctx->aggregate_prepare_codes += aggr_prepare_ss.str();
    codegen_ctx->aggregate_finish_codes += finish_ss.str();
    codegen_ctx->aggregate_finish_condition_codes += finish_condition_ss.str();
    // set join output list for next kernel.
    ///////////////////////////////
    *codegen_ctx_out = codegen_ctx;

    return arrow::Status::OK();
  }

 private:
  arrow::compute::ExecContext* ctx_;
  arrow::MemoryPool* pool_;
  std::string signature_;

  std::vector<std::shared_ptr<arrow::Field>> input_field_list_;
  std::vector<std::shared_ptr<gandiva::Node>> action_list_;
  std::vector<std::shared_ptr<arrow::Field>> result_field_list_;
  std::vector<std::shared_ptr<gandiva::Node>> result_expr_list_;

  std::vector<gandiva::NodePtr> prepare_function_list_;
  std::vector<gandiva::NodePtr> key_node_list_;
  std::vector<int> key_index_list_;

  std::vector<std::pair<std::string, gandiva::DataTypePtr>> action_name_list_;
  std::vector<std::vector<int>> action_prepare_index_list_;

  bool getActionOption(std::string action_name, std::string action_name_prefix) {
    int prefix_size = action_name_prefix.size();
    bool option = false;
    if (action_name.size() > prefix_size &&
        action_name.compare(0, prefix_size + 1, action_name_prefix + "_") == 0) {
      auto lit = action_name.substr(prefix_size + 1);
      option = (lit == "true") ? true : false;
    }
    return option;
  }

  arrow::Status PrepareActionList(
      std::vector<std::pair<std::string, gandiva::DataTypePtr>> action_name_list,
      std::vector<gandiva::DataTypePtr> result_field_list,
      std::vector<std::shared_ptr<ActionBase>>* action_list) {
    int result_id = 0;
    for (int action_id = 0; action_id < action_name_list.size(); action_id++) {
      std::shared_ptr<ActionBase> action;
      auto action_name = action_name_list[action_id].first;
      auto action_input_type = action_name_list[action_id].second;
      if (action_name.compare("action_groupby") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeUniqueAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare("action_count") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeCountAction(ctx_, res_type_list, &action));
      } else if (action_name.compare("action_sum") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeSumAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare("action_sum_partial") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        if (result_field_list[result_id]->id() == arrow::Decimal128Type::type_id) {
          result_id += 2;
        } else {
          result_id += 1;
        }
        RETURN_NOT_OK(
            MakeSumActionPartial(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare("action_avg") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeAvgAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare(0, ACTION_MIN.size(), ACTION_MIN) == 0) {
        auto res_type_list = {result_field_list[result_id]};
        bool NaN_check = getActionOption(action_name, ACTION_MIN);
        result_id += 1;
        RETURN_NOT_OK(
            MakeMinAction(ctx_, action_input_type, res_type_list, &action, NaN_check));
      } else if (action_name.compare(0, ACTION_MAX.size(), ACTION_MAX) == 0) {
        auto res_type_list = {result_field_list[result_id]};
        bool NaN_check = getActionOption(action_name, ACTION_MAX);
        result_id += 1;
        RETURN_NOT_OK(
            MakeMaxAction(ctx_, action_input_type, res_type_list, &action, NaN_check));
      } else if (action_name.compare("action_sum_count") == 0) {
        auto res_type_list = {result_field_list[result_id],
                              result_field_list[result_id + 1]};
        result_id += 2;
        RETURN_NOT_OK(
            MakeSumCountAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare("action_sum_count_merge") == 0) {
        auto res_type_list = {result_field_list[result_id],
                              result_field_list[result_id + 1]};
        result_id += 2;
        RETURN_NOT_OK(
            MakeSumCountMergeAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare("action_avgByCount") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(
            MakeAvgByCountAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare(0, 20, "action_countLiteral_") == 0) {
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        int arg = std::stol(action_name.substr(20));
        RETURN_NOT_OK(MakeCountLiteralAction(ctx_, arg, res_type_list, &action));
      } else if (action_name.compare("action_stddev_samp_partial") == 0) {
        auto res_type_list = {result_field_list[result_id],
                              result_field_list[result_id + 1],
                              result_field_list[result_id + 2]};
        result_id += 3;
        RETURN_NOT_OK(
            MakeStddevSampPartialAction(ctx_, action_input_type, res_type_list, &action));
      } else if (action_name.compare(0, 24, "action_stddev_samp_final") == 0) {
        bool null_on_divide_by_zero = false;
        if (action_name.length() > 25) {
          null_on_divide_by_zero = (action_name.substr(25) == "true" ? true : false);
        }
        auto res_type_list = {result_field_list[result_id]};
        result_id += 1;
        RETURN_NOT_OK(MakeStddevSampFinalAction(ctx_, action_input_type, res_type_list,
                                                null_on_divide_by_zero, &action));
      } else {
        return arrow::Status::NotImplemented(action_name, " is not implementetd.");
      }
      (*action_list).push_back(action);
    }
    return arrow::Status::OK();
  }

  template <typename T, typename Enable = void>
  class HashAggregateResultIterator {};

  template <typename DataType>
  class HashAggregateResultIterator<DataType, enable_if_number<DataType>>
      : public ResultIterator<arrow::RecordBatch> {
   public:
    using T = typename arrow::TypeTraits<DataType>::CType;
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    HashAggregateResultIterator(
        arrow::compute::ExecContext* ctx, gandiva::SchemaPtr result_schema,
        std::vector<int>& key_index_list,
        const std::vector<std::vector<int>>& action_prepare_index_list,
        std::shared_ptr<GandivaProjector> pre_process_projector,
        std::shared_ptr<GandivaProjector> post_process_projector,
        std::vector<std::shared_ptr<ActionBase>> action_impl_list)
        : ctx_(ctx),
          result_schema_(result_schema),
          key_index_list_(key_index_list),
          action_prepare_index_list_(action_prepare_index_list),
          pre_process_projector_(pre_process_projector),
          post_process_projector_(post_process_projector),
          action_impl_list_(action_impl_list) {
      batch_size_ = GetBatchSize();
      aggr_hash_table_ = std::make_shared<SparseHashMap<T>>(ctx->memory_pool());
    }

    arrow::Status ProcessAndCacheOne(
        const std::vector<std::shared_ptr<arrow::Array>>& orig_in,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      if (orig_in.size() == 0 || orig_in[0]->length() == 0) return arrow::Status::OK();
      // 1. do pre process and prepare action_func
      arrow::ArrayVector in;
      if (pre_process_projector_) {
        in = pre_process_projector_->Evaluate(orig_in);
      } else {
        in = orig_in;
      }

      // 2.1 handle no groupby scenario
      if (key_index_list_.size() == 0) {
        arrow::ArrayVector cols;
        for (int i = 0; i < action_impl_list_.size(); i++) {
          auto action = action_impl_list_[i];
          cols.clear();
          for (auto idx : action_prepare_index_list_[i]) {
            cols.push_back(in[idx]);
          }
          if (cols.empty()) {
            // There is a special case, when we need to do no groupby count
            // literal
            RETURN_NOT_OK(action->EvaluateCountLiteral(in[0]->length()));

          } else {
            RETURN_NOT_OK(action->Evaluate(cols));
          }
        }
        total_out_length_ = 1;
        return arrow::Status::OK();
      }

      // 2.2 Get each row's group by key
      auto typed_key_in = std::dynamic_pointer_cast<ArrayType>(in[key_index_list_[0]]);
      auto length = in[0]->length();

      std::vector<int> indices;
      indices.resize(length, -1);

      for (int i = 0; i < length; i++) {
        auto aggr_key = typed_key_in->GetView(i);
        auto aggr_key_validity =
            typed_key_in->null_count() == 0 ? true : !typed_key_in->IsNull(i);

        // 3. get key from hash_table
        int memo_index = 0;
        if (!aggr_key_validity) {
          memo_index = aggr_hash_table_->GetOrInsertNull([](int) {}, [](int) {});
        } else {
          aggr_hash_table_->GetOrInsert(
              aggr_key, [](int) {}, [](int) {}, &memo_index);
        }

        if (memo_index > max_group_id_) {
          max_group_id_ = memo_index;
        }
        indices[i] = memo_index;
      }

      total_out_length_ = max_group_id_ + 1;
      // 4. prepare action func and evaluate
      std::vector<std::function<arrow::Status(int)>> eval_func_list;
      std::vector<std::function<arrow::Status()>> eval_null_func_list;
      arrow::ArrayVector cols;
      for (int i = 0; i < action_impl_list_.size(); i++) {
        auto action = action_impl_list_[i];
        cols.clear();
        for (auto idx : action_prepare_index_list_[i]) {
          cols.push_back(in[idx]);
        }
        std::function<arrow::Status(int)> func;
        std::function<arrow::Status()> null_func;
        action->Submit(cols, max_group_id_, &func, &null_func);
        eval_func_list.push_back(func);
        eval_null_func_list.push_back(null_func);
      }

      for (auto eval_func : eval_func_list) {
        for (auto memo_index : indices) {
          RETURN_NOT_OK(eval_func(memo_index));
        }
      }

      return arrow::Status::OK();
    }

    bool HasNext() { return offset_ < total_out_length_; }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      uint64_t out_length = 0;
      int gp_idx = 0;
      std::vector<std::shared_ptr<arrow::Array>> outputs;
      for (auto action : action_impl_list_) {
        action->Finish(offset_, batch_size_, &outputs);
      }
      if (outputs.size() > 0) {
        out_length += outputs[0]->length();
        offset_ += outputs[0]->length();
      }
      if (post_process_projector_) {
        RETURN_NOT_OK(post_process_projector_->Evaluate(&outputs));
      }
      *out = arrow::RecordBatch::Make(result_schema_, out_length, outputs);
      return arrow::Status::OK();
    }

   private:
    arrow::compute::ExecContext* ctx_;
    std::vector<std::shared_ptr<ActionBase>> action_impl_list_;
    std::shared_ptr<SparseHashMap<T>> aggr_hash_table_;
    const std::vector<int>& key_index_list_;
    const std::vector<std::vector<int>>& action_prepare_index_list_;
    std::shared_ptr<GandivaProjector> pre_process_projector_;
    std::shared_ptr<GandivaProjector> post_process_projector_;
    std::shared_ptr<arrow::Schema> result_schema_;
    int max_group_id_ = -1;
    int offset_ = 0;
    int total_out_length_ = 0;
    int batch_size_;
  };

  template <typename DataType>
  class HashAggregateResultIterator<DataType, enable_if_string_like<DataType>>
      : public ResultIterator<arrow::RecordBatch> {
   public:
    HashAggregateResultIterator(
        arrow::compute::ExecContext* ctx, gandiva::SchemaPtr result_schema,
        const std::vector<int>& key_index_list,
        const std::vector<std::vector<int>>& action_prepare_index_list,
        std::shared_ptr<GandivaProjector> pre_process_projector,
        std::shared_ptr<GandivaProjector> post_process_projector,
        std::vector<std::shared_ptr<ActionBase>> action_impl_list)
        : ctx_(ctx),
          result_schema_(result_schema),
          key_index_list_(key_index_list),
          action_prepare_index_list_(action_prepare_index_list),
          pre_process_projector_(pre_process_projector),
          post_process_projector_(post_process_projector),
          action_impl_list_(action_impl_list) {
      aggr_hash_table_ = std::make_shared<StringHashMap>(ctx->memory_pool());
#ifdef DEBUG
      std::cout << "using string hashagg res" << std::endl;
#endif
      batch_size_ = GetBatchSize();
      if (key_index_list.size() > 1) {
        aggr_key_unsafe_row = std::make_shared<UnsafeRow>(key_index_list.size());
      }
    }

    arrow::Status ProcessAndCacheOne(
        const std::vector<std::shared_ptr<arrow::Array>>& orig_in,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      if (orig_in.size() == 0 || orig_in[0]->length() == 0) return arrow::Status::OK();
      // 1. do pre process and prepare action_func
      arrow::ArrayVector in;
      if (pre_process_projector_) {
        in = pre_process_projector_->Evaluate(orig_in);
      } else {
        in = orig_in;
      }

      // 2. handle multiple keys
      std::vector<std::shared_ptr<UnsafeArray>> payloads;
      std::shared_ptr<arrow::StringArray> typed_key_in;
      if (aggr_key_unsafe_row) {
        for (int idx = 0; idx < key_index_list_.size(); idx++) {
          auto arr = in[key_index_list_[idx]];
          std::shared_ptr<UnsafeArray> payload;
          MakeUnsafeArray(arr->type(), idx, arr, &payload);
          payloads.push_back(payload);
        }
      } else {
        typed_key_in =
            std::dynamic_pointer_cast<arrow::StringArray>(in[key_index_list_[0]]);
      }

      // 3. Get each row's group by key
      auto length = in[0]->length();
      std::vector<int> indices;
      indices.resize(length, -1);
      for (int i = 0; i < length; i++) {
        auto aggr_key_validity = true;
        arrow::util::string_view aggr_key;
        if (aggr_key_unsafe_row) {
          aggr_key_unsafe_row->reset();
          int idx = 0;
          for (auto payload_arr : payloads) {
            payload_arr->Append(i, &aggr_key_unsafe_row);
          }
          aggr_key = arrow::util::string_view(aggr_key_unsafe_row->data,
                                              aggr_key_unsafe_row->cursor);
        } else {
          aggr_key = typed_key_in->GetView(i);
          aggr_key_validity =
              typed_key_in->null_count() == 0 ? true : !typed_key_in->IsNull(i);
        }

        // 3. get key from hash_table
        int memo_index = 0;
        if (!aggr_key_validity) {
          memo_index = aggr_hash_table_->GetOrInsertNull([](int) {}, [](int) {});
        } else {
          aggr_hash_table_->GetOrInsert(
              aggr_key, [](int) {}, [](int) {}, &memo_index);
        }

        if (memo_index > max_group_id_) {
          max_group_id_ = memo_index;
        }
        indices[i] = memo_index;
      }

      total_out_length_ = max_group_id_ + 1;
      // 4. prepare action func and evaluate
      std::vector<std::function<arrow::Status(int)>> eval_func_list;
      std::vector<std::function<arrow::Status()>> eval_null_func_list;
      arrow::ArrayVector cols;
      for (int i = 0; i < action_impl_list_.size(); i++) {
        auto action = action_impl_list_[i];
        cols.clear();
        for (auto idx : action_prepare_index_list_[i]) {
          cols.push_back(in[idx]);
        }
        std::function<arrow::Status(int)> func;
        std::function<arrow::Status()> null_func;
        action->Submit(cols, max_group_id_, &func, &null_func);
        eval_func_list.push_back(func);
        eval_null_func_list.push_back(null_func);
      }

      for (auto eval_func : eval_func_list) {
        for (auto memo_index : indices) {
          RETURN_NOT_OK(eval_func(memo_index));
        }
      }
      return arrow::Status::OK();
    }

    bool HasNext() { return offset_ < total_out_length_; }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      uint64_t out_length = 0;
      int gp_idx = 0;
      std::vector<std::shared_ptr<arrow::Array>> outputs;
      for (auto action : action_impl_list_) {
        action->Finish(offset_, batch_size_, &outputs);
      }
      if (outputs.size() > 0) {
        out_length += outputs[0]->length();
        offset_ += outputs[0]->length();
      }
      if (post_process_projector_) {
        RETURN_NOT_OK(post_process_projector_->Evaluate(&outputs));
      }
      *out = arrow::RecordBatch::Make(result_schema_, out_length, outputs);
      return arrow::Status::OK();
    }

   private:
    arrow::compute::ExecContext* ctx_;
    std::vector<std::shared_ptr<ActionBase>> action_impl_list_;
    std::shared_ptr<StringHashMap> aggr_hash_table_;
    const std::vector<int>& key_index_list_;
    const std::vector<std::vector<int>>& action_prepare_index_list_;
    std::shared_ptr<GandivaProjector> pre_process_projector_;
    std::shared_ptr<GandivaProjector> post_process_projector_;
    std::shared_ptr<UnsafeRow> aggr_key_unsafe_row;
    std::shared_ptr<arrow::Schema> result_schema_;
    int max_group_id_ = -1;
    int offset_ = 0;
    int total_out_length_ = 0;
    int batch_size_;
  };

  template <typename DataType>
  class HashAggregateResultIterator<DataType, precompile::enable_if_decimal<DataType>>
      : public ResultIterator<arrow::RecordBatch> {
   public:
    using T = arrow::Decimal128;
    using ArrayType = precompile::Decimal128Array;
    HashAggregateResultIterator(
        arrow::compute::ExecContext* ctx, gandiva::SchemaPtr result_schema,
        std::vector<int>& key_index_list,
        const std::vector<std::vector<int>>& action_prepare_index_list,
        std::shared_ptr<GandivaProjector> pre_process_projector,
        std::shared_ptr<GandivaProjector> post_process_projector,
        std::vector<std::shared_ptr<ActionBase>> action_impl_list)
        : ctx_(ctx),
          result_schema_(result_schema),
          key_index_list_(key_index_list),
          action_prepare_index_list_(action_prepare_index_list),
          pre_process_projector_(pre_process_projector),
          post_process_projector_(post_process_projector),
          action_impl_list_(action_impl_list) {
      batch_size_ = GetBatchSize();
      aggr_hash_table_ =
          std::make_shared<precompile::Decimal128HashMap>(ctx->memory_pool());
    }

    arrow::Status ProcessAndCacheOne(
        const std::vector<std::shared_ptr<arrow::Array>>& orig_in,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      if (orig_in.size() == 0 || orig_in[0]->length() == 0) return arrow::Status::OK();
      // 1. do pre process and prepare action_func
      arrow::ArrayVector in;
      if (pre_process_projector_) {
        in = pre_process_projector_->Evaluate(orig_in);
      } else {
        in = orig_in;
      }

      // 2.1 handle no groupby scenario
      if (key_index_list_.size() == 0) {
        arrow::ArrayVector cols;
        for (int i = 0; i < action_impl_list_.size(); i++) {
          auto action = action_impl_list_[i];
          cols.clear();
          for (auto idx : action_prepare_index_list_[i]) {
            cols.push_back(in[idx]);
          }
          if (cols.empty()) {
            // There is a special case, when we need to do no groupby count
            // literal
            RETURN_NOT_OK(action->EvaluateCountLiteral(in[0]->length()));

          } else {
            RETURN_NOT_OK(action->Evaluate(cols));
          }
        }
        total_out_length_ = 1;
        return arrow::Status::OK();
      }

      // 2.2 Get each row's group by key
      auto typed_key_in = std::make_shared<ArrayType>(in[key_index_list_[0]]);
      auto length = in[0]->length();

      std::vector<int> indices;
      indices.resize(length, -1);

      for (int i = 0; i < length; i++) {
        auto aggr_key = typed_key_in->GetView(i);
        auto aggr_key_validity =
            typed_key_in->null_count() == 0 ? true : !typed_key_in->IsNull(i);

        // 3. get key from hash_table
        int memo_index = 0;
        if (!aggr_key_validity) {
          memo_index = aggr_hash_table_->GetOrInsertNull([](int) {}, [](int) {});
        } else {
          aggr_hash_table_->GetOrInsert(
              aggr_key, [](int) {}, [](int) {}, &memo_index);
        }

        if (memo_index > max_group_id_) {
          max_group_id_ = memo_index;
        }
        indices[i] = memo_index;
      }

      total_out_length_ = max_group_id_ + 1;
      // 4. prepare action func and evaluate
      std::vector<std::function<arrow::Status(int)>> eval_func_list;
      std::vector<std::function<arrow::Status()>> eval_null_func_list;
      arrow::ArrayVector cols;
      for (int i = 0; i < action_impl_list_.size(); i++) {
        auto action = action_impl_list_[i];
        cols.clear();
        for (auto idx : action_prepare_index_list_[i]) {
          cols.push_back(in[idx]);
        }
        std::function<arrow::Status(int)> func;
        std::function<arrow::Status()> null_func;
        action->Submit(cols, max_group_id_, &func, &null_func);
        eval_func_list.push_back(func);
        eval_null_func_list.push_back(null_func);
      }

      for (auto eval_func : eval_func_list) {
        for (auto memo_index : indices) {
          RETURN_NOT_OK(eval_func(memo_index));
        }
      }

      return arrow::Status::OK();
    }

    bool HasNext() { return offset_ < total_out_length_; }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      uint64_t out_length = 0;
      int gp_idx = 0;
      std::vector<std::shared_ptr<arrow::Array>> outputs;
      for (auto action : action_impl_list_) {
        action->Finish(offset_, batch_size_, &outputs);
      }
      if (outputs.size() > 0) {
        out_length += outputs[0]->length();
        offset_ += outputs[0]->length();
      }

      if (post_process_projector_) {
        RETURN_NOT_OK(post_process_projector_->Evaluate(&outputs));
      }

      *out = arrow::RecordBatch::Make(result_schema_, out_length, outputs);
      return arrow::Status::OK();
    }

   private:
    arrow::compute::ExecContext* ctx_;
    std::vector<std::shared_ptr<ActionBase>> action_impl_list_;
    std::shared_ptr<precompile::Decimal128HashMap> aggr_hash_table_;
    const std::vector<int>& key_index_list_;
    const std::vector<std::vector<int>>& action_prepare_index_list_;
    std::shared_ptr<GandivaProjector> pre_process_projector_;
    std::shared_ptr<GandivaProjector> post_process_projector_;
    std::shared_ptr<arrow::Schema> result_schema_;
    int max_group_id_ = -1;
    int offset_ = 0;
    int total_out_length_ = 0;
    int batch_size_;
  };
};
arrow::Status HashAggregateKernel::Make(
    arrow::compute::ExecContext* ctx,
    std::vector<std::shared_ptr<gandiva::Node>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::vector<std::shared_ptr<gandiva::Node>> result_field_node_list,
    std::vector<std::shared_ptr<gandiva::Node>> result_expr_node_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashAggregateKernel>(
      ctx, input_field_list, action_list, result_field_node_list, result_expr_node_list);
  return arrow::Status::OK();
}

HashAggregateKernel::HashAggregateKernel(
    arrow::compute::ExecContext* ctx,
    std::vector<std::shared_ptr<gandiva::Node>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::vector<std::shared_ptr<gandiva::Node>> result_field_node_list,
    std::vector<std::shared_ptr<gandiva::Node>> result_expr_node_list) {
  impl_.reset(new Impl(ctx, input_field_list, action_list, result_field_node_list,
                       result_expr_node_list));
  kernel_name_ = "HashAggregateKernelKernel";
  ctx_ = ctx;
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status HashAggregateKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

arrow::Status HashAggregateKernel::DoCodeGen(
    int level,
    std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
        input,
    std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
  return impl_->DoCodeGen(level, input, codegen_ctx_out, var_id);
}

std::string HashAggregateKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
