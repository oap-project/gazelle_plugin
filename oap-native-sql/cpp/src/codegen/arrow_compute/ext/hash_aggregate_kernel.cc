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

#include <arrow/compute/context.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/codegen_register.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_action_codegen_impl.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  SortArraysToIndices  ////////////////
class HashAggregateKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::Field>> input_field_list,
       std::vector<std::shared_ptr<gandiva::Node>> action_list,
       std::shared_ptr<arrow::Schema> result_schema)
      : ctx_(ctx),
        input_field_list_(input_field_list),
        action_list_(action_list),
        result_schema_(result_schema) {
    // if there is projection inside aggregate, we need to extract them into
    // projector_list
    auto status = PrepareActionCodegen();
    if (status.ok()) {
      THROW_NOT_OK(LoadJITFunction());
    }
  }
  virtual arrow::Status LoadJITFunction() {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << "[HashAggregate]";
    func_args_ss << "[input_schema]";
    for (auto field : input_field_list_) {
      func_args_ss << field->type()->ToString() << "|";
    }
    func_args_ss << "[ordinal]";
    for (auto ordinal : input_ordinal_list_) {
      func_args_ss << ordinal << "|";
    }
    func_args_ss << "[actions]";
    for (auto action : action_list_) {
      std::shared_ptr<CodeGenRegister> node_tmp;
      RETURN_NOT_OK(MakeCodeGenRegister(action, &node_tmp));
      func_args_ss << node_tmp->GetFingerprint() << "|";
    }
    func_args_ss << "[output_schema]";
    for (auto field : result_schema_->fields()) {
      func_args_ss << field->type()->ToString() << "|";
    }

#ifdef DEBUG
    std::cout << "func_args_ss is " << func_args_ss.str() << std::endl;
#endif

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    signature_ = signature_ss.str();
#ifdef DEBUG
    std::cout << "signature is " << signature_ << std::endl;
#endif

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, &hash_aggregater_);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes();
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature_));
      RETURN_NOT_OK(LoadLibrary(signature_, ctx_, &hash_aggregater_));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  virtual arrow::Status Evaluate(const ArrayList& in) {
    if (projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      arrow::ArrayVector outputs;
      auto in_batch = arrow::RecordBatch::Make(original_input_schema_, length, in);
      RETURN_NOT_OK(projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      auto out_batch = arrow::RecordBatch::Make(projected_input_schema_, length, outputs);
      RETURN_NOT_OK(hash_aggregater_->Evaluate(in, out_batch));
    } else {
      std::shared_ptr<arrow::RecordBatch> empty_batch;
      RETURN_NOT_OK(hash_aggregater_->Evaluate(in, empty_batch));
    }
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(hash_aggregater_->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::OK();
  }

  std::string GetSignature() { return signature_; }

 protected:
  std::string signature_;
  std::vector<std::shared_ptr<arrow::Field>> input_field_list_;
  std::shared_ptr<arrow::Schema> original_input_schema_;
  std::shared_ptr<arrow::Schema> projected_input_schema_;
  std::vector<std::shared_ptr<gandiva::Node>> action_list_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<CodeGenBase> hash_aggregater_;
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<arrow::Field>> field_list_;
  std::shared_ptr<gandiva::Projector> projector_;
  std::vector<std::shared_ptr<ActionCodeGen>> action_impl_list_;
  std::vector<std::pair<gandiva::NodePtr, std::string>> key_list_;
  std::vector<std::string> input_ordinal_list_;

  arrow::Status PrepareActionCodegen() {
    std::vector<gandiva::ExpressionPtr> expr_list;
    std::vector<gandiva::FieldPtr> output_field_list;
    int index = 0;
    for (auto func_node : action_list_) {
      std::shared_ptr<CodeGenNodeVisitor> codegen_visitor;
      std::shared_ptr<ActionCodeGen> action_codegen;
      RETURN_NOT_OK(MakeCodeGenNodeVisitor(func_node, input_field_list_, &action_codegen,
                                           &codegen_visitor));
      if (!action_codegen) {
        // some action use field not in input schema
        // This is happened when spark pass none field.
        return arrow::Status::Invalid("some action field is not exists in input schema");
      }
      action_impl_list_.push_back(action_codegen);
      if (action_codegen->IsPreProjected()) {
        auto expr = action_codegen->GetProjectorExpr();
        output_field_list.push_back(expr->result());
        // chendi: We need to use index in project output to replace project name
        action_codegen->WithProjectIndex(index++);
        expr_list.push_back(expr);
      }
    }
    RETURN_NOT_OK(GetInputOrdinalList(action_impl_list_, &input_ordinal_list_));
    RETURN_NOT_OK(GetGroupKey(action_impl_list_, &key_list_));
    if (key_list_.size() > 1) {
      std::vector<gandiva::NodePtr> key_expr_list;
      for (auto key : key_list_) {
        key_expr_list.push_back(key.first);
      }
      auto expr = GetConcatedKernel(key_expr_list);
      output_field_list.push_back(expr->result());
      expr_list.push_back(expr);
    }
    if (!expr_list.empty()) {
      original_input_schema_ = arrow::schema(input_field_list_);
      projected_input_schema_ = arrow::schema(output_field_list);
      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      THROW_NOT_OK(gandiva::Projector::Make(original_input_schema_, expr_list,
                                            configuration, &projector_));
    }
    return arrow::Status::OK();
  }

  virtual std::string ProduceCodes() {
    int indice = 0;

    std::vector<std::string> cached_list;
    std::vector<std::string> typed_input_list;
    std::vector<std::string> typed_group_input_list;
    std::vector<std::string> result_cached_list;

    std::stringstream evaluate_get_typed_array_ss;
    std::stringstream compute_on_exists_ss;
    std::stringstream compute_on_new_ss;
    std::stringstream cached_define_ss;
    std::stringstream on_finish_ss;
    std::stringstream result_cached_define_ss;
    std::vector<std::string> result_cached_parameter_list;
    std::vector<std::string> result_cached_array_list;
    std::stringstream result_cached_prepare_ss;
    std::stringstream result_cached_to_builder_ss;
    std::stringstream result_cached_to_array_ss;
    std::stringstream result_cached_array_ss;
    for (auto action : action_impl_list_) {
      auto func_sig_list = action->GetVariablesList();
      auto evaluate_get_typed_array_list = action->GetTypedInputList();
      auto compute_on_exists_prepare_list = action->GetOnExistsPrepareCodesList();
      auto compute_on_new_prepare_list = action->GetOnNewPrepareCodesList();
      auto compute_on_exists_list = action->GetOnExistsCodesList();
      auto compute_on_new_list = action->GetOnNewCodesList();
      auto cached_define_list = action->GetVariablesDefineList();
      int i = 0;
      for (auto sig : func_sig_list) {
        if (std::find(cached_list.begin(), cached_list.end(), sig) == cached_list.end()) {
          cached_list.push_back(sig);
          auto tmp_typed_name = evaluate_get_typed_array_list[i].first;
          if (tmp_typed_name != "" &&
              std::find(typed_input_list.begin(), typed_input_list.end(),
                        tmp_typed_name) == typed_input_list.end()) {
            if (std::find(typed_group_input_list.begin(), typed_group_input_list.end(),
                          tmp_typed_name) == typed_group_input_list.end()) {
              evaluate_get_typed_array_ss << evaluate_get_typed_array_list[i].second
                                          << std::endl;
            }

            if (!action->IsGroupBy()) {
              typed_input_list.push_back(tmp_typed_name);
            } else {
              typed_group_input_list.push_back(tmp_typed_name);
            }
            compute_on_exists_ss << compute_on_exists_prepare_list[i] << std::endl;
            compute_on_new_ss << compute_on_new_prepare_list[i] << std::endl;
          }
          compute_on_exists_ss << compute_on_exists_list[i] << std::endl;
          compute_on_new_ss << compute_on_new_list[i] << std::endl;
          cached_define_ss << cached_define_list[i] << std::endl;
        }
        i++;
      }
      auto result_variables_list = action->GetFinishVariablesList();
      auto on_finish_list = action->GetOnFinishCodesList();
      auto result_cached_define_list = action->GetFinishVariablesDefineList();
      auto result_cached_parameter_list_tmp = action->GetFinishVariablesParameterList();
      auto result_cached_prepare_list = action->GetFinishVariablesPrepareList();
      auto result_cached_to_builder_list = action->GetFinishVariablesToBuilderList();
      auto result_cached_to_array_list = action->GetFinishVariablesToArrayList();
      auto result_cached_array_list_tmp = action->GetFinishVariablesArrayList();
      i = 0;
      for (auto sig : result_variables_list) {
        if (std::find(result_cached_list.begin(), result_cached_list.end(), sig) ==
            result_cached_list.end()) {
          result_cached_list.push_back(sig);
          on_finish_ss << on_finish_list[i] << std::endl;
          result_cached_define_ss << result_cached_define_list[i] << std::endl;
          result_cached_parameter_list.push_back(result_cached_parameter_list_tmp[i]);
          result_cached_prepare_ss << result_cached_prepare_list[i] << std::endl;
          result_cached_to_builder_ss << result_cached_to_builder_list[i] << std::endl;
          result_cached_to_array_ss << result_cached_to_array_list[i] << std::endl;
        }
        result_cached_array_list.push_back(result_cached_array_list_tmp[i]);
        i++;
      }
    }
    auto evaluate_get_typed_array_str = evaluate_get_typed_array_ss.str();
    for (auto input : typed_group_input_list) {
      if (std::find(typed_input_list.begin(), typed_input_list.end(), input) ==
          typed_input_list.end())
        typed_input_list.push_back(input);
    }
    auto typed_input_parameter_str = GetParameterList(typed_input_list);
    auto compute_on_exists_str = compute_on_exists_ss.str();
    auto compute_on_new_str = compute_on_new_ss.str();
    auto impl_cached_define_str = cached_define_ss.str();
    auto on_finish_str = on_finish_ss.str();
    auto on_finish_cached_parameter_str = GetParameterList(result_cached_list, false);
    auto result_cached_parameter_str =
        GetParameterList(result_cached_parameter_list, false);
    auto result_cached_prepare_str = result_cached_prepare_ss.str();
    auto result_cached_define_str = result_cached_define_ss.str();
    auto result_cached_to_builder_str = result_cached_to_builder_ss.str();
    auto result_cached_to_array_str = result_cached_to_array_ss.str();
    auto result_cached_array_str = GetParameterList(result_cached_array_list, false);

    bool multiple_cols = (key_list_.size() > 1);
    std::string concat_kernel;
    std::string hash_map_include_str = R"(#include "precompile/sparse_hash_map.h")";
    std::string hash_map_type_str =
        "SparseHashMap<" + GetCTypeString(arrow::int64()) + ">";
    std::string hash_map_define_str =
        "std::make_shared<" + hash_map_type_str + ">(ctx_->memory_pool());";
    std::string evaluate_get_typed_key_array_str;
    std::string evaluate_get_typed_key_method_str;
    if (!multiple_cols) {
      auto key_type = key_list_[0].first->return_type();
      if (key_type->id() == arrow::Type::STRING) {
        hash_map_type_str = GetTypeString(arrow::utf8(), "") + "HashMap";
        hash_map_include_str = R"(#include "precompile/hash_map.h")";
      } else {
        hash_map_type_str = "SparseHashMap<" + GetCTypeString(key_type) + ">";
      }
      hash_map_define_str =
          "std::make_shared<" + hash_map_type_str + ">(ctx_->memory_pool());";
      evaluate_get_typed_key_array_str = "auto typed_array = std::make_shared<" +
                                         GetTypeString(key_type, "Array") + ">(" +
                                         key_list_[0].second + ");\n";
      if (key_type->id() != arrow::Type::STRING) {
        evaluate_get_typed_key_method_str = "GetView";
      } else {
        evaluate_get_typed_key_method_str = "GetString";
      }
    } else {
      evaluate_get_typed_key_array_str =
          "auto typed_array = "
          "std::make_shared<Int64Array>(projected_batch->GetColumnByName("
          "\"projection_key\"));\n";
      evaluate_get_typed_key_method_str = "GetView";
    }

    return BaseCodes() + R"(
#include "precompile/builder.h"
)" + hash_map_include_str +
           R"(  
using namespace sparkcolumnarplugin::precompile;

class TypedGroupbyHashAggregateImpl : public CodeGenBase {
 public:
  TypedGroupbyHashAggregateImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    hash_table_ = )" +
           hash_map_define_str + R"(
  }

  arrow::Status Evaluate(const ArrayList& in, const std::shared_ptr<arrow::RecordBatch>& projected_batch) override {
    )" + evaluate_get_typed_array_str +
           evaluate_get_typed_key_array_str +
           R"(
    auto insert_on_found = [this)" +
           typed_input_parameter_str + R"(](int32_t i) {
      )" + compute_on_exists_str +
           R"(
    };
    auto insert_on_not_found = [this)" +
           typed_input_parameter_str + R"(](int32_t i) {
      )" + compute_on_new_str +
           R"(
      num_groups_ ++;
    };

    cur_id_ = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        hash_table_->GetOrInsert(typed_array->)" +
           evaluate_get_typed_key_method_str + R"((cur_id_), [](int32_t){},
                                 [](int32_t){}, &memo_index);
        if (memo_index < num_groups_) {
          insert_on_found(memo_index);
        } else {
          insert_on_not_found(memo_index);
        }
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          memo_index = hash_table_->GetOrInsertNull([](int32_t){}, [](int32_t){});
          if (memo_index < num_groups_) {
            insert_on_found(memo_index);
          } else {
            insert_on_not_found(memo_index);
          }
        } else {
          hash_table_->GetOrInsert(typed_array->)" +
           evaluate_get_typed_key_method_str + R"((cur_id_),
                                   [](int32_t){}, [](int32_t){},
                                   &memo_index);
        if (memo_index < num_groups_) {
          insert_on_found(memo_index);
        } else {
          insert_on_not_found(memo_index);
        }
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
        for (auto i = 0; i < num_groups_; i++) {)" +
           on_finish_str + R"(
        }
      *out = std::make_shared<HashAggregationResultIterator>(
        ctx_, schema, num_groups_,)" +
           on_finish_cached_parameter_str + R"(
    );
    return arrow::Status::OK();
  }

 private:
  )" + impl_cached_define_str +
           R"(
  arrow::compute::FunctionContext* ctx_;
  uint64_t num_groups_ = 0;
  uint64_t cur_id_ = 0;
  std::shared_ptr<)" +
           hash_map_type_str + R"(> hash_table_;

  class HashAggregationResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    HashAggregationResultIterator(
      arrow::compute::FunctionContext* ctx,
      std::shared_ptr<arrow::Schema> schema,
      uint64_t num_groups,
   )" + result_cached_parameter_str +
           R"(): ctx_(ctx), result_schema_(schema), total_length_(num_groups) {
     )" + result_cached_prepare_str +
           R"(
    }

    std::string ToString() override { return "HashAggregationResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > )" +
           std::to_string(GetBatchSize()) + R"( ? )" + std::to_string(GetBatchSize()) +
           R"( : (total_length_ - offset_);
      uint64_t count = 0;
      while (count < length) {
        )" +
           result_cached_to_builder_str + R"(
        count ++;
      }
      offset_ += length;
      )" + result_cached_to_array_str +
           R"(
      *out = arrow::RecordBatch::Make(result_schema_, length, {)" +
           result_cached_array_str + R"(});
      return arrow::Status::OK();
    }

   private:
   )" + result_cached_define_str +
           R"(
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
  };
};

extern "C" void MakeCodeGen(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedGroupbyHashAggregateImpl>(ctx);
}

    )";
  }

  arrow::Status GetInputOrdinalList(
      std::vector<std::shared_ptr<ActionCodeGen>> action_impl_list,
      std::vector<std::string>* input_ordinal_list) {
    for (auto action : action_impl_list) {
      for (auto name : action->GetInputDataNameList()) {
        input_ordinal_list->push_back(name);
      }
    }
    return arrow::Status::OK();
  }
  arrow::Status GetGroupKey(
      std::vector<std::shared_ptr<ActionCodeGen>> action_impl_list,
      std::vector<std::pair<gandiva::NodePtr, std::string>>* key_index_list) {
    for (auto action : action_impl_list) {
      if (action->IsGroupBy()) {
        key_index_list->push_back(std::make_pair(action->GetInputFieldList()[0],
                                                 action->GetInputDataNameList()[0]));
        // std::cout << action->GetInputFieldList()[0]->ToString() << std::endl;
      }
    }
    return arrow::Status::OK();
  }

  std::string GetParameterList(const std::vector<std::string>& cached_list_in,
                               bool pre_comma = true) {
    // filter not-empty cached_list
    std::vector<std::string> cached_list;
    for (auto s : cached_list_in) {
      if (s != "") {
        cached_list.push_back(s);
      }
    }
    std::stringstream ss;
    for (int i = 0; i < cached_list.size(); i++) {
      if (i != (cached_list.size() - 1)) {
        ss << cached_list[i] << ", ";
      } else {
        ss << cached_list[i];
      }
    }
    auto ret = ss.str();
    if (!pre_comma) {
      return ret;
    }
    if (ret.empty()) {
      return ret;
    } else {
      return ", " + ret;
    }
  }
};  // namespace extra

arrow::Status HashAggregateKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::shared_ptr<arrow::Schema> result_schema, std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashAggregateKernel>(ctx, input_field_list, action_list,
                                               result_schema);
  return arrow::Status::OK();
}

HashAggregateKernel::HashAggregateKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> input_field_list,
    std::vector<std::shared_ptr<gandiva::Node>> action_list,
    std::shared_ptr<arrow::Schema> result_schema) {
  impl_.reset(new Impl(ctx, input_field_list, action_list, result_schema));
  kernel_name_ = "HashAggregateKernelKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status HashAggregateKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status HashAggregateKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string HashAggregateKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
