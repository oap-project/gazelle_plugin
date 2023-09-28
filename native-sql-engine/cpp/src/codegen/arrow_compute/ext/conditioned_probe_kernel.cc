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

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/compute/api.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/array_appender.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/expression_codegen_visitor.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "codegen/common/hash_relation_number.h"
#include "codegen/common/hash_relation_string.h"
#include "precompile/unsafe_array.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedProbe  ////////////////
class ConditionedProbeKernel::Impl {
 public:
  Impl(arrow::compute::ExecContext* ctx, const gandiva::NodeVector& left_key_node_list,
       const gandiva::NodeVector& right_key_node_list,
       const gandiva::NodeVector& left_schema_node_list,
       const gandiva::NodeVector& right_schema_node_list,
       const gandiva::NodePtr& condition, int join_type, bool is_null_aware_anti_join,
       const gandiva::NodeVector& result_node_list,
       const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx)
      : ctx_(ctx),
        join_type_(join_type),
        condition_(condition),
        hash_relation_id_(hash_relation_idx),
        is_null_aware_anti_join_(is_null_aware_anti_join) {
    for (auto node : left_schema_node_list) {
      left_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }
    for (auto node : right_schema_node_list) {
      right_field_list_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }
    for (auto node : result_node_list) {
      result_schema_.push_back(
          std::dynamic_pointer_cast<gandiva::FieldNode>(node)->field());
    }

    auto hash_map_type_str = gandiva::ToString(
        std::dynamic_pointer_cast<gandiva::LiteralNode>(hash_configuration_list[0])
            ->holder());

    /////////// right_key_list may need to do precodegen /////////////
    gandiva::FieldVector right_key_list;
    /** two scenarios:
     *  1. hash_map_type 0 => SHJ probe with no condition and single join
     *  2. hash_map_type 1 => BHJ probe with no condition and single join
     **/
    pre_processed_key_ = true;

    right_key_project_codegen_ = GetGandivaKernel(right_key_node_list);
    right_key_hash_codegen_ = GetHash32Kernel(right_key_node_list);
    for (auto expr : right_key_project_codegen_) {
      key_hash_field_list_.push_back(expr->result());
    }

    /////////// map result_schema to input schema /////////////
    THROW_NOT_OK(
        GetIndexList(result_schema_, left_field_list_, &left_shuffle_index_list_));
    THROW_NOT_OK(
        GetIndexList(result_schema_, right_field_list_, &right_shuffle_index_list_));
    if (join_type != 4) {
      THROW_NOT_OK(GetIndexList(result_schema_, left_field_list_, right_field_list_,
                                false, &exist_index_, &result_schema_index_list_));
    } else {
      THROW_NOT_OK(GetIndexList(result_schema_, left_field_list_, right_field_list_, true,
                                &exist_index_, &result_schema_index_list_));
    }

    pool_ = nullptr;
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    if (condition_) {
      return arrow::Status::NotImplemented(
          "ConditionedProbeKernel(Non-Codegen) doesn't support condition.");
    }
    std::vector<gandiva::ExpressionVector> right_key_projector_list;
    std::shared_ptr<arrow::DataType> key_type;
    if (right_key_project_) {
      // hash_map_type == 0
      key_type = right_key_project_->return_type();
      right_key_projector_list.push_back({right_key_project_expr_});
    } else if (right_key_hash_codegen_) {
      // hash_map_type == 1
      key_type = right_key_hash_codegen_->result()->type();
      right_key_projector_list.push_back({right_key_hash_codegen_});
      right_key_projector_list.push_back(right_key_project_codegen_);
    } else {
      key_type = right_field_list_[right_key_index_list_[0]]->type();
    }
    *out = std::make_shared<ConditionedProbeResultIterator>(
        ctx_, right_key_index_list_, key_type, join_type_, is_null_aware_anti_join_,
        right_key_projector_list, result_schema_, result_schema_index_list_, exist_index_,
        left_field_list_, right_field_list_);
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
    bool cond_check = false;
    if (condition_) cond_check = true;
    // 1.0 prepare hash relation columns
    std::stringstream hash_prepare_ss;
    std::stringstream hash_define_ss;
    auto relation_list_name =
        "hash_relation_list_" + std::to_string(hash_relation_id_) + "_";
    hash_prepare_ss << "auto typed_dependent_iter_list_" << hash_relation_id_
                    << " = "
                       "std::dynamic_pointer_cast<ResultIterator<HashRelation>>("
                       "dependent_iter_list["
                    << hash_relation_id_ << "]);" << std::endl;
    hash_prepare_ss << "RETURN_NOT_OK(typed_dependent_iter_list_" << hash_relation_id_
                    << "->Next("
                    << "&" << relation_list_name << "));" << std::endl;

    hash_define_ss << "std::shared_ptr<HashRelation> " << relation_list_name << ";"
                   << std::endl;
    for (int i = 0; i < left_field_list_.size(); i++) {
      std::stringstream hash_relation_col_name_ss;
      hash_relation_col_name_ss << "hash_relation_" << hash_relation_id_ << "_" << i;
      auto hash_relation_col_name = hash_relation_col_name_ss.str();
      auto hash_relation_col_type = left_field_list_[i]->type();
      hash_define_ss << "std::shared_ptr<"
                     << GetTemplateString(hash_relation_col_type,
                                          "TypedHashRelationColumn", "Type", "arrow::")
                     << "> " << hash_relation_col_name << ";" << std::endl;
      hash_define_ss << "bool " << hash_relation_col_name << "_has_null;" << std::endl;
      hash_prepare_ss << "RETURN_NOT_OK(" << relation_list_name << "->GetColumn(" << i
                      << ", &" << hash_relation_col_name << "));" << std::endl;
      hash_prepare_ss << hash_relation_col_name
                      << "_has_null = " << hash_relation_col_name << "->HasNull();"
                      << std::endl;
    }
    codegen_ctx->relation_prepare_codes = hash_prepare_ss.str();

    codegen_ctx->definition_codes = hash_define_ss.str();
    // 1.1 prepare probe key column, name is key_0 and key_0_validity
    std::stringstream prepare_ss;

    std::vector<std::string> input_list;
    std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
        project_output_list;
    auto unsafe_row_name = "unsafe_row_" + std::to_string(hash_relation_id_);
    bool do_unsafe_row = true;
    if (right_key_project_codegen_.size() == 1) {
      // when right_key is single and not string, we don't need to use unsafeRow
      // chendi: But we still use name unsafe_row_${id} to pass key data
      prepare_ss << GetCTypeString(right_key_project_codegen_[0]->result()->type()) << " "
                 << unsafe_row_name << ";" << std::endl;
      prepare_ss << "bool " << unsafe_row_name << "_validity = false;" << std::endl;
      do_unsafe_row = false;
    } else {
      std::stringstream unsafe_row_define_ss;
      unsafe_row_define_ss << "std::shared_ptr<UnsafeRow> " << unsafe_row_name
                           << " = std::make_shared<UnsafeRow>("
                           << right_key_project_codegen_.size() << ");" << std::endl;
      codegen_ctx->unsafe_row_prepare_codes = unsafe_row_define_ss.str();
      prepare_ss << unsafe_row_name << "->reset();" << std::endl;
    }
    int idx = 0;
    for (auto expr : right_key_project_codegen_) {
      std::shared_ptr<ExpressionCodegenVisitor> project_node_visitor;
      auto is_local = false;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(
          expr->root(), &input, {right_field_list_}, -1, var_id, is_local, &input_list,
          &project_node_visitor));
      prepare_ss << project_node_visitor->GetPrepare();
      auto key_name = project_node_visitor->GetResult();
      auto validity_name = project_node_visitor->GetPreCheck();
      if (do_unsafe_row) {
        prepare_ss << "if (" << validity_name << ") {" << std::endl;
        prepare_ss << "appendToUnsafeRow(" << unsafe_row_name << ".get(), " << idx << ", "
                   << key_name << ");" << std::endl;
        prepare_ss << "} else {" << std::endl;
        prepare_ss << "setNullAt(" << unsafe_row_name << ".get(), " << idx << ");"
                   << std::endl;
        prepare_ss << "}" << std::endl;
      } else {
        prepare_ss << "if (" << validity_name << ") {" << std::endl;
        prepare_ss << unsafe_row_name << " = " << key_name << ";" << std::endl;
        prepare_ss << unsafe_row_name << "_validity = true;" << std::endl;
        prepare_ss << "}" << std::endl;
      }

      project_output_list.push_back(
          std::make_pair(std::make_pair(key_name, ""), nullptr));
      for (auto header : project_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                      header) == codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }
      idx++;
    }
    if (key_hash_field_list_.size() > 1) {
      std::shared_ptr<ExpressionCodegenVisitor> hash_node_visitor;
      auto is_local = false;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(
          right_key_hash_codegen_->root(), &project_output_list, {key_hash_field_list_},
          -1, var_id, is_local, &input_list, &hash_node_visitor));
      prepare_ss << hash_node_visitor->GetPrepare();
      auto key_name = hash_node_visitor->GetResult();
      auto validity_name = hash_node_visitor->GetPreCheck();
      prepare_ss << "auto key_" << hash_relation_id_ << " = " << key_name << ";"
                 << std::endl;
      prepare_ss << "auto key_" << hash_relation_id_ << "_validity = " << validity_name
                 << ";" << std::endl;
      /*for (auto header : hash_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(),
      codegen_ctx->header_codes.end(), header) ==
      codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }*/
    }
    codegen_ctx->prepare_codes = prepare_ss.str();
    /////   inside loop  //////
    // 2. probe in hash_relation
    RETURN_NOT_OK(GetProcessProbe(input, join_type_, cond_check, &codegen_ctx));
    // 3. do continue if not exists
    if (cond_check) {
      std::shared_ptr<ExpressionCodegenVisitor> condition_node_visitor;
      auto is_local = true;
      RETURN_NOT_OK(MakeExpressionCodegenVisitor(
          condition_, &input, {left_field_list_, right_field_list_}, hash_relation_id_,
          var_id, is_local, &prepare_list, &condition_node_visitor));
      auto function_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
      std::stringstream function_define_ss;
      function_define_ss << "bool " << function_name << "(ArrayItemIndex x, int y) {"
                         << std::endl;
      function_define_ss << condition_node_visitor->GetPrepare() << std::endl;
      function_define_ss << "return " << condition_node_visitor->GetResult() << ";"
                         << std::endl;
      function_define_ss << "}" << std::endl;
      codegen_ctx->function_list.push_back(function_define_ss.str());
      for (auto header : condition_node_visitor->GetHeaders()) {
        if (std::find(codegen_ctx->header_codes.begin(), codegen_ctx->header_codes.end(),
                      header) == codegen_ctx->header_codes.end()) {
          codegen_ctx->header_codes.push_back(header);
        }
      }
    }
    // set join output list for next kernel.
    ///////////////////////////////
    *codegen_ctx_out = codegen_ctx;
    return arrow::Status::OK();
  }

 private:
  arrow::compute::ExecContext* ctx_;
  arrow::MemoryPool* pool_;
  std::string signature_;
  int join_type_;

  gandiva::NodePtr condition_;
  bool is_null_aware_anti_join_ = false;
  int hash_map_type_;

  // only be used when hash_map_type_ == 0
  gandiva::ExpressionPtr right_key_project_expr_;
  gandiva::NodePtr right_key_project_;
  // only be used when hash_map_type_ == 1
  gandiva::ExpressionPtr right_key_hash_codegen_;
  gandiva::ExpressionVector right_key_project_codegen_;
  gandiva::FieldVector key_hash_field_list_;

  bool pre_processed_key_ = false;
  gandiva::FieldVector left_field_list_;
  gandiva::FieldVector right_field_list_;
  gandiva::FieldVector result_schema_;
  std::vector<int> right_key_index_list_;
  std::vector<int> left_shuffle_index_list_;
  std::vector<int> right_shuffle_index_list_;
  std::vector<std::pair<int, int>> result_schema_index_list_;
  int exist_index_ = -1;
  int hash_relation_id_;
  std::vector<arrow::ArrayVector> cached_;

  class ConditionedProbeResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ConditionedProbeResultIterator(
        arrow::compute::ExecContext* ctx, std::vector<int> right_key_index_list,
        std::shared_ptr<arrow::DataType> key_type, int join_type,
        bool is_null_aware_anti_join,
        std::vector<gandiva::ExpressionVector> right_key_project_list,
        gandiva::FieldVector result_schema,
        std::vector<std::pair<int, int>> result_schema_index_list, int exist_index,
        gandiva::FieldVector left_field_list, gandiva::FieldVector right_field_list)
        : ctx_(ctx),
          right_key_index_list_(right_key_index_list),
          key_type_(key_type),
          join_type_(join_type),
          is_null_aware_anti_join_(is_null_aware_anti_join),
          result_schema_index_list_(result_schema_index_list),
          exist_index_(exist_index),
          left_field_list_(left_field_list),
          right_field_list_(right_field_list) {
      result_schema_ = arrow::schema(result_schema);
      batch_size_ = GetBatchSize();

      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      for (auto expr : right_key_project_list[1]) {
        right_projected_field_list_.push_back(expr->result());
      }
      THROW_NOT_OK(gandiva::Projector::Make(arrow::schema(right_projected_field_list_),
                                            right_key_project_list[0], configuration,
                                            &right_hash_key_project_));
      THROW_NOT_OK(gandiva::Projector::Make(arrow::schema(right_field_list_),
                                            right_key_project_list[1], configuration,
                                            &right_keys_project_));
    }

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
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::StringType)
    arrow::Status SetDependencies(
        const std::vector<std::shared_ptr<ResultIteratorBase>>& dependent_iter_list) {
      auto iter = dependent_iter_list[0];
      auto typed_dependent =
          std::dynamic_pointer_cast<ResultIterator<HashRelation>>(iter);
      if (typed_dependent == nullptr) {
        throw JniPendingException("casting on hash relation iterator failed");
      }
      RETURN_NOT_OK(typed_dependent->Next(&hash_relation_));

      // chendi: previous result_schema_index_list design is little tricky, it
      // put existentce col at the back of all col while exists_index_ may be at
      // middle out real result. Add two index here.
      auto result_schema_length =
          (exist_index_ == -1 || exist_index_ == right_field_list_.size())
              ? result_schema_index_list_.size()
              : (result_schema_index_list_.size() - 1);
      int result_idx = 0;
      for (int i = 0; i < result_schema_length; i++) {
        auto pair = result_schema_index_list_[i];
        std::shared_ptr<arrow::DataType> type;
        AppenderBase::AppenderType appender_type;
        if (result_idx++ == exist_index_) {
          appender_type = AppenderBase::exist;
          type = arrow::boolean();
          if (result_idx < result_schema_index_list_.size()) i -= 1;
        } else {
          appender_type = pair.first == 0 ? AppenderBase::left : AppenderBase::right;
          if (pair.first == 0) {
            type = left_field_list_[pair.second]->type();
          } else {
            type = right_field_list_[pair.second]->type();
          }
        }

        std::shared_ptr<AppenderBase> appender;
        RETURN_NOT_OK(MakeAppender(ctx_, type, appender_type, &appender));
        // insert all left arrays
        if (pair.first == 0) {
          arrow::ArrayVector cached;
          RETURN_NOT_OK(hash_relation_->GetArrayVector(pair.second, &cached));
          for (auto arr : cached) {
            appender->AddArray(arr);
          }
        }
        appender_list_.push_back(appender);
      }

      // prepare probe function

      // if hash_map_type == 1, we will simply use HashRelation
      switch (join_type_) {
        case 0: { /*Inner Join*/
          auto func =
              std::make_shared<UnsafeInnerProbeFunction>(hash_relation_, appender_list_);
          probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
        } break;
        case 1: { /*Outer Join*/
          auto func =
              std::make_shared<UnsafeOuterProbeFunction>(hash_relation_, appender_list_);
          probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
        } break;
        case 2: { /*Anti Join*/
          auto func = std::make_shared<UnsafeAntiProbeFunction>(
              hash_relation_, appender_list_, is_null_aware_anti_join_);
          probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
        } break;
        case 3: { /*Semi Join*/
          auto func =
              std::make_shared<UnsafeSemiProbeFunction>(hash_relation_, appender_list_);
          probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
        } break;
        case 4: { /*Existence Join*/
          auto func = std::make_shared<UnsafeExistenceProbeFunction>(hash_relation_,
                                                                     appender_list_);
          probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
        } break;
        default:
          return arrow::Status::NotImplemented(
              "ConditionedProbeArraysTypedImpl only support join type: "
              "InnerJoin, "
              "RightJoin");
      }

      return arrow::Status::OK();
    }
#undef PROCESS_SUPPORTED_TYPES

    arrow::Status Process(
        const std::vector<std::shared_ptr<arrow::Array>>& in,
        std::shared_ptr<arrow::RecordBatch>* out,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      // Get key array, which should be typed
      std::shared_ptr<arrow::Array> key_array;
      arrow::ArrayVector projected_keys_outputs;
      /**
       * if hash_map_type_ == 0, we only need to build a single-column hashArray
       *for key if hash_map_type_ == 1, we need to both get a single-column
       *hashArray and projected result of original keys for hashmap
       **/
      arrow::ArrayVector outputs;
      auto length = in.size() > 0 ? in[0]->length() : 0;
      std::shared_ptr<arrow::RecordBatch> in_batch =
          arrow::RecordBatch::Make(arrow::schema(right_field_list_), length, in);

      RETURN_NOT_OK(right_keys_project_->Evaluate(*in_batch, ctx_->memory_pool(),
                                                  &projected_keys_outputs));
      in_batch = arrow::RecordBatch::Make(arrow::schema(right_projected_field_list_),
                                          in_batch->num_rows(), projected_keys_outputs);
      RETURN_NOT_OK(
          right_hash_key_project_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      key_array = outputs[0];

      // put in to ArrayAppender then doing evaluate
      for (int tmp_idx = 0; tmp_idx < appender_list_.size(); tmp_idx++) {
        auto appender = appender_list_[tmp_idx];
        if (appender->GetType() == AppenderBase::right) {
          auto idx_exclude_exist =
              (exist_index_ == -1 || tmp_idx < exist_index_) ? tmp_idx : (tmp_idx - 1);
          auto right_in_idx = result_schema_index_list_[idx_exclude_exist].second;
          RETURN_NOT_OK(appender->AddArray(in[right_in_idx]));
        }
      }
      uint64_t out_length = 0;

      out_length = probe_func_->Evaluate(key_array, projected_keys_outputs);

      arrow::ArrayVector out_arr_list;
      for (auto appender : appender_list_) {
        std::shared_ptr<arrow::Array> out_arr;
        RETURN_NOT_OK(appender->Finish(&out_arr));
        out_arr_list.push_back(out_arr);
        if (appender->GetType() == AppenderBase::right) {
          RETURN_NOT_OK(appender->PopArray());
        }
        RETURN_NOT_OK(appender->Reset());
      }
      *out = record_batch_holder_ =
          arrow::RecordBatch::Make(result_schema_, out_length, out_arr_list);
      // Initialize for iterator
      record_batch_holder_length_ = out_length;
      record_batch_holder_offset_ = 0;
      return arrow::Status::OK();
    }

    bool HasNext() {
      if (record_batch_holder_offset_ >= record_batch_holder_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      if (record_batch_holder_length_ <= batch_size_) {
        *out = record_batch_holder_;
        record_batch_holder_offset_ = record_batch_holder_length_;
        return arrow::Status::OK();
      }
      auto length =
          (record_batch_holder_length_ - record_batch_holder_offset_) > batch_size_
              ? batch_size_
              : (record_batch_holder_length_ - record_batch_holder_offset_);
      std::vector<std::shared_ptr<arrow::Array>> out_arrs;
      for (auto arr : record_batch_holder_->columns()) {
        std::shared_ptr<arrow::Array> copied;
        auto sliced = arr->Slice(record_batch_holder_offset_, length);
        RETURN_NOT_OK(arrow::Concatenate({sliced}, ctx_->memory_pool(), &copied));
        out_arrs.push_back(copied);
      }
      *out = arrow::RecordBatch::Make(result_schema_, length, out_arrs);
      record_batch_holder_offset_ += length;
      return arrow::Status::OK();
    }

   private:
    class ProbeFunctionBase {
     public:
      virtual ~ProbeFunctionBase() {}
      virtual uint64_t Evaluate(std::shared_ptr<arrow::Array>) { return 0; }
      virtual uint64_t Evaluate(std::shared_ptr<arrow::Array>,
                                const arrow::ArrayVector&) {
        return 0;
      }
    };
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
  PROCESS(arrow::TimestampType)
    class UnsafeInnerProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeInnerProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                               std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                       \
  case TypeTraits<InType>::type_id: {                                         \
    using ArrayType_ = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType_>(key_payloads[0]); \
    if (typed_first_key_arr->null_count() == 0) {                             \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        return hash_relation_->Get(typed_key_array->GetView(i),               \
                                   typed_first_key_arr->GetView(i));          \
      };                                                                      \
    } else {                                                                  \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        if (typed_first_key_arr->IsNull(i)) {                                 \
          return hash_relation_->GetNull();                                   \
        } else {                                                              \
          return hash_relation_->Get(typed_key_array->GetView(i),             \
                                     typed_first_key_arr->GetView(i));        \
        }                                                                     \
      };                                                                      \
    }                                                                         \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              if (typed_first_key_arr->null_count() == 0) {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  return hash_relation_->Get(typed_key_array->GetView(i),
                                             typed_first_key_arr->GetString(i));
                };
              } else {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  if (typed_first_key_arr->IsNull(i)) {
                    return hash_relation_->GetNull();
                  } else {
                    return hash_relation_->Get(typed_key_array->GetView(i),
                                               typed_first_key_arr->GetString(i));
                  }
                };
              }
            } break;
            default: {
              throw JniPendingException(
                  "UnsafeInnerProbeFunction Evaluate doesn't support single " +
                  key_payloads[0]->type()->ToString());
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());

        if (do_unsafe_row) {
          for (int i = 0; i < key_array->length(); i++) {
            unsafe_key_row->reset();
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            int index = hash_relation_->Get(typed_key_array->GetView(i), unsafe_key_row);
            if (index == -1) {
              continue;
            }
            auto index_list = hash_relation_->GetItemListByIndex(index);
            // TODO(): move this out of the loop
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(index_list));
              } else {
                THROW_NOT_OK(appender->Append(0, i, index_list.size()));
              }
            }
            out_length += index_list.size();
          }
        } else {
          for (int i = 0; i < key_array->length(); i++) {
            int index = fast_probe(i);
            if (index == -1) {
              continue;
            }
            auto index_list = hash_relation_->GetItemListByIndex(index);
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(index_list));
              } else {
                THROW_NOT_OK(appender->Append(0, i, index_list.size()));
              }
            }
            out_length += index_list.size();
          }
        }

        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };
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
  PROCESS(arrow::TimestampType)
    class UnsafeOuterProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeOuterProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                               std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                       \
  case TypeTraits<InType>::type_id: {                                         \
    using ArrayType_ = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType_>(key_payloads[0]); \
    if (typed_first_key_arr) {                                                \
      if (typed_first_key_arr->null_count() == 0) {                           \
        fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {    \
          return hash_relation_->Get(typed_key_array->GetView(i),             \
                                     typed_first_key_arr->GetView(i));        \
        };                                                                    \
      } else {                                                                \
        fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {    \
          if (typed_first_key_arr->IsNull(i)) {                               \
            return hash_relation_->GetNull();                                 \
          } else {                                                            \
            return hash_relation_->Get(typed_key_array->GetView(i),           \
                                       typed_first_key_arr->GetView(i));      \
          }                                                                   \
        };                                                                    \
      }                                                                       \
    }                                                                         \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              if (typed_first_key_arr) {
                if (typed_first_key_arr->null_count() == 0) {
                  fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                    return hash_relation_->Get(typed_key_array->GetView(i),
                                               typed_first_key_arr->GetString(i));
                  };
                } else {
                  fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                    if (typed_first_key_arr->IsNull(i)) {
                      return hash_relation_->GetNull();
                    } else {
                      return hash_relation_->Get(typed_key_array->GetView(i),
                                                 typed_first_key_arr->GetString(i));
                    }
                  };
                }
              }
            } break;
            default: {
              throw JniPendingException(
                  "UnsafeOuterProbeFunction Evaluate doesn't support single " +
                  key_payloads[0]->type()->ToString());
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
        if (do_unsafe_row) {
          for (int i = 0; i < key_array->length(); i++) {
            unsafe_key_row->reset();
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            int index = hash_relation_->Get(typed_key_array->GetView(i), unsafe_key_row);
            if (index == -1) {
              for (auto appender : appender_list_) {
                if (appender->GetType() == AppenderBase::left) {
                  THROW_NOT_OK(appender->AppendNull());
                } else {
                  THROW_NOT_OK(appender->Append(0, i));
                }
              }
              out_length += 1;
              continue;
            }
            auto index_list = hash_relation_->GetItemListByIndex(index);
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(index_list));
              } else {
                THROW_NOT_OK(appender->Append(0, i, index_list.size()));
              }
            }
            out_length += index_list.size();
          }
        } else {
          for (int i = 0; i < key_array->length(); i++) {
            int index = fast_probe(i);

            if (index == -1) {
              for (auto appender : appender_list_) {
                if (appender->GetType() == AppenderBase::left) {
                  THROW_NOT_OK(appender->AppendNull());
                } else {
                  THROW_NOT_OK(appender->Append(0, i));
                }
              }
              out_length += 1;
              continue;
            }
            auto index_list = hash_relation_->GetItemListByIndex(index);
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->Append(index_list));
              } else {
                THROW_NOT_OK(appender->Append(0, i, index_list.size()));
              }
            }
            out_length += index_list.size();
          }
        }

        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    /******************* Rules for Anti Join *******************/
    /** The config naming "is_null_aware_anti_join" from Spark decides how to
     * handle null value. This config is true if below conditions are satisfied:
      if (is_null_aware_anti_join) {
        require(leftKeys.length == 1, "leftKeys length should be 1")
        require(rightKeys.length == 1, "rightKeys length should be 1")
        require(joinType == LeftAnti, "joinType must be LeftAnti.")
        require(buildSide == BuildRight, "buildSide must be BuildRight.")
        require(condition.isEmpty, "null aware anti join optimize condition should be
    empty.")
      }
    * If this config is true, there are four cases:
      (1) If hash table is empty, return stream side;
      (2) Else if existing key in hash table being null, no row will be joined;
      (3) Else if the key from stream side is null, this row will not be joined;
      (4) Else if the key from stream side cannot be found in build side, this row will be
    joined.
     * If this config is false, there are three cases:
      (1) If key from stream side contains null, this row will be joined;
      (2) Else if the key from stream side cannot be found in build side, this row will be
    joined;
      (3) Else the key from stream side does not statisfy the condition, this row
    will be joined.
    */

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
  PROCESS(arrow::TimestampType)
    class UnsafeAntiProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeAntiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                              std::vector<std::shared_ptr<AppenderBase>> appender_list,
                              bool is_null_aware_anti_join)
          : hash_relation_(hash_relation),
            appender_list_(appender_list),
            is_null_aware_anti_join_(is_null_aware_anti_join) {}

      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                       \
  case TypeTraits<InType>::type_id: {                                         \
    using ArrayType_ = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType_>(key_payloads[0]); \
    if (typed_first_key_arr->null_count() == 0) {                             \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        return hash_relation_->IfExists(typed_key_array->GetView(i),          \
                                        typed_first_key_arr->GetView(i));     \
      };                                                                      \
    } else {                                                                  \
      if (is_null_aware_anti_join_) {                                         \
        fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {    \
          if (typed_first_key_arr->IsNull(i)) {                               \
            return 0;                                                         \
          } else {                                                            \
            return hash_relation_->IfExists(typed_key_array->GetView(i),      \
                                            typed_first_key_arr->GetView(i)); \
          }                                                                   \
        };                                                                    \
      } else {                                                                \
        fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {    \
          if (typed_first_key_arr->IsNull(i)) {                               \
            return -1;                                                        \
          } else {                                                            \
            return hash_relation_->IfExists(typed_key_array->GetView(i),      \
                                            typed_first_key_arr->GetView(i)); \
          }                                                                   \
        };                                                                    \
      }                                                                       \
    }                                                                         \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              if (typed_first_key_arr->null_count() == 0) {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                  typed_first_key_arr->GetString(i));
                };
              } else {
                if (is_null_aware_anti_join_) {
                  fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                    if (typed_first_key_arr->IsNull(i)) {
                      return 0;
                    } else {
                      return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                      typed_first_key_arr->GetString(i));
                    }
                  };
                } else {
                  fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                    if (typed_first_key_arr->IsNull(i)) {
                      return -1;
                    } else {
                      return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                      typed_first_key_arr->GetString(i));
                    }
                  };
                }
              }
            } break;
            default: {
              throw JniPendingException(
                  "UnsafeAntiProbeFunction Evaluate doesn't support single key " +
                  key_payloads[0]->type()->ToString());
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
            if (arr->null_count() > 0) {
              has_null_list.push_back(true);
            } else {
              has_null_list.push_back(false);
            }
          }
        }
        uint64_t out_length = 0;
        auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
        if (do_unsafe_row) {
          for (int i = 0; i < key_array->length(); i++) {
            int index;

            for (int colIdx = 0; colIdx < payloads.size(); colIdx++) {
              if (has_null_list[colIdx] && key_payloads[colIdx]->IsNull(i)) {
                // If the keys in stream side contains null, will join this row.
                index = -1;
              } else {
                unsafe_key_row->reset();
                for (auto payload_arr : payloads) {
                  payload_arr->Append(i, &unsafe_key_row);
                }
                index =
                    hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);
              }
            }

            if (index == -1) {
              for (auto appender : appender_list_) {
                if (appender->GetType() == AppenderBase::left) {
                  THROW_NOT_OK(appender->AppendNull());
                } else {
                  THROW_NOT_OK(appender->Append(0, i));
                }
              }
              out_length += 1;
            }
          }
        } else {
          for (int i = 0; i < key_array->length(); i++) {
            int index = getSingleKeyIndex(fast_probe, i);

            if (index == -1) {
              for (auto appender : appender_list_) {
                if (appender->GetType() == AppenderBase::left) {
                  THROW_NOT_OK(appender->AppendNull());
                } else {
                  THROW_NOT_OK(appender->Append(0, i));
                }
              }
              out_length += 1;
            }
          }
        }

        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      bool is_null_aware_anti_join_ = false;
      std::vector<bool> has_null_list;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;

      int getSingleKeyIndex(std::function<int(int i)>& fast_probe, int i) {
        if (!is_null_aware_anti_join_) {
          return fast_probe(i);
        } else {
          if (hash_relation_->GetHashTableSize() == 0 &&
              hash_relation_->GetRealNull() != 0) {
            // If hash table is empty, will return stream side.
            return -1;
          } else if (hash_relation_->GetRealNull() == 0) {
            // If build side has null key, will not join any row.
            return 0;
          } else {
            return fast_probe(i);
          }
        }
      }
    };

    class UnsafeSemiProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeSemiProbeFunction(std::shared_ptr<HashRelation> hash_relation,
                              std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
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
  PROCESS(arrow::TimestampType)
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<arrow::Int32Array>(key_array);
        assert(typed_key_array != nullptr);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                       \
  case TypeTraits<InType>::type_id: {                                         \
    using ArrayType_ = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType_>(key_payloads[0]); \
    if (typed_first_key_arr->null_count() == 0) {                             \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        return hash_relation_->IfExists(typed_key_array->GetView(i),          \
                                        typed_first_key_arr->GetView(i));     \
      };                                                                      \
    } else {                                                                  \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        if (typed_first_key_arr->IsNull(i)) {                                 \
          return hash_relation_->GetNull();                                   \
        } else {                                                              \
          return hash_relation_->IfExists(typed_key_array->GetView(i),        \
                                          typed_first_key_arr->GetView(i));   \
        }                                                                     \
      };                                                                      \
    }                                                                         \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              if (typed_first_key_arr->null_count() == 0) {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                  typed_first_key_arr->GetString(i));
                };
              } else {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  if (typed_first_key_arr->IsNull(i)) {
                    return hash_relation_->GetNull();
                  } else {
                    return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                    typed_first_key_arr->GetString(i));
                  }
                };
              }
            } break;
            default: {
              throw JniPendingException(
                  "UnsafeSemiProbeFunction Evaluate doesn't support single key " +
                  key_payloads[0]->type()->ToString());
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }

        uint64_t out_length = 0;
        auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
        if (do_unsafe_row) {
          for (int i = 0; i < key_array->length(); i++) {
            unsafe_key_row->reset();
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }

            int index =
                hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);

            if (index == -1) {
              continue;
            }
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        } else {
          for (int i = 0; i < key_array->length(); i++) {
            int index = fast_probe(i);

            if (index == -1) {
              continue;
            }
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                THROW_NOT_OK(appender->AppendNull());
              } else {
                THROW_NOT_OK(appender->Append(0, i));
              }
            }
            out_length += 1;
          }
        }

        return out_length;
      }

     private:
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };
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
  PROCESS(arrow::TimestampType)
    class UnsafeExistenceProbeFunction : public ProbeFunctionBase {
     public:
      UnsafeExistenceProbeFunction(
          std::shared_ptr<HashRelation> hash_relation,
          std::vector<std::shared_ptr<AppenderBase>> appender_list)
          : hash_relation_(hash_relation), appender_list_(appender_list) {}
      uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array,
                        const arrow::ArrayVector& key_payloads) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);
        std::vector<std::shared_ptr<UnsafeArray>> payloads;
        int i = 0;
        bool do_unsafe_row = true;
        std::function<int(int i)> fast_probe;
        /* for single key case, we don't need to create unsafeRow */
        if (key_payloads.size() == 1) {
          do_unsafe_row = false;
          switch (key_payloads[0]->type_id()) {
#define PROCESS(InType)                                                       \
  case TypeTraits<InType>::type_id: {                                         \
    using ArrayType_ = precompile::TypeTraits<InType>::ArrayType;             \
    auto typed_first_key_arr = std::make_shared<ArrayType_>(key_payloads[0]); \
    if (typed_first_key_arr->null_count() == 0) {                             \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        return hash_relation_->IfExists(typed_key_array->GetView(i),          \
                                        typed_first_key_arr->GetView(i));     \
      };                                                                      \
    } else {                                                                  \
      fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {      \
        if (typed_first_key_arr->IsNull(i)) {                                 \
          return hash_relation_->GetNull();                                   \
        } else {                                                              \
          return hash_relation_->IfExists(typed_key_array->GetView(i),        \
                                          typed_first_key_arr->GetView(i));   \
        }                                                                     \
      };                                                                      \
    }                                                                         \
  } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
            case TypeTraits<arrow::StringType>::type_id: {
              auto typed_first_key_arr = std::make_shared<StringArray>(key_payloads[0]);
              if (typed_first_key_arr->null_count() == 0) {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                  typed_first_key_arr->GetString(i));
                };
              } else {
                fast_probe = [this, typed_key_array, typed_first_key_arr](int i) {
                  if (typed_first_key_arr->IsNull(i)) {
                    return hash_relation_->GetNull();
                  } else {
                    return hash_relation_->IfExists(typed_key_array->GetView(i),
                                                    typed_first_key_arr->GetString(i));
                  }
                };
              }
            } break;
            default: {
              throw JniPendingException(
                  "UnsafeExistenceProbeFunction Evaluate doesn't support single key " +
                  key_payloads[0]->type()->ToString());
            } break;
          }
#undef PROCESS_SUPPORTED_TYPES
        } else {
          for (auto arr : key_payloads) {
            std::shared_ptr<UnsafeArray> payload;
            MakeUnsafeArray(arr->type(), i++, arr, &payload);
            payloads.push_back(payload);
          }
        }
        uint64_t out_length = 0;
        auto unsafe_key_row = std::make_shared<UnsafeRow>(payloads.size());
        if (do_unsafe_row) {
          for (int i = 0; i < key_array->length(); i++) {
            unsafe_key_row->reset();
            for (auto payload_arr : payloads) {
              payload_arr->Append(i, &unsafe_key_row);
            }
            int index =
                hash_relation_->IfExists(typed_key_array->GetView(i), unsafe_key_row);

            bool exists = true;
            if (index == -1) {
              exists = false;
            }
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::exist) {
                THROW_NOT_OK(appender->AppendExistence(exists));
              } else if (appender->GetType() == AppenderBase::right) {
                THROW_NOT_OK(appender->Append(0, i));
              } else {
                THROW_NOT_OK(appender->AppendNull());
              }
            }
            out_length += 1;
          }
        } else {
          for (int i = 0; i < key_array->length(); i++) {
            int index = fast_probe(i);

            bool exists = true;
            if (index == -1) {
              exists = false;
            }
            for (auto appender : appender_list_) {
              if (appender->GetType() == AppenderBase::exist) {
                THROW_NOT_OK(appender->AppendExistence(exists));
              } else if (appender->GetType() == AppenderBase::right) {
                THROW_NOT_OK(appender->Append(0, i));
              } else {
                THROW_NOT_OK(appender->AppendNull());
              }
            }
            out_length += 1;
          }
        }

        return out_length;
      }

     private:
      using ArrayType = arrow::Int32Array;
      std::shared_ptr<HashRelation> hash_relation_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    };

    arrow::compute::ExecContext* ctx_;
    int join_type_;
    bool is_null_aware_anti_join_ = false;
    std::vector<int> right_key_index_list_;
    // used for hash key to hashMap probe
    int hash_map_type_ = 0;
    std::shared_ptr<gandiva::Projector> right_hash_key_project_;
    std::shared_ptr<gandiva::Projector> right_keys_project_;

    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<HashRelation> hash_relation_;

    std::shared_ptr<arrow::Schema> result_schema_;
    std::vector<std::pair<int, int>> result_schema_index_list_;
    int exist_index_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;

    gandiva::FieldVector left_field_list_;
    gandiva::FieldVector right_field_list_;
    gandiva::FieldVector right_projected_field_list_;
    std::shared_ptr<ProbeFunctionBase> probe_func_;

    std::shared_ptr<arrow::RecordBatch> record_batch_holder_;
    uint64_t batch_size_;
    uint64_t record_batch_holder_length_ = 0;
    uint64_t record_batch_holder_offset_ = 0;
  };

  arrow::Status GetInnerJoin(bool cond_check, std::string index_name,
                             std::string hash_relation_name,
                             std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto item_index_list_name = index_name + "_item_list";
    auto range_index_name = "range_" + std::to_string(hash_relation_id_) + "_i";
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    if (key_hash_field_list_.size() == 1) {
      codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
               << hash_relation_name << "->Get(unsafe_row_" << hash_relation_id_
               << "):-1;" << std::endl;
    } else {
      codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
               << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
               << std::endl;
    }
    codes_ss << "if (" << index_name << " == -1) { continue; }" << std::endl;
    codes_ss << "auto " << item_index_list_name << " = " << hash_relation_name
             << "->GetItemListByIndex(" << index_name << ");" << std::endl;
    codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < "
             << item_index_list_name << ".size(); " << range_index_name << "++) {"
             << std::endl;
    codes_ss << tmp_name << " = " << item_index_list_name << "[" << range_index_name
             << "];" << std::endl;
    if (cond_check) {
      auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
      codes_ss << "if (!" << condition_name << "(" << tmp_name << ", i)) {" << std::endl;
      codes_ss << "  continue;" << std::endl;
      codes_ss << "}" << std::endl;
    }
    finish_codes_ss << "} // end of Inner Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetOuterJoin(bool cond_check, std::string index_name,
                             std::string hash_relation_name,
                             std::shared_ptr<CodeGenContext>* output) {
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;

    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto is_outer_null_name = "is_outer_null_" + std::to_string(hash_relation_id_);
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto item_index_list_name = index_name + "_item_list";
    auto range_index_name = "range_" + std::to_string(hash_relation_id_) + "_i";
    auto range_size_name = "range_" + std::to_string(hash_relation_id_) + "_size";

    codes_ss << "int32_t " << index_name << ";" << std::endl;
    codes_ss << "std::vector<ArrayItemIndex> " << item_index_list_name << ";"
             << std::endl;
    if (key_hash_field_list_.size() == 1) {
      codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
               << hash_relation_name << "->Get(unsafe_row_" << hash_relation_id_
               << "):-1;" << std::endl;
    } else {
      codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
               << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
               << std::endl;
    }
    codes_ss << "auto " << range_size_name << " = 1;" << std::endl;
    codes_ss << "if (" << index_name << " != -1) {" << std::endl;
    codes_ss << item_index_list_name << " = " << hash_relation_name
             << "->GetItemListByIndex(" << index_name << ");" << std::endl;
    codes_ss << range_size_name << " = " << item_index_list_name << ".size();"
             << std::endl;
    codes_ss << "}" << std::endl;
    if (cond_check) {
      codes_ss << "int outer_join_num_matches = 0;" << std::endl;
    }
    codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < "
             << range_size_name << "; " << range_index_name << "++) {" << std::endl;
    codes_ss << "if (!" << item_index_list_name << ".empty()) {" << std::endl;
    codes_ss << tmp_name << " = " << item_index_list_name << "[" << range_index_name
             << "];" << std::endl;
    codes_ss << is_outer_null_name << " = false;" << std::endl;
    if (cond_check) {
      codes_ss << "if (!" << condition_name << "(" << tmp_name << ", i)) {" << std::endl;
      // if all matches do not pass condition check, null will be appended later
      codes_ss << "if ((" << range_index_name << " + 1 == " << range_size_name
               << ") && outer_join_num_matches == 0) {" << std::endl;
      codes_ss << is_outer_null_name << " = true;" << std::endl;
      codes_ss << "} else {" << std::endl;
      codes_ss << "  continue;" << std::endl;
      codes_ss << "}\n}" << std::endl;
      codes_ss << "outer_join_num_matches++;" << std::endl;
    }
    codes_ss << "} else {" << std::endl;
    codes_ss << is_outer_null_name << " = true;" << std::endl;
    codes_ss << "}" << std::endl;
    finish_codes_ss << "} // end of Outer Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetAntiJoin(bool cond_check, std::string index_name,
                            std::string hash_relation_name,
                            std::shared_ptr<CodeGenContext>* output,
                            bool is_null_aware_anti_join = false) {
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto item_index_list_name = index_name + "_item_list";
    auto range_index_name = "range_" + std::to_string(hash_relation_id_) + "_i";
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    if (cond_check) {
      if (key_hash_field_list_.size() == 1) {
        codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                 << hash_relation_name << "->Get(unsafe_row_" << hash_relation_id_
                 << ") : -1;" << std::endl;
      } else {
        codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    } else {
      if (key_hash_field_list_.size() == 1) {
        if (!is_null_aware_anti_join) {
          codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                   << hash_relation_name << "->IfExists(unsafe_row_" << hash_relation_id_
                   << ") : -1;" << std::endl;
        } else {
          codes_ss << "if (" << hash_relation_name << "->GetHashTableSize() == 0 && "
                   << hash_relation_name << "->GetRealNull() != 0"
                   << ") {\n"
                   << index_name << " = -1;\n"
                   << "} else if (" << hash_relation_name << "->GetRealNull() == 0) {\n"
                   << index_name << " = 0;\n"
                   << "} else {\n"
                   << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                   << hash_relation_name << "->IfExists(unsafe_row_" << hash_relation_id_
                   << ") : 0;\n"
                   << "}" << std::endl;
        }
      } else {
        // If any key from stream side is null, this row should be joined.
        // Since null rows are skipped when building hash table (see hash_relation:
        // AppendKeyColumn), this works for both key-has-null and key-has-no-null cases.
        codes_ss << index_name << " = " << hash_relation_name << "->IfExists(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    }
    if (cond_check) {
      codes_ss << "if (" << index_name << " != -1) {" << std::endl;
      codes_ss << "  bool found = false;" << std::endl;
      codes_ss << "auto " << item_index_list_name << " = " << hash_relation_name
               << "->GetItemListByIndex(" << index_name << ");" << std::endl;
      codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < "
               << item_index_list_name << ".size(); " << range_index_name << "++) {"
               << std::endl;
      codes_ss << tmp_name << " = " << item_index_list_name << "[" << range_index_name
               << "];" << std::endl;
      codes_ss << "    if (" << condition_name << "(" << tmp_name << ", i)) {"
               << std::endl;
      codes_ss << "      found = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
      codes_ss << "if (found) continue;" << std::endl;
      codes_ss << "}" << std::endl;
    } else {
      codes_ss << "if (" << index_name << " != -1) {" << std::endl;
      codes_ss << "  continue;" << std::endl;
      codes_ss << "}" << std::endl;
    }
    codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < 1;"
             << range_index_name << "++) {" << std::endl;
    finish_codes_ss << "} // end of Anti Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetSemiJoin(bool cond_check, std::string index_name,
                            std::string hash_relation_name,
                            std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto item_index_list_name = index_name + "_item_list";
    auto range_index_name = "range_" + std::to_string(hash_relation_id_) + "_i";
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    if (cond_check) {
      if (key_hash_field_list_.size() == 1) {
        codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                 << hash_relation_name << "->Get(unsafe_row_" << hash_relation_id_
                 << "):-1;" << std::endl;
      } else {
        codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    } else {
      if (key_hash_field_list_.size() == 1) {
        codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                 << hash_relation_name << "->IfExists(unsafe_row_" << hash_relation_id_
                 << "):-1;" << std::endl;
      } else {
        codes_ss << index_name << " = " << hash_relation_name << "->IfExists(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    }
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << "continue;" << std::endl;
    if (cond_check) {
      codes_ss << "} else {" << std::endl;
      codes_ss << "  bool found = false;" << std::endl;
      codes_ss << "auto " << item_index_list_name << " = " << hash_relation_name
               << "->GetItemListByIndex(" << index_name << ");" << std::endl;
      codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < "
               << item_index_list_name << ".size(); " << range_index_name << "++) {"
               << std::endl;
      codes_ss << tmp_name << " = " << item_index_list_name << "[" << range_index_name
               << "];" << std::endl;
      codes_ss << "    if (" << condition_name << "(" << tmp_name << ", i)) {"
               << std::endl;
      codes_ss << "      found = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
      codes_ss << "  if (!found) {" << std::endl;
      codes_ss << "    continue;" << std::endl;
      codes_ss << "  }" << std::endl;
    }
    codes_ss << "}" << std::endl;
    codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < 1;"
             << range_index_name << "++) {" << std::endl;
    finish_codes_ss << "} // end of Semi Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetExistenceJoin(bool cond_check, std::string index_name,
                                 std::string hash_relation_name,
                                 std::shared_ptr<CodeGenContext>* output) {
    std::stringstream shuffle_ss;
    std::stringstream codes_ss;
    std::stringstream finish_codes_ss;
    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto condition_name = "ConditionCheck_" + std::to_string(hash_relation_id_);
    auto item_index_list_name = index_name + "_item_list";
    auto range_index_name = "range_" + std::to_string(hash_relation_id_) + "_i";
    auto exist_name =
        "hash_relation_" + std::to_string(hash_relation_id_) + "_existence_value";
    auto exist_validity = exist_name + "_validity";
    codes_ss << "int32_t " << index_name << ";" << std::endl;
    if (cond_check) {
      if (key_hash_field_list_.size() == 1) {
        codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                 << hash_relation_name << "->Get(unsafe_row_" << hash_relation_id_
                 << "):-1;" << std::endl;
      } else {
        codes_ss << index_name << " = " << hash_relation_name << "->Get(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    } else {
      if (key_hash_field_list_.size() == 1) {
        codes_ss << index_name << " = unsafe_row_" << hash_relation_id_ << "_validity?"
                 << hash_relation_name << "->IfExists(unsafe_row_" << hash_relation_id_
                 << "):-1;" << std::endl;
      } else {
        codes_ss << index_name << " = " << hash_relation_name << "->IfExists(key_"
                 << hash_relation_id_ << ", unsafe_row_" << hash_relation_id_ << ");"
                 << std::endl;
      }
    }
    codes_ss << "bool " << exist_name << " = false;" << std::endl;
    codes_ss << "bool " << exist_validity << " = true;" << std::endl;
    codes_ss << "if (" << index_name << " == -1) {" << std::endl;
    codes_ss << exist_name << " = false;" << std::endl;
    if (cond_check) {
      codes_ss << "} else {" << std::endl;
      codes_ss << "auto " << item_index_list_name << " = " << hash_relation_name
               << "->GetItemListByIndex(" << index_name << ");" << std::endl;
      codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < "
               << item_index_list_name << ".size(); " << range_index_name << "++) {"
               << std::endl;
      codes_ss << tmp_name << " = " << item_index_list_name << "[" << range_index_name
               << "];" << std::endl;
      codes_ss << "    if (" << condition_name << "(" << tmp_name << ", i)) {"
               << std::endl;
      codes_ss << "      " << exist_name << " = true;" << std::endl;
      codes_ss << "      break;" << std::endl;
      codes_ss << "    }" << std::endl;
      codes_ss << "  }" << std::endl;
    } else {
      codes_ss << "} else {" << std::endl;
      codes_ss << exist_name << " = true;" << std::endl;
    }
    codes_ss << "}" << std::endl;
    codes_ss << "for (int " << range_index_name << " = 0; " << range_index_name << " < 1;"
             << range_index_name << "++) {" << std::endl;
    finish_codes_ss << "} // end of Existence Join" << std::endl;
    (*output)->process_codes += codes_ss.str();
    (*output)->finish_codes += finish_codes_ss.str();
    return arrow::Status::OK();
  }
  arrow::Status GetProcessProbe(
      const std::vector<
          std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
          input,
      int join_type, bool cond_check, std::shared_ptr<CodeGenContext>* output) {
    auto hash_relation_name =
        "hash_relation_list_" + std::to_string(hash_relation_id_) + "_";
    auto index_name = "hash_relation_" + std::to_string(hash_relation_id_) + "_index";

    int output_idx = 0;
    std::stringstream ss;
    auto tmp_name = "tmp_" + std::to_string(hash_relation_id_);
    auto is_outer_null_name = "is_outer_null_" + std::to_string(hash_relation_id_);
    std::stringstream prepare_ss;
    if (join_type == 1) {
      prepare_ss << "bool " << is_outer_null_name << ";" << std::endl;
    }
    prepare_ss << "ArrayItemIndex " << tmp_name << ";" << std::endl;
    (*output)->definition_codes += prepare_ss.str();

    int right_index_shift = 0;
    for (auto pair : result_schema_index_list_) {
      // set result to output list
      auto output_name = "hash_relation_" + std::to_string(hash_relation_id_) +
                         "_output_col_" + std::to_string(output_idx++);
      auto output_validity = output_name + "_validity";
      gandiva::DataTypePtr type;
      std::stringstream valid_ss;
      if (pair.first == 0) { /* left_table */
        auto name = "hash_relation_" + std::to_string(hash_relation_id_) + "_" +
                    std::to_string(pair.second);
        type = left_field_list_[pair.second]->type();
        if (join_type == 1) {
          valid_ss << output_validity << " = !" << is_outer_null_name << " && !(" << name
                   << "_has_null && " << name << "->IsNull(" << tmp_name << ".array_id, "
                   << tmp_name << ".id));" << std::endl;
        } else {
          valid_ss << output_validity << " = !(" << name << "_has_null && " << name
                   << "->IsNull(" << tmp_name << ".array_id, " << tmp_name << ".id));"
                   << std::endl;
        }
        std::stringstream value_define_ss;
        value_define_ss << "bool " << output_validity << ";" << std::endl;
        value_define_ss << GetCTypeString(type) << " " << output_name << ";" << std::endl;
        (*output)->definition_codes += value_define_ss.str();
        valid_ss << "if (" << output_validity << ")" << std::endl;
        valid_ss << output_name << " = " << name << "->GetValue(" << tmp_name
                 << ".array_id, " << tmp_name << ".id);" << std::endl;
      } else { /* right table */
        std::string name;
        if (exist_index_ != -1 && exist_index_ == pair.second) {
          name =
              "hash_relation_" + std::to_string(hash_relation_id_) + "_existence_value";
          valid_ss << output_validity << " = true;" << std::endl;
          valid_ss << output_name << " = " << name << ";" << std::endl;
          type = arrow::boolean();
          std::stringstream value_define_ss;
          value_define_ss << "bool " << output_validity << ";" << std::endl;
          value_define_ss << GetCTypeString(type) << " " << output_name << ";"
                          << std::endl;
          (*output)->definition_codes += value_define_ss.str();
          right_index_shift = -1;
        } else {
          auto i = pair.second + right_index_shift;
          output_name = input[i].first.first;
          output_validity = output_name + "_validity";
          valid_ss << input[i].first.second;
          type = input[i].second;
        }
      }
      (*output)->output_list.push_back(
          std::make_pair(std::make_pair(output_name, valid_ss.str()), type));
    }

    switch (join_type) {
      case 0: { /*Inner Join*/
        return GetInnerJoin(cond_check, index_name, hash_relation_name, output);
      } break;
      case 1: { /*Outer Join*/
        return GetOuterJoin(cond_check, index_name, hash_relation_name, output);
      } break;
      case 2: { /*Anti Join*/
        return GetAntiJoin(cond_check, index_name, hash_relation_name, output,
                           is_null_aware_anti_join_);
      } break;
      case 3: { /*Semi Join*/
        return GetSemiJoin(cond_check, index_name, hash_relation_name, output);
      } break;
      case 4: { /*Existence Join*/
        return GetExistenceJoin(cond_check, index_name, hash_relation_name, output);
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ConditionedProbeArraysTypedImpl only support join type: "
            "InnerJoin, "
            "RightJoin");
    }
    return arrow::Status::OK();
  }
};  // namespace extra

arrow::Status ConditionedProbeKernel::Make(
    arrow::compute::ExecContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, bool is_null_aware_anti_join, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedProbeKernel>(
      ctx, left_key_list, right_key_list, left_schema_list, right_schema_list, condition,
      join_type, is_null_aware_anti_join, result_schema, hash_configuration_list,
      hash_relation_idx);
  return arrow::Status::OK();
}

ConditionedProbeKernel::ConditionedProbeKernel(
    arrow::compute::ExecContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, bool is_null_aware_anti_join, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx) {
  impl_.reset(new Impl(ctx, left_key_list, right_key_list, left_schema_list,
                       right_schema_list, condition, join_type, is_null_aware_anti_join,
                       result_schema, hash_configuration_list, hash_relation_idx));
  kernel_name_ = "ConditionedProbeKernel";
  ctx_ = nullptr;
}

arrow::Status ConditionedProbeKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string ConditionedProbeKernel::GetSignature() { return impl_->GetSignature(); }

arrow::Status ConditionedProbeKernel::DoCodeGen(
    int level,
    std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
        input,
    std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
  return impl_->DoCodeGen(level, input, codegen_ctx_out, var_id);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
