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

#include <arrow/compute/api.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/codegen_register.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedProbeArrays  ////////////////
class ConditionedProbeArraysKernel::Impl {
 public:
  Impl(arrow::compute::ExecContext* ctx,
       const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
       const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
       const std::shared_ptr<gandiva::Node>& func_node, int join_type,
       const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
       const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
       const std::shared_ptr<arrow::Schema>& result_schema)
      : ctx_(ctx),
        left_schema_(arrow::schema(left_field_list)),
        right_schema_(arrow::schema(right_field_list)) {
    std::vector<int> left_key_index_list;
    THROW_NOT_OK(GetIndexList(left_key_list, left_field_list, &left_key_index_list));
    std::vector<int> right_key_index_list;
    THROW_NOT_OK(GetIndexList(right_key_list, right_field_list, &right_key_index_list));
    std::vector<int> left_shuffle_index_list;
    std::vector<int> right_shuffle_index_list;
    THROW_NOT_OK(
        GetIndexListFromSchema(result_schema, left_field_list, &left_shuffle_index_list));
    THROW_NOT_OK(GetIndexListFromSchema(result_schema, right_field_list,
                                        &right_shuffle_index_list));

    std::vector<std::pair<int, int>> result_schema_index_list;
    int exist_index = -1;
    THROW_NOT_OK(GetResultIndexList(result_schema, left_field_list, right_field_list,
                                    join_type, exist_index, &result_schema_index_list));

    THROW_NOT_OK(LoadJITFunction(
        func_node, join_type, left_key_index_list, right_key_index_list,
        left_shuffle_index_list, right_shuffle_index_list, left_field_list,
        right_field_list, result_schema_index_list, exist_index, &prober_));
  }

  arrow::Status Evaluate(const ArrayList& in) {
    arrow::ArrayVector outputs;
    if (left_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(left_schema_, length, in);
      RETURN_NOT_OK(left_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
    }
    RETURN_NOT_OK(prober_->Evaluate(in, outputs));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> prober_res_iter;
    RETURN_NOT_OK(prober_->MakeResultIterator(schema, &prober_res_iter));
    *out = std::make_shared<ProjectedProberResultIterator>(
        ctx_, right_projector_, right_schema_, prober_res_iter);
    return arrow::Status::OK();
  }

  std::string GetSignature() { return signature_; }

 private:
  using ArrayType = typename arrow::TypeTraits<arrow::Int64Type>::ArrayType;

  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<CodeGenBase> prober_;
  std::shared_ptr<gandiva::Projector> left_projector_;
  std::shared_ptr<gandiva::Projector> right_projector_;
  std::shared_ptr<arrow::Schema> left_schema_;
  std::shared_ptr<arrow::Schema> right_schema_;
  std::string signature_;

  class ProjectedProberResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ProjectedProberResultIterator(
        arrow::compute::ExecContext* ctx,
        const std::shared_ptr<gandiva::Projector>& right_projector,
        const std::shared_ptr<arrow::Schema>& right_schema,
        const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& prober_res_iter)
        : ctx_(ctx),
          right_projector_(right_projector),
          right_schema_(right_schema),
          prober_res_iter_(prober_res_iter) {}
    bool HasNext() override { return prober_res_iter_->HasNext(); }
    arrow::Status Process(
        const std::vector<std::shared_ptr<arrow::Array>>& in,
        std::shared_ptr<arrow::RecordBatch>* out,
        const std::shared_ptr<arrow::Array>& selection = nullptr) override {
      arrow::ArrayVector outputs;
      if (right_projector_) {
        auto length = in.size() > 0 ? in[0]->length() : 0;
        auto in_batch = arrow::RecordBatch::Make(right_schema_, length, in);
        RETURN_NOT_OK(
            right_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      }

      return prober_res_iter_->Process(in, outputs, out, selection);
    }

   private:
    arrow::compute::ExecContext* ctx_;
    std::shared_ptr<gandiva::Projector> right_projector_;
    std::shared_ptr<arrow::Schema> right_schema_;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> prober_res_iter_;
  };

  arrow::Status GetResultIndexList(
      const std::shared_ptr<arrow::Schema>& result_schema,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const int join_type, int& exist_index,
      std::vector<std::pair<int, int>>* result_schema_index_list) {
    int i = 0;
    bool found = false;
    int target_index = -1;
    int right_found = 0;
    for (auto target_field : result_schema->fields()) {
      target_index++;
      i = 0;
      found = false;
      for (auto field : left_field_list) {
        if (target_field->name() == field->name()) {
          (*result_schema_index_list).push_back(std::make_pair(0, i));
          found = true;
          break;
        }
        i++;
      }
      if (found == true) continue;
      i = 0;
      for (auto field : right_field_list) {
        if (target_field->name() == field->name()) {
          (*result_schema_index_list).push_back(std::make_pair(1, i));
          found = true;
          right_found++;
          break;
        }
        i++;
      }
      if (found == true) continue;
      if (join_type == 4) exist_index = target_index;
    }
    // Add one more col if join_type is ExistenceJoin
    if (join_type == 4) {
      (*result_schema_index_list).push_back(std::make_pair(1, right_found));
    }
    return arrow::Status::OK();
  }
  arrow::Status LoadJITFunction(
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<int>& left_key_index_list,
      const std::vector<int>& right_key_index_list,
      const std::vector<int>& left_shuffle_index_list,
      const std::vector<int>& right_shuffle_index_list,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::vector<std::pair<int, int>>& result_schema_index_list, int exist_index,
      std::shared_ptr<CodeGenBase>* out) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << "<HashJoin>"
                 << "[JoinType]" << join_type;
    if (func_node) {
      std::shared_ptr<CodeGenRegister> node_tmp;
      RETURN_NOT_OK(MakeCodeGenRegister(func_node, &node_tmp));
      func_args_ss << "[cond]" << node_tmp->GetFingerprint();
    }
    func_args_ss << "[BuildSchema]";
    for (auto field : left_field_list) {
      func_args_ss << field->type()->ToString();
    }
    func_args_ss << "[ProbeSchema]";
    for (auto field : right_field_list) {
      func_args_ss << field->type()->ToString();
    }
    func_args_ss << "[LeftKeyIndex]";
    for (auto i : left_key_index_list) {
      func_args_ss << i << ",";
    }
    func_args_ss << "[RightKeyIndex]";
    for (auto i : right_key_index_list) {
      func_args_ss << i << ",";
    }
    func_args_ss << "[LeftShuffleIndex]";
    for (auto i : left_shuffle_index_list) {
      func_args_ss << i << ",";
    }
    func_args_ss << "[RightShuffleIndex]";
    for (auto i : right_shuffle_index_list) {
      func_args_ss << i << ",";
    }
    func_args_ss << "[ResultOrdinal]";
    for (auto pair : result_schema_index_list) {
      func_args_ss << pair.first << "_" << pair.second << ",";
    }

#ifdef DEBUG
    std::cout << "signature original line is " << func_args_ss.str() << std::endl;
#endif
    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    signature_ = signature_ss.str();

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, out);
    if (!status.ok()) {
      // process
      try {
        auto codes = ProduceCodes(
            func_node, join_type, left_key_index_list, right_key_index_list,
            left_shuffle_index_list, right_shuffle_index_list, left_field_list,
            right_field_list, result_schema_index_list, exist_index);
        // compile codes
        RETURN_NOT_OK(CompileCodes(codes, signature_));
        RETURN_NOT_OK(LoadLibrary(signature_, ctx_, out));
      } catch (const std::runtime_error& error) {
        FileSpinUnLock(file_lock);
        throw error;
      }
    } else {
      std::vector<int> left_cond_index_list;
      std::vector<int> right_cond_index_list;
      std::vector<std::pair<gandiva::DataTypePtr, std::string>> left_projected_batch_list;
      std::vector<std::pair<gandiva::DataTypePtr, std::string>>
          right_projected_batch_list;
      if (func_node) {
        GetConditionCheckFunc(func_node, left_field_list, right_field_list,
                              &left_cond_index_list, &right_cond_index_list,
                              &left_projected_batch_list, &right_projected_batch_list);
      }
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  class TypedProberCodeGenImpl {
   public:
    TypedProberCodeGenImpl(std::string indice, std::shared_ptr<arrow::DataType> data_type,
                           bool left = true)
        : indice_(indice), data_type_(data_type), left_(left) {}
    std::string GetImplCachedDefine() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " << GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      return ss.str();
    }
    std::string GetResultIteratorPrepare() {
      std::stringstream ss;
      ss << "builder_" << indice_ << "_ = std::make_shared<"
         << GetTypeString(data_type_, "Builder") << ">(ctx_->memory_pool());"
         << std::endl;
      return ss.str();
    }
    std::string GetProcessFinish() {
      std::stringstream ss;
      ss << "std::shared_ptr<arrow::Array> out_" << indice_ << ";" << std::endl;
      ss << "RETURN_NOT_OK(builder_" << indice_ << "_->Finish(&out_" << indice_ << "));"
         << std::endl;
      ss << "builder_" << indice_ << "_->Reset();" << std::endl;
      return ss.str();
    }
    std::string GetProcessOutList() {
      std::stringstream ss;
      ss << "out_" << indice_;
      return ss.str();
    }
    std::string GetResultIterCachedDefine() {
      std::stringstream ss;
      if (left_) {
        ss << "std::vector<std::shared_ptr<" << GetTypeString(data_type_, "Array")
           << ">> cached_" << indice_ << "_;" << std::endl;
      } else {
        ss << "std::shared_ptr<" << GetTypeString(data_type_, "Array") << "> cached_"
           << indice_ << "_;" << std::endl;
      }
      ss << "using ArrayType_" << indice_ << " = " << GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "std::shared_ptr<" << GetTypeString(data_type_, "Builder") << "> builder_"
         << indice_ << "_;" << std::endl;
      return ss.str();
    }

   private:
    std::string indice_;
    std::shared_ptr<arrow::DataType> data_type_;
    bool left_;
  };
  std::string GetJoinKeyFieldListDefine(
      const std::vector<int>& left_key_index_list,
      const std::vector<std::shared_ptr<arrow::Field>>& field_list) {
    std::stringstream ss;
    for (int i = 0; i < left_key_index_list.size(); i++) {
      auto field = field_list[left_key_index_list[i]];
      if (i != (left_key_index_list.size() - 1)) {
        ss << "arrow::field(\"" << field->name()
           << "\", arrow::" << GetArrowTypeDefString(field->type()) << "), ";
      } else {
        ss << "arrow::field(\"" << field->name()
           << "\", arrow::" << GetArrowTypeDefString(field->type()) << ")";
      }
    }
    return ss.str();
  }
  std::string GetEvaluateCacheInsert(const std::vector<int>& index_list) {
    std::stringstream ss;
    for (auto i : index_list) {
      ss << "cached_0_" << i << "_.push_back(std::make_shared<ArrayType_0_" << i
         << ">(in[" << i << "]));" << std::endl;
    }
    return ss.str();
  }
  std::string GetEncodeJoinKey(std::vector<int> key_indices) {
    std::stringstream ss;
    for (int i = 0; i < key_indices.size(); i++) {
      if (i != (key_indices.size() - 1)) {
        ss << "in[" << key_indices[i] << "], ";
      } else {
        ss << "in[" << key_indices[i] << "]";
      }
    }
    return ss.str();
  }
  std::string GetFinishCachedParameter(const std::vector<int>& key_indices) {
    std::stringstream ss;
    for (int i = 0; i < key_indices.size(); i++) {
      if (i != (key_indices.size() - 1)) {
        ss << "cached_0_" << key_indices[i] << "_, ";
      } else {
        ss << "cached_0_" << key_indices[i] << "_";
      }
    }
    auto ret = ss.str();
    if (ret.empty()) {
      return ret;
    } else {
      return ", " + ret;
    }
  }
  std::string GetImplCachedDefine(
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> codegen_list) {
    std::stringstream ss;
    for (auto codegen : codegen_list) {
      ss << codegen->GetImplCachedDefine() << std::endl;
    }
    return ss.str();
  }
  std::string GetLeftProjectedDefine(
      std::vector<std::pair<gandiva::DataTypePtr, std::string>>
          left_projected_batch_list) {
    std::stringstream ss;
    for (auto name : left_projected_batch_list) {
      ss << "std::vector<std::shared_ptr<" << GetTypeString(name.first, "Array") << ">> "
         << name.second << ";" << std::endl;
    }
    return ss.str();
  }
  std::string GetRightProjectedDefine(
      std::vector<std::pair<gandiva::DataTypePtr, std::string>>
          right_projected_batch_list) {
    std::stringstream ss;
    for (auto name : right_projected_batch_list) {
      ss << "std::shared_ptr<" << GetTypeString(name.first, "Array") << "> "
         << name.second << ";" << std::endl;
    }
    return ss.str();
  }
  std::string GetResultIteratorParams(std::vector<int> key_indices) {
    std::stringstream ss;
    for (int i = 0; i < key_indices.size(); i++) {
      if (i != (key_indices.size() - 1)) {
        ss << "const std::vector<std::shared_ptr<ArrayType_0_" << key_indices[i]
           << ">> &cached_0_" << key_indices[i] << ", " << std::endl;
      } else {
        ss << "const std::vector<std::shared_ptr<ArrayType_0_" << key_indices[i]
           << ">> &cached_0_" << key_indices[i];
      }
    }
    auto ret = ss.str();
    if (ret.empty()) {
      return ret;
    } else {
      return ", " + ret;
    }
  }
  std::string GetResultIteratorSet(std::vector<int> key_indices) {
    std::stringstream ss;
    for (auto i : key_indices) {
      ss << "cached_0_" << i << "_ = cached_0_" << i << ";" << std::endl;
    }
    return ss.str();
  }
  std::string GetResultIteratorPrepare(
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_codegen_list,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> right_codegen_list) {
    std::stringstream ss;
    for (auto codegen : left_codegen_list) {
      ss << codegen->GetResultIteratorPrepare() << std::endl;
    }
    for (auto codegen : right_codegen_list) {
      ss << codegen->GetResultIteratorPrepare() << std::endl;
    }
    return ss.str();
  }
  std::string GetProcessRightSet(std::vector<int> indices) {
    std::stringstream ss;
    for (auto i : indices) {
      ss << "cached_1_" << i << "_ = std::make_shared<ArrayType_1_" << i << ">(in[" << i
         << "]);" << std::endl;
    }
    return ss.str();
  }
  std::string GetLeftProjectedSet(
      std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> name_list) {
    std::stringstream ss;
    int i = 0;
    for (auto name : name_list) {
      ss << name.second << ".push_back(std::make_shared<"
         << GetTypeString(name.first, "Array") << ">(projected_batch[" << i++ << "]));"
         << std::endl;
    }
    return ss.str();
  }
  std::string GetRightProjectedSet(
      std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> name_list) {
    std::stringstream ss;
    int i = 0;
    for (auto name : name_list) {
      ss << name.second << " = std::make_shared<" << GetTypeString(name.first, "Array")
         << ">(projected_batch[" << i++ << "]);" << std::endl;
    }
    return ss.str();
  }
  std::string GetResultIteratorProjectedParams(
      std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> name_list) {
    std::vector<std::string> param_list;
    for (auto name : name_list) {
      std::stringstream ss;
      ss << "const std::vector<std::shared_ptr<" << GetTypeString(name.first, "Array")
         << ">>& " << name.second.substr(0, name.second.size() - 1);
      param_list.push_back(ss.str());
    }
    return GetParameterList(param_list);
  }
  std::string GetResultIteratorProjectedSet(
      std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> name_list) {
    std::stringstream ss;
    for (auto name : name_list) {
      ss << name.second << " = " << name.second.substr(0, name.second.size() - 1) << ";"
         << std::endl;
    }
    return ss.str();
  }
  std::string GetProcessFinish(
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_codegen_list,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> right_codegen_list) {
    std::stringstream ss;
    for (auto codegen : left_codegen_list) {
      ss << codegen->GetProcessFinish() << std::endl;
    }
    for (auto codegen : right_codegen_list) {
      ss << codegen->GetProcessFinish() << std::endl;
    }
    return ss.str();
  }
  std::string GetProcessOutList(
      const std::vector<std::pair<int, int>>& result_schema_index_list,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_codegen_list,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> right_codegen_list) {
    std::stringstream ss;
    auto item_count = result_schema_index_list.size();
    int i = 0;
    for (auto index : result_schema_index_list) {
      std::shared_ptr<TypedProberCodeGenImpl> codegen;
      if (index.first == 0) {
        codegen = left_codegen_list[index.second];
      } else {
        codegen = right_codegen_list[index.second];
      }
      if (i++ != (item_count - 1)) {
        ss << codegen->GetProcessOutList() << ", ";
      } else {
        ss << codegen->GetProcessOutList();
      }
    }
    return ss.str();
  }
  std::string GetResultIterCachedDefine(
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_codegen_list,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>> right_codegen_list) {
    std::stringstream ss;
    for (auto codegen : left_codegen_list) {
      ss << codegen->GetResultIterCachedDefine() << std::endl;
    }
    for (auto codegen : right_codegen_list) {
      ss << codegen->GetResultIterCachedDefine() << std::endl;
    }
    return ss.str();
  }
  std::string GetInnerJoin(bool cond_check,
                           const std::vector<int>& left_shuffle_index_list,
                           const std::vector<int>& right_shuffle_index_list) {
    std::stringstream ss;
    for (auto i : left_shuffle_index_list) {
      ss << "if (cached_0_" << i << "_[tmp.array_id]->IsNull(tmp.id)) {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_0_" << i << "_->AppendNull());" << std::endl;
      ss << "} else {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_0_" << i << "_->Append(cached_0_" << i
         << "_[tmp.array_id]->GetView(tmp.id)));" << std::endl;
      ss << "}" << std::endl;
    }
    for (auto i : right_shuffle_index_list) {
      ss << "if (cached_1_" << i << "_->IsNull(i)) {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_1_" << i << "_->AppendNull());" << std::endl;
      ss << "} else {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
         << "_->GetView(i)));" << std::endl;
      ss << "}" << std::endl;
    }
    std::string shuffle_str;
    if (cond_check) {
      shuffle_str = R"(
              if (ConditionCheck(tmp, i)) {
                )" + ss.str() +
                    R"(
                out_length += 1;
              }
      )";
    } else {
      shuffle_str = R"(
              )" + ss.str() +
                    R"(
              out_length += 1;
      )";
    }
    return R"(
        if (!typed_array->IsNull(i)) {
          auto index = hash_table_->Get(typed_array->GetView(i));
          if (index != -1) {
            for (auto tmp : (*memo_index_to_arrayid_)[index]) {
              )" +
           shuffle_str + R"(
            }
          }
        }
  )";
  }
  std::string GetOuterJoin(bool cond_check,
                           const std::vector<int>& left_shuffle_index_list,
                           const std::vector<int>& right_shuffle_index_list) {
    std::stringstream left_null_ss;
    std::stringstream left_valid_ss;
    std::stringstream right_valid_ss;
    for (auto i : left_shuffle_index_list) {
      left_valid_ss << "if (cached_0_" << i << "_[tmp.array_id]->null_count()) {" << std::endl;
      left_valid_ss << "if (cached_0_" << i << "_[tmp.array_id]->IsNull(tmp.id)) {"
                    << std::endl;
      left_valid_ss << "  RETURN_NOT_OK(builder_0_" << i << "_->AppendNull());"
                    << std::endl;
      left_valid_ss << "} else {" << std::endl;
      left_valid_ss << "  RETURN_NOT_OK(builder_0_" << i << "_->Append(cached_0_" << i
                    << "_[tmp.array_id]->GetView(tmp.id)));" << std::endl;
      left_valid_ss << "}" << std::endl;
      left_valid_ss << "} else {" << std::endl;
      left_valid_ss << "  RETURN_NOT_OK(builder_0_" << i << "_->Append(cached_0_" << i
                    << "_[tmp.array_id]->GetView(tmp.id)));" << std::endl;
      left_valid_ss << "}" << std::endl;
      left_null_ss << "RETURN_NOT_OK(builder_0_" << i << "_->AppendNull());" << std::endl;
    }
    for (auto i : right_shuffle_index_list) {
      right_valid_ss << "if (cached_1_" << i << "_->null_count()) {" << std::endl;
      right_valid_ss << "if (cached_1_" << i << "_->IsNull(i)) {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->AppendNull());"
                     << std::endl;
      right_valid_ss << "} else {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
                     << "_->GetView(i)));" << std::endl;
      right_valid_ss << "}" << std::endl;
      right_valid_ss << "} else {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
         << "_->GetView(i)));" << std::endl;
      right_valid_ss << "}" << std::endl;
    }
    std::string shuffle_str;
    if (cond_check) {
      shuffle_str = R"(
              if (ConditionCheck(tmp, i)) {
                )" + left_valid_ss.str() +
                    right_valid_ss.str() + R"(
                out_length += 1;
              }
      )";
    } else {
      shuffle_str = R"(
              )" + left_valid_ss.str() +
                    right_valid_ss.str() + R"(
              out_length += 1;
      )";
    }
    return R"(
        int32_t index;
        if (!typed_array->IsNull(i)) {
          index = hash_table_->Get(typed_array->GetView(i));
        } else {
          index = hash_table_->GetNull();
        }
        if (index == -1) {
          )" +
           left_null_ss.str() + right_valid_ss.str() + R"(
          out_length += 1;
        } else {
          for (auto tmp : (*memo_index_to_arrayid_)[index]) {
            )" +
           shuffle_str + R"(
          }
        }
  )";
  }
  std::string GetAntiJoin(bool cond_check,
                          const std::vector<int>& left_shuffle_index_list,
                          const std::vector<int>& right_shuffle_index_list) {
    std::stringstream left_null_ss;
    std::stringstream right_valid_ss;
    for (auto i : left_shuffle_index_list) {
      left_null_ss << "RETURN_NOT_OK(builder_0_" << i << "_->AppendNull());" << std::endl;
    }
    for (auto i : right_shuffle_index_list) {
      right_valid_ss << "if (cached_1_" << i << "_->null_count()) {" << std::endl;
      right_valid_ss << "if (cached_1_" << i << "_->IsNull(i)) {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->AppendNull());"
                     << std::endl;
      right_valid_ss << "} else {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
                     << "_->GetView(i)));" << std::endl;
      right_valid_ss << "}" << std::endl;
      right_valid_ss << "} else {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
         << "_->GetView(i)));" << std::endl;
      right_valid_ss << "}" << std::endl;
    }
    std::string shuffle_str;
    if (cond_check) {
      shuffle_str = R"(
        } else {
          bool found = false;
          for (auto tmp : (*memo_index_to_arrayid_)[index]) {
            if (ConditionCheck(tmp, i)) {
              found = true;
              break;
            }
          }
          if (!found) {
              )" + left_null_ss.str() +
                    right_valid_ss.str() + R"(
            out_length += 1;
          }
      )";
    }
    return R"(
        int32_t index;
        if (!typed_array->IsNull(i)) {
          index = hash_table_->Get(typed_array->GetView(i));
        } else {
          index = hash_table_->GetNull();
        }
        if (index == -1) {
          )" +
           left_null_ss.str() + right_valid_ss.str() + R"(
          out_length += 1;
          )" +
           shuffle_str + R"(
        }
  )";
  }
  std::string GetSemiJoin(bool cond_check,
                          const std::vector<int>& left_shuffle_index_list,
                          const std::vector<int>& right_shuffle_index_list) {
    std::stringstream ss;
    for (auto i : left_shuffle_index_list) {
      ss << "RETURN_NOT_OK(builder_0_" << i << "_->AppendNull());" << std::endl;
    }
    for (auto i : right_shuffle_index_list) {
      ss << "if (cached_1_" << i << "_->null_count()) {" << std::endl;
      ss << "if (cached_1_" << i << "_->IsNull(i)) {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_1_" << i << "_->AppendNull());" << std::endl;
      ss << "} else {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
         << "_->GetView(i)));" << std::endl;
      ss << "}" << std::endl;
      ss << "} else {" << std::endl;
      ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
         << "_->GetView(i)));" << std::endl;
      ss << "}" << std::endl;
    }
    std::string shuffle_str;
    if (cond_check) {
      shuffle_str = R"(
            for (auto tmp : (*memo_index_to_arrayid_)[index]) {
              if (ConditionCheck(tmp, i)) {
                )" + ss.str() +
                    R"(
                out_length += 1;
                break;
              }
            }
      )";
    } else {
      shuffle_str = R"(
              )" + ss.str() +
                    R"(
              out_length += 1;
      )";
    }
    return R"(
        if (!typed_array->IsNull(i)) {
          auto index = hash_table_->Get(typed_array->GetView(i));
          if (index != -1) {
                )" +
           shuffle_str + R"(
          }
        }
  )";
  }
  std::string GetExistenceJoin(bool cond_check,
                               const std::vector<int>& left_shuffle_index_list,
                               const std::vector<int>& right_shuffle_index_list) {
    std::stringstream right_exist_ss;
    std::stringstream right_not_exist_ss;
    std::stringstream left_valid_ss;
    std::stringstream right_valid_ss;
    auto right_size = right_shuffle_index_list.size();

    right_exist_ss
        << "const bool exist = true; RETURN_NOT_OK(builder_1_exists_->Append(exist));"
        << std::endl;
    right_not_exist_ss << "const bool not_exist = false; "
                          "RETURN_NOT_OK(builder_1_exists_->Append(not_exist));"
                       << std::endl;

    for (auto i : right_shuffle_index_list) {
      right_valid_ss << "if (cached_1_" << i << "_->IsNull(i)) {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->AppendNull());"
                     << std::endl;
      right_valid_ss << "} else {" << std::endl;
      right_valid_ss << "  RETURN_NOT_OK(builder_1_" << i << "_->Append(cached_1_" << i
                     << "_->GetView(i)));" << std::endl;
      right_valid_ss << "}" << std::endl;
    }
    std::string shuffle_str;
    if (cond_check) {
      shuffle_str = R"(
              if (ConditionCheck(tmp, i)) {
                )" + right_valid_ss.str() +
                    right_exist_ss.str() + R"(
                out_length += 1;
              }
      )";
    } else {
      shuffle_str = R"(
              )" + right_valid_ss.str() +
                    right_exist_ss.str() + R"(
              out_length += 1;
      )";
    }
    return R"(
        int32_t index;
        if (!typed_array->IsNull(i)) {
          index = hash_table_->Get(typed_array->GetView(i));
        } else {
          index = hash_table_->GetNull();
        }
        if (index == -1) {
          )" +
           right_valid_ss.str() + right_not_exist_ss.str() + R"(
          out_length += 1;
        } else {
            )" +
           shuffle_str + R"(
        }
  )";
  }
  std::string GetProcessProbe(int join_type, bool cond_check,
                              const std::vector<int>& left_shuffle_index_list,
                              const std::vector<int>& right_shuffle_index_list) {
    switch (join_type) {
      case 0: { /*Inner Join*/
        return GetInnerJoin(cond_check, left_shuffle_index_list,
                            right_shuffle_index_list);
      } break;
      case 1: { /*Outer Join*/
        return GetOuterJoin(cond_check, left_shuffle_index_list,
                            right_shuffle_index_list);
      } break;
      case 2: { /*Anti Join*/
        return GetAntiJoin(cond_check, left_shuffle_index_list, right_shuffle_index_list);
      } break;
      case 3: { /*Semi Join*/
        return GetSemiJoin(cond_check, left_shuffle_index_list, right_shuffle_index_list);
      } break;
      case 4: { /*Existence Join*/
        return GetExistenceJoin(cond_check, left_shuffle_index_list,
                                right_shuffle_index_list);
      } break;
      default:
        std::cout << "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "
                     "RightJoin"
                  << std::endl;
        throw;
    }
    return "";
  }
  std::string GetConditionCheckFunc(
      const std::shared_ptr<gandiva::Node>& func_node,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      std::vector<int>* left_out_index_list, std::vector<int>* right_out_index_list,
      std::vector<std::pair<gandiva::DataTypePtr, std::string>>*
          left_projected_batch_list,
      std::vector<std::pair<gandiva::DataTypePtr, std::string>>*
          right_projected_batch_list) {
    std::shared_ptr<CodeGenNodeVisitor> func_node_visitor;
    int func_count = 0;
    std::vector<std::string> input_list;
    std::vector<gandiva::ExpressionPtr> project_node_list;
    THROW_NOT_OK(MakeCodeGenNodeVisitor(func_node, {left_field_list, right_field_list},
                                        &func_count, &input_list, left_out_index_list,
                                        right_out_index_list, &project_node_list,
                                        &func_node_visitor));
    std::vector<gandiva::ExpressionPtr> left_project_node_list;
    std::vector<gandiva::ExpressionPtr> right_project_node_list;
    for (auto project : project_node_list) {
      auto name = project->result()->name();
      auto data_type = project->result()->type();
      if (name.find("left") != std::string::npos) {
        left_project_node_list.push_back(project);
        (*left_projected_batch_list).push_back(std::make_pair(data_type, name));
      } else {
        right_project_node_list.push_back(project);
        (*right_projected_batch_list).push_back(std::make_pair(data_type, name));
      }
    }

    if (!left_project_node_list.empty()) {
      auto schema = arrow::schema(left_field_list);
      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      auto status = gandiva::Projector::Make(schema, left_project_node_list,
                                             configuration, &left_projector_);
    }

    if (!right_project_node_list.empty()) {
      auto schema = arrow::schema(right_field_list);
      auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
      auto status = gandiva::Projector::Make(schema, right_project_node_list,
                                             configuration, &right_projector_);
    }

    return R"(
    inline bool ConditionCheck(ArrayItemIndex x, int y) {
      )" + func_node_visitor->GetPrepare() +
           R"(
        return )" +
           func_node_visitor->GetResult() +
           R"(;
    }
  )";
  }
  arrow::Status GetTypedProberCodeGen(
      std::string prefix, bool left, const std::vector<int>& index_list,
      const std::vector<std::shared_ptr<arrow::Field>>& field_list, int exist_index,
      std::vector<std::shared_ptr<TypedProberCodeGenImpl>>* out_list,
      int join_type = -1) {
    for (auto i : index_list) {
      auto field = field_list[i];
      auto codegen = std::make_shared<TypedProberCodeGenImpl>(prefix + std::to_string(i),
                                                              field->type(), left);
      (*out_list).push_back(codegen);
    }
    if (join_type == 4 && exist_index != -1) {
      auto codegen = std::make_shared<TypedProberCodeGenImpl>(prefix + "exists",
                                                              arrow::boolean(), left);
      (*out_list).insert((*out_list).begin() + exist_index, codegen);
    }
    return arrow::Status::OK();
  }
  std::vector<int> MergeKeyIndexList(const std::vector<int>& left_index_list,
                                     const std::vector<int>& right_index_list) {
    std::vector<int> ret = left_index_list;
    for (auto i : right_index_list) {
      if (std::find(left_index_list.begin(), left_index_list.end(), i) ==
          left_index_list.end()) {
        ret.push_back(i);
      }
    }
    std::sort(ret.begin(), ret.end());
    return ret;
  }
  std::string GetKeyCType(const std::vector<int>& key_index_list,
                          const std::vector<std::shared_ptr<arrow::Field>>& field_list) {
    auto field = field_list[key_index_list[0]];
    return GetCTypeString(field->type());
  }
  std::string GetTypedArray(bool multiple_cols, std::string index, int i,
                            std::string data_type,
                            std::string evaluate_encode_join_key_str) {
    std::stringstream ss;
    if (multiple_cols) {
      ss << "auto concat_kernel_arr_list = {" << evaluate_encode_join_key_str << "};"
         << std::endl;
      ss << "std::shared_ptr<arrow::Array> hash_in;" << std::endl;
      ss << "RETURN_NOT_OK(hash_kernel_->Evaluate(concat_kernel_arr_list, &hash_in));"
         << std::endl;
      ss << "auto typed_array = std::make_shared<Int64Array>(hash_in);" << std::endl;
    } else {
      ss << "auto typed_array = std::make_shared<" << data_type << ">(in[" << i << "]);"
         << std::endl;
    }
    return ss.str();
  }
  std::string ProduceCodes(
      const std::shared_ptr<gandiva::Node>& func_node, int join_type,
      const std::vector<int>& left_key_index_list,
      const std::vector<int>& right_key_index_list,
      const std::vector<int>& left_shuffle_index_list,
      const std::vector<int>& right_shuffle_index_list,
      const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
      const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
      const std::vector<std::pair<int, int>>& result_schema_index_list, int exist_index) {
    std::vector<int> left_cond_index_list;
    std::vector<int> right_cond_index_list;
    bool cond_check = false;
    bool multiple_cols = (left_key_index_list.size() > 1);
    std::string hash_map_include_str = R"(#include "precompile/sparse_hash_map.h")";
    std::string hash_map_type_str =
        "SparseHashMap<" + GetCTypeString(arrow::int64()) + ">";
    std::string hash_map_define_str =
        "std::make_shared<" + hash_map_type_str + ">(ctx_->memory_pool());";
    if (!multiple_cols) {
      if (left_field_list[left_key_index_list[0]]->type()->id() == arrow::Type::STRING) {
        hash_map_type_str =
            GetTypeString(left_field_list[left_key_index_list[0]]->type(), "") +
            "HashMap";
        hash_map_include_str = R"(#include "precompile/hash_map.h")";
      } else {
        hash_map_type_str =
            "SparseHashMap<" +
            GetCTypeString(left_field_list[left_key_index_list[0]]->type()) + ">";
      }
      hash_map_define_str =
          "std::make_shared<" + hash_map_type_str + ">(ctx_->memory_pool());";
    }
    std::string condition_check_str;
    std::string left_projected_prepare_str;
    std::string right_projected_prepare_str;
    std::vector<std::pair<gandiva::DataTypePtr, std::string>> left_projected_batch_list;
    std::vector<std::pair<gandiva::DataTypePtr, std::string>> right_projected_batch_list;
    std::vector<std::string> left_projected_batch_name_list;
    if (func_node) {
      condition_check_str =
          GetConditionCheckFunc(func_node, left_field_list, right_field_list,
                                &left_cond_index_list, &right_cond_index_list,
                                &left_projected_batch_list, &right_projected_batch_list);
      left_projected_prepare_str = GetLeftProjectedSet(left_projected_batch_list);
      right_projected_prepare_str = GetRightProjectedSet(right_projected_batch_list);
      for (auto name : left_projected_batch_list) {
        left_projected_batch_name_list.push_back(name.second);
      }
      cond_check = true;
    }
    auto process_probe_str = GetProcessProbe(
        join_type, cond_check, left_shuffle_index_list, right_shuffle_index_list);
    auto left_cache_index_list =
        MergeKeyIndexList(left_cond_index_list, left_shuffle_index_list);
    auto right_cache_index_list =
        MergeKeyIndexList(right_cond_index_list, right_shuffle_index_list);

    std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_cache_codegen_list;
    std::vector<std::shared_ptr<TypedProberCodeGenImpl>> left_shuffle_codegen_list;
    std::vector<std::shared_ptr<TypedProberCodeGenImpl>> right_shuffle_codegen_list;
    GetTypedProberCodeGen("0_", true, left_cache_index_list, left_field_list, exist_index,
                          &left_cache_codegen_list);
    GetTypedProberCodeGen("0_", true, left_shuffle_index_list, left_field_list,
                          exist_index, &left_shuffle_codegen_list);
    GetTypedProberCodeGen("1_", false, right_shuffle_index_list, right_field_list,
                          exist_index, &right_shuffle_codegen_list, join_type);
    auto join_key_type_list_define_str =
        GetJoinKeyFieldListDefine(left_key_index_list, left_field_list);
    auto evaluate_cache_insert_str = GetEvaluateCacheInsert(left_cache_index_list);
    auto evaluate_encode_join_key_str = GetEncodeJoinKey(left_key_index_list);
    auto finish_cached_parameter_str = GetFinishCachedParameter(left_cache_index_list) +
                                       GetParameterList(left_projected_batch_name_list);
    auto impl_cached_define_str = GetImplCachedDefine(left_cache_codegen_list);
    auto impl_projected_define_str = GetLeftProjectedDefine(left_projected_batch_list);
    auto res_iter_projected_define_str =
        GetRightProjectedDefine(right_projected_batch_list);
    auto result_iter_params_str = GetResultIteratorParams(left_cache_index_list);
    auto result_iter_projected_params_str =
        GetResultIteratorProjectedParams(left_projected_batch_list);
    auto result_iter_set_str = GetResultIteratorSet(left_cache_index_list);
    auto result_iter_projected_set_str =
        GetResultIteratorProjectedSet(left_projected_batch_list);
    auto result_iter_prepare_str =
        GetResultIteratorPrepare(left_shuffle_codegen_list, right_shuffle_codegen_list);
    auto process_right_set_str = GetProcessRightSet(right_cache_index_list);
    auto process_encode_join_key_str = GetEncodeJoinKey(right_key_index_list);
    auto process_finish_str =
        GetProcessFinish(left_shuffle_codegen_list, right_shuffle_codegen_list);
    auto process_out_list_str = GetProcessOutList(
        result_schema_index_list, left_shuffle_codegen_list, right_shuffle_codegen_list);
    auto result_iter_cached_define_str =
        GetResultIterCachedDefine(left_cache_codegen_list, right_shuffle_codegen_list);
    auto evaluate_get_typed_array_str = GetTypedArray(
        multiple_cols, "0_" + std::to_string(left_key_index_list[0]),
        left_key_index_list[0],
        GetTypeString(left_field_list[left_key_index_list[0]]->type(), "Array"),
        evaluate_encode_join_key_str);
    auto process_get_typed_array_str = GetTypedArray(
        multiple_cols, "1_" + std::to_string(right_key_index_list[0]),
        right_key_index_list[0],
        GetTypeString(left_field_list[left_key_index_list[0]]->type(), "Array"),
        process_encode_join_key_str);
    return BaseCodes() + R"(
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/builder.h"
#include "precompile/hash_arrays_kernel.h"
)" + hash_map_include_str +
           R"(
using namespace sparkcolumnarplugin::precompile;

class TypedProberImpl : public CodeGenBase {
 public:
  TypedProberImpl(arrow::compute::ExecContext *ctx) : ctx_(ctx) {
    hash_table_ = )" +
           hash_map_define_str +
           (multiple_cols ? R"(
    // Create Hash Kernel
    auto field_list = {)" + join_key_type_list_define_str +
                                R"(};
    hash_kernel_ = std::make_shared<HashArraysKernel>(ctx_->memory_pool(), field_list);)"
                          : "") +
           R"(

  }
  ~TypedProberImpl() {}

  arrow::Status Evaluate(const ArrayList& in, const ArrayList& projected_batch) override {
    )" + evaluate_cache_insert_str +
           evaluate_get_typed_array_str + left_projected_prepare_str +
           R"(

    auto insert_on_found = [this](int32_t i) {
      memo_index_to_arrayid_[i].emplace_back(cur_array_id_, cur_id_);
    };
    auto insert_on_not_found = [this](int32_t i) {
      num_items_++;
      memo_index_to_arrayid_.push_back(
          {ArrayItemIndex(cur_array_id_, cur_id_)});
    };

    cur_id_ = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id_), [](int32_t){},
                                 [](int32_t){}, &memo_index);
        if (memo_index < num_items_) {
          insert_on_found(memo_index);
        } else {
          insert_on_not_found(memo_index);
        }
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          hash_table_->GetOrInsertNull([](int32_t){}, [](int32_t){});
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id_),
                                   [](int32_t){}, [](int32_t){},
                                   &memo_index);
        if (memo_index < num_items_) {
          insert_on_found(memo_index);
        } else {
          insert_on_not_found(memo_index);
        }
        }
      }
    }
    cur_array_id_++;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>> *out) override {
    *out = std::make_shared<ProberResultIterator>(
        ctx_, schema, hash_kernel_, hash_table_, &memo_index_to_arrayid_)" +
           finish_cached_parameter_str + R"(
    );
    return arrow::Status::OK();
  }

private:
  uint64_t cur_array_id_ = 0;
  uint64_t cur_id_ = 0;
  uint64_t num_items_ = 0;
  arrow::compute::ExecContext *ctx_;
  std::shared_ptr<HashArraysKernel> hash_kernel_;
  std::shared_ptr<)" +
           hash_map_type_str + R"(> hash_table_;
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  )" + impl_cached_define_str +
           impl_projected_define_str +
           R"( 

  class ProberResultIterator : public ResultIterator<arrow::RecordBatch> {
  public:
    ProberResultIterator(
        arrow::compute::ExecContext *ctx,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<HashArraysKernel> hash_kernel,
        std::shared_ptr<)" +
           hash_map_type_str + R"(> hash_table,
        std::vector<std::vector<ArrayItemIndex>> *memo_index_to_arrayid)" +
           result_iter_params_str + result_iter_projected_params_str + R"(
        )
        : ctx_(ctx), result_schema_(schema), hash_kernel_(hash_kernel), hash_table_(hash_table),
          memo_index_to_arrayid_(memo_index_to_arrayid) {
            )" +
           result_iter_set_str + result_iter_prepare_str + result_iter_projected_set_str +
           R"(
    }

    std::string ToString() override { return "ProberResultIterator"; }

    arrow::Status
    Process(const ArrayList &in, const ArrayList& projected_batch,
            std::shared_ptr<arrow::RecordBatch> *out,
            const std::shared_ptr<arrow::Array> &selection) override {
      uint64_t out_length = 0;
      )" + process_right_set_str +
           process_get_typed_array_str + right_projected_prepare_str +
           R"(
      auto length = cached_1_0_->length();

      for (int i = 0; i < length; i++) {)" +
           process_probe_str + R"(
      }
      )" + process_finish_str +
           R"(
      *out = arrow::RecordBatch::Make(
          result_schema_, out_length,
          {)" +
           process_out_list_str + R"(});
      //arrow::PrettyPrint(*(*out).get(), 2, &std::cout);
      return arrow::Status::OK();
    }

  private:
    arrow::compute::ExecContext *ctx_;
    std::shared_ptr<arrow::Schema> result_schema_;
    std::shared_ptr<HashArraysKernel> hash_kernel_;
    std::shared_ptr<)" +
           hash_map_type_str + R"(> hash_table_;
    std::vector<std::vector<ArrayItemIndex>> *memo_index_to_arrayid_;
)" + result_iter_cached_define_str +
           impl_projected_define_str + res_iter_projected_define_str +
           R"(
      )" + condition_check_str +
           R"(
  };
};

extern "C" void MakeCodeGen(arrow::compute::ExecContext *ctx,
                            std::shared_ptr<CodeGenBase> *out) {
  *out = std::make_shared<TypedProberImpl>(ctx);
}
    )";
  }
};

arrow::Status ConditionedProbeArraysKernel::Make(
    arrow::compute::ExecContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
    const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
    const std::shared_ptr<gandiva::Node>& func_node, int join_type,
    const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
    const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
    const std::shared_ptr<arrow::Schema>& result_schema,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedProbeArraysKernel>(
      ctx, left_key_list, right_key_list, func_node, join_type, left_field_list,
      right_field_list, result_schema);
  return arrow::Status::OK();
}

ConditionedProbeArraysKernel::ConditionedProbeArraysKernel(
    arrow::compute::ExecContext* ctx,
    const std::vector<std::shared_ptr<arrow::Field>>& left_key_list,
    const std::vector<std::shared_ptr<arrow::Field>>& right_key_list,
    const std::shared_ptr<gandiva::Node>& func_node, int join_type,
    const std::vector<std::shared_ptr<arrow::Field>>& left_field_list,
    const std::vector<std::shared_ptr<arrow::Field>>& right_field_list,
    const std::shared_ptr<arrow::Schema>& result_schema) {
  impl_.reset(new Impl(ctx, left_key_list, right_key_list, func_node, join_type,
                       left_field_list, right_field_list, result_schema));
  kernel_name_ = "ConditionedProbeArraysKernel";
}

arrow::Status ConditionedProbeArraysKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ConditionedProbeArraysKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string ConditionedProbeArraysKernel::GetSignature() { return impl_->GetSignature(); }
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
