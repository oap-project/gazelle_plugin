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

#include <arrow/array/concatenate.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernels/sort_to_indices.h>
#include <arrow/compute/kernels/take.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <gandiva/projector.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "third_party/ska_sort.hpp"
#include "precompile/array.h"
#include "precompile/type.h"
#include "array_appender.h"
#include "utils/macros.h"

/**
 *               The Overall Implementation of Sort Kernel
 * In general, there are three kenels to use when sorting for different data.
   They are SortInplaceKernel, SortOnekeyKernel and SortArraysToIndicesKernel.
 * If sorting for one non-string col without payload, SortInplaceKernel is used.
   In this kernel, if sorted data has no null value, ska_sort is used for asc 
   direciton, and std sort is used for desc direciton. If sorted data has null
   value, arrow sort is used.
 * If sorting for one col with payload, or one string col without payload,
   SortOnekeyKernel is used. There are two template classes for this kernel,
   and the key dataype (numeric or string) determines which one will be compiled
   in success. In this kernel, data is partitioned to null and valid value before
   sort. ska_sort is used for asc direciton, and std sort is used for desc 
   direciton.
 * If sorting for multiple cols, SortArraysToIndicesKernel is used.
   This kernel will do codegen, and std sort is used.
 * Projection is supported in all the three kernels. If projection is required, 
   projection is completed before sort, and the projected cols are used to do 
   comparison.
   FIXME: 1. when data has null value, desc and nulls last are not supported in
   Inplace. 2. datatype change after projection is not supported in Inplace.
*/

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using namespace sparkcolumnarplugin::precompile;

template <typename CTYPE>
using enable_if_number = std::enable_if_t<std::is_arithmetic<CTYPE>::value>;

template <typename CTYPE>
using enable_if_string = std::enable_if_t<std::is_same<CTYPE, std::string>::value>;

///////////////  SortArraysToIndices  ////////////////
class SortArraysToIndicesKernel::Impl {
 public:
  Impl() {}
  Impl(arrow::compute::FunctionContext* ctx,
       std::shared_ptr<arrow::Schema> result_schema,
       std::shared_ptr<gandiva::Projector> key_projector,
       std::vector<std::shared_ptr<arrow::DataType>> projected_types,
       std::vector<std::shared_ptr<arrow::Field>> key_field_list,
       std::vector<bool> sort_directions, 
       std::vector<bool> nulls_order)
      : ctx_(ctx), result_schema_(result_schema), 
        key_projector_(key_projector), key_field_list_(key_field_list),
        sort_directions_(sort_directions), nulls_order_(nulls_order), 
        asc_(sort_directions[0]), nulls_first_(nulls_order[0]),
        projected_types_(projected_types) {
    for (auto field : key_field_list) {
      auto indices = result_schema->GetAllFieldIndices(field->name());
      if (indices.size() != 1) {
        std::cout << "[ERROR] SortArraysToIndicesKernel::Impl can't find key "
                  << field->ToString() << " from " << result_schema->ToString()
                  << std::endl;
        throw;
      }
      key_index_list_.push_back(indices[0]);
    }
  }
  virtual ~Impl() {}
  virtual arrow::Status LoadJITFunction(
      std::vector<std::shared_ptr<arrow::Field>> key_field_list,
      std::shared_ptr<arrow::Schema> result_schema) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << (key_projector_? "project" : "original");
    int col_num = sort_directions_.size();
    for (int i = 0; i < col_num; i++) {
        func_args_ss << "[Sorter]" << (nulls_order_[i] ? "nulls_first" : "nulls_last") << "|"
                     << (sort_directions_[i] ? "asc" : "desc");
    }
    for (auto i : key_index_list_) {
      auto field = result_schema->field(i);
      func_args_ss << "[sort_key_" << i << "]" << field->type()->ToString();
    }

    func_args_ss << "[schema]";
    for (auto field : result_schema->fields()) {
      func_args_ss << field->type()->ToString();
    }

#ifdef DEBUG
    std::cout << "func_args_ss is " << func_args_ss.str() << std::endl;
#endif

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    signature_ = signature_ss.str();

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, &sorter);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(result_schema);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature_));
      RETURN_NOT_OK(LoadLibrary(signature_, ctx_, &sorter));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  virtual arrow::Status Evaluate(const ArrayList& in) {
    std::vector<std::shared_ptr<arrow::Array>> outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
    }
    RETURN_NOT_OK(sorter->Evaluate(in, outputs));
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(sorter->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::OK();
  }
  std::string GetSignature() { return signature_; }

 protected:
  std::shared_ptr<CodeGenBase> sorter;
  arrow::compute::FunctionContext* ctx_;
  std::string signature_;
  std::vector<int> key_index_list_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types_;
  std::vector<std::shared_ptr<arrow::Field>> key_field_list_;
  // true for asc, false for desc
  std::vector<bool> sort_directions_;
  // true for nulls_first, false for nulls_last
  std::vector<bool> nulls_order_;
  // keep the direction and nulls order for the first key
  bool nulls_first_;
  bool asc_;

  class TypedSorterCodeGenImpl {
   public:
    TypedSorterCodeGenImpl(std::string indice, std::shared_ptr<arrow::DataType> data_type,
                           std::string name)
        : indice_(indice), data_type_(data_type), name_(name) {}
    std::string GetCachedVariablesDefine() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " << GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      return ss.str();
    }
    std::string GetResultIterDefine() {
      std::stringstream ss;
      ss << "cached_" << indice_ << "_ = cached_" << indice_ << ";" << std::endl;
      ss << "builder_" + indice_ + "_ = std::make_shared<"
         << GetTypeString(data_type_, "Builder") << ">(ctx_->memory_pool());"
         << std::endl;
      return ss.str();
    }
    std::string GetFieldDefine() {
      return "arrow::field(\"" + name_ + "\", data_type_" + indice_ + ")";
    }
    std::string GetResultIterVariables() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " + GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "using BuilderType_" << indice_ << " = "
         << GetTypeString(data_type_, "Builder") << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      ss << "std::shared_ptr<BuilderType_" << indice_ << "> builder_" << indice_ << "_;"
         << std::endl;
      ss << "std::shared_ptr<arrow::DataType> data_type_" << indice_
         << " = arrow::" + GetArrowTypeDefString(data_type_) << ";" << std::endl;
      return ss.str();
    }

   private:
    std::string indice_;
    std::string name_;
    std::shared_ptr<arrow::DataType> data_type_;
  };

  virtual std::string ProduceCodes(std::shared_ptr<arrow::Schema> result_schema) {
    int indice = 0;
    std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list;
    for (auto field : result_schema->fields()) {
      auto codegen = std::make_shared<TypedSorterCodeGenImpl>(
          std::to_string(indice), field->type(), field->name());
      shuffle_typed_codegen_list.push_back(codegen);
      indice++;
    }
    std::string cached_insert_str = GetCachedInsert(shuffle_typed_codegen_list.size(),
                                                    projected_types_.size(),
                                                    key_projector_);
    std::string comp_func_str = GetCompFunction(key_index_list_, key_projector_,
                                                projected_types_, key_field_list_, 
                                                sort_directions_, nulls_order_);

    std::string pre_sort_valid_str = GetPreSortValid();

    std::string pre_sort_null_str = GetPreSortNull();

    std::string sort_func_str = GetSortFunction();

    std::string make_result_iter_str =
        GetMakeResultIter(shuffle_typed_codegen_list.size());

    std::string cached_variables_define_str =
        GetCachedVariablesDefine(shuffle_typed_codegen_list);

    std::string result_iter_param_define_str =
        GetResultIterParamsDefine(shuffle_typed_codegen_list.size());

    std::string result_iter_define_str = GetResultIterDefine(shuffle_typed_codegen_list);

    std::string typed_build_str = GetTypedBuild(shuffle_typed_codegen_list.size());

    std::string result_variables_define_str =
        GetResultIterVariables(shuffle_typed_codegen_list);

    std::string typed_res_array_build_str =
        GetTypedResArrayBuild(shuffle_typed_codegen_list.size());

    std::string typed_res_array_str = GetTypedResArray(shuffle_typed_codegen_list.size());

    std::string projected_variables_str = GetProjectedVariables(key_projector_, 
                                                                projected_types_);
    
    std::string cur_col_str = GetCurCol(key_index_list_[0], key_projector_);

    std::string first_cmp_col_str = GetFirstCmpCol(key_index_list_[0], key_projector_);

    return BaseCodes() + R"(
#include <arrow/buffer.h>

#include <algorithm>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/builder.h"
#include "precompile/type.h"
#include "third_party/ska_sort.hpp"
using namespace sparkcolumnarplugin::precompile;

class TypedSorterImpl : public CodeGenBase {
 public:
  TypedSorterImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}

  arrow::Status Evaluate(const ArrayList& in, const ArrayList& projected_batch) override {
    num_batches_++;
    )" + cached_insert_str 
       + cur_col_str +
           R"(
    items_total_ += cur->length();
    nulls_total_ += cur->null_count();
    length_list_.push_back(cur->length());
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    )" + comp_func_str +
           R"(
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndex);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));

    ArrayItemIndex* indices_begin =
        reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total_;

    int64_t indices_i = 0;
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        (indices_begin + indices_i)->array_id = array_id;
        (indices_begin + indices_i)->id = i;
        indices_i++;
      }
    }

    )" + sort_func_str +
           R"(
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndex) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    )" + make_result_iter_str +
           R"(
    return arrow::Status::OK();
  }

 private:
  )" + cached_variables_define_str
     + projected_variables_str +
           R"(
  std::vector<int64_t> length_list_;
  arrow::compute::FunctionContext* ctx_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                       std::shared_ptr<FixedSizeBinaryArray> indices_in,
   )" + result_iter_param_define_str +
           R"(): ctx_(ctx), total_length_(indices_in->length()), indices_in_cache_(indices_in) {
     )" + result_iter_define_str +
           R"(
      indices_begin_ = (ArrayItemIndex*)indices_in->value_data();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

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
        auto item = indices_begin_ + offset_ + count++;
      )" + typed_build_str +
           R"(
      }
      offset_ += length;
      )" + typed_res_array_build_str +
           R"(
      *out = arrow::RecordBatch::Make(result_schema_, length, {)" +
           typed_res_array_str + R"(});
      return arrow::Status::OK();
    }

   private:
   )" + result_variables_define_str +
           R"(
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
    uint64_t offset_ = 0;
    ArrayItemIndex* indices_begin_;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
  };
};

extern "C" void MakeCodeGen(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedSorterImpl>(ctx);
}

    )";
  }
  std::string GetCachedInsert(int shuffle_size, int projected_size,
                              const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "cached_" << i << "_.push_back(std::make_shared<ArrayType_" << i 
          << ">(in[" << i << "]));" << std::endl;
    }
    if (key_projector) {
      for (int i = 0; i < projected_size; i++) {
        ss << "projected_" << i << "_.push_back(std::make_shared<ProjectedType_" << i 
           << ">(projected_batch[" << i << "]));" << std::endl;
      }
    }
    return ss.str();
  }
  std::string GetCompFunction(const std::vector<int>& sort_key_index_list, 
                              const std::shared_ptr<gandiva::Projector>& key_projector,
                              const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
                              const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
                              const std::vector<bool>& sort_directions, 
                              const std::vector<bool>& nulls_order) {
    std::stringstream ss;
    bool projected;
    if (key_projector) {
      projected = true;
    } else {
      projected = false;
    }
    ss << "auto comp = [this](ArrayItemIndex x, ArrayItemIndex y) {"
       << GetCompFunction_(0, projected, sort_key_index_list, key_field_list, 
                           projected_types, sort_directions, nulls_order) << "};";
    return ss.str();
  }
  std::string GetCompFunction_(int cur_key_index, bool projected, 
                               const std::vector<int>& sort_key_index_list,
                               const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
                               const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
                               const std::vector<bool>& sort_directions, 
                               const std::vector<bool>& nulls_order) {
    std::string comp_str;
    int cur_key_id;
    auto field = key_field_list[cur_key_index];
    bool asc = sort_directions[cur_key_index];
    bool nulls_first = nulls_order[cur_key_index];
    std::shared_ptr<arrow::DataType> data_type;
    std::string array;
    // if projected, use projected batch to compare, and use projected type
    if (projected) {
      array = "projected_";
      data_type = projected_types[cur_key_index];
      // use the index of projected key
      cur_key_id = cur_key_index;
    } else {
      array = "cached_";
      data_type = field->type();
      // use the key_id
      cur_key_id = sort_key_index_list[cur_key_index];
    }

    auto x_num_value = array + std::to_string(cur_key_id) + "_[x.array_id]->GetView(x.id)";
    auto x_str_value = array + std::to_string(cur_key_id) + "_[x.array_id]->GetString(x.id)";
    auto y_num_value = array + std::to_string(cur_key_id) + "_[y.array_id]->GetView(y.id)";
    auto y_str_value = array + std::to_string(cur_key_id) + "_[y.array_id]->GetString(y.id)";
    auto is_x_null = array + std::to_string(cur_key_id) + "_[x.array_id]->IsNull(x.id)";
    auto is_y_null = array + std::to_string(cur_key_id) + "_[y.array_id]->IsNull(y.id)";

    // Multiple keys sorting w/ nulls first/last is supported.
    std::stringstream ss;
    // For cols except the first one, we need to determine the position of nulls.
    ss << "if (" << is_x_null << " && " << is_y_null << ") {\n";
    ss << "return false;} " << "else if (" << is_x_null << ") {\n";
    // If value accessed from x is null, return true to make nulls first.
    if (nulls_first) {
      ss << "return true;\n}";
    } else {
      ss << "return false;\n}";
    }
    // If value accessed from y is null, return false to make nulls first.
    ss << " else if (" << is_y_null << ") {\n";
    if (nulls_first) {
      ss << "return false;\n}";
    } else {
      ss << "return true;\n}";
    }
    // If values accessed from x and y are both not null
    ss << " else {\n";

    // Multiple keys sorting w/ different ordering is supported.
    // For string type of data, GetString should be used instead of GetView.
    if (asc) {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " < " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " < " << y_num_value << ";\n}\n";
      }
    } else {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " > " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " > " << y_num_value << ";\n}\n";
      }
    }
    comp_str = ss.str();
    if ((cur_key_index + 1) == sort_key_index_list.size()) {
      return comp_str;
    }
    // clear the contents of stringstream
    ss.str(std::string());
    if (data_type->id() == arrow::Type::STRING) {
      ss << "if ((" <<  is_x_null << " && " << is_y_null << ") || (" 
         << x_str_value << " == " << y_str_value << ")) {";
    } else {
      ss << "if ((" <<  is_x_null << " && " << is_y_null << ") || (" 
         << x_num_value << " == " << y_num_value << ")) {";
    }
    ss << GetCompFunction_(
        cur_key_index + 1, projected, sort_key_index_list, key_field_list, 
        projected_types, sort_directions, nulls_order) 
        << "} else { " << comp_str << "}";
    return ss.str();
  }
  std::string GetPreSortValid() {
    if (nulls_first_) {
      return R"(
    (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
    (indices_begin + nulls_total_ + indices_i)->id = i;)";
    } else {
      return R"(
    (indices_begin + indices_i)->array_id = array_id;
    (indices_begin + indices_i)->id = i;)";
    }
  }
  std::string GetPreSortNull() {
    if (nulls_first_) {
      return R"(
    (indices_begin + indices_null)->array_id = array_id;
    (indices_begin + indices_null)->id = i;)";
    } else {
      return R"(
    (indices_end - nulls_total_ + indices_null)->array_id = array_id;
    (indices_end - nulls_total_ + indices_null)->id = i;)";
    }
  }
  std::string GetSortFunction() {
    return "std::sort(indices_begin, indices_begin + "
            "items_total_, "
            "comp);"; 
  }
  std::string GetMakeResultIter(int shuffle_size) {
    std::stringstream ss;
    std::stringstream params_ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        params_ss << "cached_" << i << "_,";
      } else {
        params_ss << "cached_" << i << "_";
      }
    }
    auto params = params_ss.str();
    ss << "*out = std::make_shared<SorterResultIterator>(ctx_, indices_out, " << params
       << ");";
    return ss.str();
  }
  std::string GetCachedVariablesDefine(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetCachedVariablesDefine() << std::endl;
    }
    return ss.str();
  }
  std::string GetResultIterParamsDefine(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        ss << "std::vector<std::shared_ptr<ArrayType_" << i << ">> cached_" << i << ","
           << std::endl;
      } else {
        ss << "std::vector<std::shared_ptr<ArrayType_" << i << ">> cached_" << i;
      }
    }
    return ss.str();
  }
  std::string GetResultIterDefine(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    std::stringstream field_define_ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetResultIterDefine() << std::endl;
      if (codegen != *(shuffle_typed_codegen_list.end() - 1)) {
        field_define_ss << codegen->GetFieldDefine() << ",";
      } else {
        field_define_ss << codegen->GetFieldDefine();
      }
    }
    ss << "result_schema_ = arrow::schema({" << field_define_ss.str() << "});\n"
       << std::endl;
    return ss.str();
  }
  std::string GetTypedBuild(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "if (!cached_" << i << "_[item->array_id]->IsNull(item->id)) {\n"
         << "  RETURN_NOT_OK(builder_" << i << "_->Append(cached_" << i
         << "_[item->array_id]->GetView(item->id)));\n"
         << "} else {\n"
         << "  RETURN_NOT_OK(builder_" << i << "_->AppendNull());\n"
         << "}" << std::endl;
    }
    return ss.str();
  }
  std::string GetTypedResArrayBuild(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "std::shared_ptr<arrow::Array> out_" << i << ";\n"
         << "RETURN_NOT_OK(builder_" << i << "_->Finish(&out_" << i << "));\n"
         << "builder_" << i << "_->Reset();" << std::endl;
    }
    return ss.str();
  }
  std::string GetTypedResArray(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      if (i + 1 < shuffle_size) {
        ss << "out_" << i << ", ";
      } else {
        ss << "out_" << i;
      }
    }
    return ss.str();
  }
  std::string GetResultIterVariables(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetResultIterVariables() << std::endl;
    }
    return ss.str();
  }
  std::string GetProjectedVariables(
      const std::shared_ptr<gandiva::Projector>& key_projector,
      const std::vector<std::shared_ptr<arrow::DataType>>& projected_types) {
    std::stringstream ss;
    if (key_projector) {
      for (int i = 0; i < projected_types.size(); i++) {
        ss << "using ProjectedType_" << i << " = " 
               + GetTypeString(projected_types[i], "Array") << ";" << std::endl;
        ss << "std::vector<std::shared_ptr<ProjectedType_" << i << ">> projected_" 
           << i << "_;" << std::endl;
        ss << std::endl;
      }
    }
    return ss.str();
  }
  std::string GetCurCol(int first_key_id,
      const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    std::string array;
    if (key_projector) {
      array = "projected_";
      ss << "auto cur = " + array << "0_[" + array << "0_.size() - 1];" 
         << std::endl;
    } else {
      array = "cached_";
      ss << "auto cur = " + array << first_key_id << "_[" + array 
         << first_key_id << "_.size() - 1];" << std::endl;
    }
    return ss.str();   
  }
  std::string GetFirstCmpCol(int first_key_id,
      const std::shared_ptr<gandiva::Projector>& key_projector) {
    std::stringstream ss;
    std::string array;
    if (key_projector) {
      array = "projected_";
      ss << array << "0_[array_id]";
    } else {
      array = "cached_";
      ss << array << first_key_id << "_[array_id]";
    }
    return ss.str();   
  }
};

///////////////  SortArraysInPlace  ////////////////
template <typename DATATYPE, typename CTYPE>
class SortInplaceKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortInplaceKernel(arrow::compute::FunctionContext* ctx,
                    std::shared_ptr<arrow::Schema> result_schema,
                    std::shared_ptr<gandiva::Projector> key_projector,
                    std::vector<bool> sort_directions, 
                    std::vector<bool> nulls_order)
      : ctx_(ctx), nulls_first_(nulls_order[0]), asc_(sort_directions[0]),
        result_schema_(result_schema), key_projector_(key_projector) {}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_0_.push_back(outputs[0]);   
      nulls_total_ += outputs[0]->null_count(); 
    } else {
      cached_0_.push_back(in[0]);
      nulls_total_ += in[0]->null_count();
    }

    items_total_ += in[0]->length();
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    RETURN_NOT_OK(
        arrow::Concatenate(cached_0_, ctx_->memory_pool(), &concatenated_array_));
    CTYPE* indices_begin = concatenated_array_->data()->GetMutableValues<CTYPE>(1);
    CTYPE* indices_end = indices_begin + concatenated_array_->length();
    auto valid_indices_begin = indices_begin;
    auto valid_indices_end = indices_end;
    if (nulls_total_ > 0) {
      // we use arrow sort for this scenario
      // the indices_out of arrow sort is nulls_last
      std::shared_ptr<arrow::Array> indices_out;
      RETURN_NOT_OK(
          arrow::compute::SortToIndices(ctx_, *concatenated_array_.get(), &indices_out));
      std::shared_ptr<arrow::Array> sort_out;
      arrow::compute::TakeOptions options;
      RETURN_NOT_OK(arrow::compute::Take(ctx_, *concatenated_array_.get(),
                                         *indices_out.get(), options, &sort_out));
      *out = std::make_shared<SorterResultIterator>(ctx_, schema, sort_out);
    } else {
      if (asc_) {
        ska_sort(valid_indices_begin, valid_indices_end);
      } else {
        auto desc_comp = [this](CTYPE& x, CTYPE& y) { return x > y; };
        std::sort(valid_indices_begin, valid_indices_end, desc_comp);
      }
      *out = std::make_shared<SorterResultIterator>(ctx_, schema, concatenated_array_);
    }
    return arrow::Status::OK();
  }

 private:
  arrow::ArrayVector cached_0_;
  std::shared_ptr<arrow::Array> concatenated_array_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                         std::shared_ptr<arrow::Schema> result_schema,
                         std::shared_ptr<arrow::Array> result_arr)
        : ctx_(ctx),
          result_schema_(result_schema),
          total_length_(result_arr->length()),
          nulls_total_(result_arr->null_count()) {
      result_arr_ = std::dynamic_pointer_cast<ArrayType_0>(result_arr);
      std::unique_ptr<arrow::ArrayBuilder> builder_0;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_0, &builder_0);
      builder_0_.reset(
          arrow::internal::checked_cast<BuilderType_0*>(builder_0.release()));
      batch_size_ = GetBatchSize();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (total_offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - total_offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - total_offset_);
      /**
       * Here we take value from the sorted result_arr_ and append to builder.
       * valid_count is used to count the valid value, for accessing the valid value 
         in result_arr_.
       * total_count is used to count both valid and null value, for determing if all values
         are appended to builder in while loop.
       * valid_offset_ is used to count the total added valid value, for accessing the valid value
         in result_arr_.
       * total_offset_ is used to count the total added null and valid value.
      **/
      uint64_t valid_count = 0;
      uint64_t total_count = 0;
      if (total_offset_ >= nulls_total_) {
        // If no null value
        while (total_count < length) {
          RETURN_NOT_OK(builder_0_->Append(result_arr_->GetView(valid_offset_ + valid_count)));
          valid_count++;
          total_count++;
        }
        total_offset_ += length;
        valid_offset_ += length;
      } else {
        // If has null value
        while (total_count < length) {
          if ((total_offset_ + total_count) < nulls_total_) {
            // Append nulls first
            // TODO: support nulls_last
            RETURN_NOT_OK(builder_0_->AppendNull());
          } else {
            // After appending all null value, append valid value
            // Because result_arr_ from arrow sort is nulls_last, valid_count is used to
            // access data from the beginning of result_arr_.
            RETURN_NOT_OK(builder_0_->Append(result_arr_->GetView(valid_offset_ + valid_count)));
            // Add valid_count after appending one valid value
            valid_count++;
          }
          // Add total_count for both valid and null value
          total_count++;
        }
        valid_offset_ += valid_count;
        total_offset_ += length;
      }
      std::shared_ptr<arrow::Array> out_0;
      RETURN_NOT_OK(builder_0_->Finish(&out_0));
      builder_0_->Reset();

      *out = arrow::RecordBatch::Make(result_schema_, length, {out_0});
      return arrow::Status::OK();
    }

   private:
    using ArrayType_0 = typename arrow::TypeTraits<DATATYPE>::ArrayType;
    using BuilderType_0 = typename arrow::TypeTraits<DATATYPE>::BuilderType;
    std::shared_ptr<arrow::DataType> data_type_0 =
        arrow::TypeTraits<DATATYPE>::type_singleton();
    std::shared_ptr<ArrayType_0> result_arr_;
    std::shared_ptr<BuilderType_0> builder_0_;

    uint64_t total_offset_ = 0;
    uint64_t valid_offset_ = 0;
    const uint64_t total_length_;
    const uint64_t nulls_total_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::FunctionContext* ctx_;
    uint64_t batch_size_;
  };
};

template <typename DATATYPE, typename CTYPE, typename Enable = void>
class SortOnekeyKernel {};

///////////////  SortArraysOneKey  ////////////////
// This class is used when key type is arithmetic
template <typename DATATYPE, typename CTYPE>
class SortOnekeyKernel<DATATYPE, CTYPE, enable_if_number<CTYPE>> 
  : public SortArraysToIndicesKernel::Impl {
 public:
  SortOnekeyKernel(arrow::compute::FunctionContext* ctx,
                   std::shared_ptr<arrow::Schema> result_schema,
                   std::shared_ptr<gandiva::Projector> key_projector,
                   std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                   std::vector<bool> sort_directions, 
                   std::vector<bool> nulls_order)
      : ctx_(ctx), nulls_first_(nulls_order[0]), asc_(sort_directions[0]), 
        result_schema_(result_schema), key_projector_(key_projector) {
      #ifdef DEBUG
          std::cout << "UseSortOneKeyForArithmetic" << std::endl;
      #endif
      auto indices = result_schema->GetAllFieldIndices(key_field_list[0]->name());
      if (indices.size() < 1) {
        std::cout << "[ERROR] SortOnekeyKernel for arithmetic can't find key "
                  << key_field_list[0]->ToString() << " from " 
                  << result_schema->ToString() << std::endl;
        throw;
      }
      key_id_ = indices[0];
      col_num_ = result_schema->num_fields();
  }
  ~SortOnekeyKernel(){}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[key_id_]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(outputs[0]));   
      nulls_total_ += outputs[0]->null_count(); 
    } else {
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(in[key_id_]));
      nulls_total_ += in[key_id_]->null_count();
    }

    items_total_ += in[key_id_]->length();
    length_list_.push_back(in[key_id_]->length());
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {    
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndex);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));
    // start to partition not_null with null
    ArrayItemIndex* indices_begin = 
      reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total_;
    int64_t indices_i = 0;
    int64_t indices_null = 0;
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        if (nulls_first_) {
          if (!cached_key_[array_id]->IsNull(i)) {
            (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
            (indices_begin + nulls_total_ + indices_i)->id = i;
            indices_i++;
          } else {
            (indices_begin + indices_null)->array_id = array_id;
            (indices_begin + indices_null)->id = i;
            indices_null++;
          }
        } else {
          if (!cached_key_[array_id]->IsNull(i)) {
            (indices_begin + indices_i)->array_id = array_id;
            (indices_begin + indices_i)->id = i;
            indices_i++;
          } else {
            (indices_end - nulls_total_ + indices_null)->array_id = array_id;
            (indices_end - nulls_total_ + indices_null)->id = i;
            indices_null++;
          }
        }
      }
    }
    if (asc_) {
      if (nulls_first_) {
        ska_sort(indices_begin + nulls_total_, indices_begin + items_total_, 
            [this](auto& x) -> decltype(auto){ return cached_key_[x.array_id]->GetView(x.id); });
      } else {
        ska_sort(indices_begin, indices_begin + items_total_ - nulls_total_, 
            [this](auto& x) -> decltype(auto){ return cached_key_[x.array_id]->GetView(x.id); });
      }
    } else {
      auto comp = [this](ArrayItemIndex x, ArrayItemIndex y) {
        return cached_key_[x.array_id]->GetView(x.id) > cached_key_[y.array_id]->GetView(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    }
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndex) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_key = typename arrow::TypeTraits<DATATYPE>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_key>> cached_key_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;
  int col_num_;
  int key_id_;
  
  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                         std::shared_ptr<arrow::Schema> schema,
                         std::shared_ptr<FixedSizeBinaryArray> indices_in,
                         std::vector<arrow::ArrayVector>& cached)
        : ctx_(ctx),
          schema_(schema),
          indices_in_cache_(indices_in),
          total_length_(indices_in->length()),
          cached_in_(cached) {
      col_num_ = schema->num_fields();
      indices_begin_ = (ArrayItemIndex*)indices_in->value_data();
      // appender_type won't be used
      AppenderBase::AppenderType appender_type = AppenderBase::left;
      for (int i = 0; i < col_num_; i++) {
        auto field = schema->field(i);
        std::shared_ptr<AppenderBase> appender;
        MakeAppender(ctx_, field->type(), appender_type, &appender);
        appender_list_.push_back(appender);
      }
      for (int i = 0; i < col_num_; i++) {
        arrow::ArrayVector array_vector = cached_in_[i];
        int array_num = array_vector.size();
        for (int array_id = 0; array_id < array_num; array_id++) {
          auto arr = array_vector[array_id];
          appender_list_[i]->AddArray(arr);
        }
      }
      batch_size_ = GetBatchSize();
    }
    ~SorterResultIterator(){}

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - offset_);
      uint64_t count = 0;
      for (int i = 0; i < col_num_; i++) {
        while (count < length) {
          auto item = indices_begin_ + offset_ + count++;
          RETURN_NOT_OK(appender_list_[i]->Append(item->array_id, item->id));
        }
        count = 0;
      }
      offset_ += length;
      ArrayList arrays;
      for (int i = 0; i < col_num_; i++) {
        std::shared_ptr<arrow::Array> out_array;
        RETURN_NOT_OK(appender_list_[i]->Finish(&out_array));
        arrays.push_back(out_array);
        appender_list_[i]->Reset();
      }

      *out = arrow::RecordBatch::Make(schema_, length, arrays);
      return arrow::Status::OK();
    }

   private:
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> schema_;
    arrow::compute::FunctionContext* ctx_;
    uint64_t batch_size_;
    int col_num_;
    ArrayItemIndex* indices_begin_;
    std::vector<arrow::ArrayVector> cached_in_;
    std::vector<std::shared_ptr<arrow::DataType>> type_list_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    std::vector<std::shared_ptr<arrow::Array>> array_list_;
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
  };
};

///////////////  SortArraysOneKey  ////////////////
// This class is used when key type is string
template <typename DATATYPE, typename CTYPE>
class SortOnekeyKernel<DATATYPE, CTYPE, enable_if_string<CTYPE>> 
  : public SortArraysToIndicesKernel::Impl {
 public:
  SortOnekeyKernel(arrow::compute::FunctionContext* ctx, 
      std::shared_ptr<arrow::Schema> result_schema,
      std::shared_ptr<gandiva::Projector> key_projector,
      std::vector<std::shared_ptr<arrow::Field>> key_field_list,
      std::vector<bool> sort_directions, 
      std::vector<bool> nulls_order)
      : ctx_(ctx), nulls_first_(nulls_order[0]), asc_(sort_directions[0]), 
        result_schema_(result_schema), key_projector_(key_projector) {
      #ifdef DEBUG
          std::cout << "UseSortOneKeyForString" << std::endl;
      #endif
      auto indices = result_schema->GetAllFieldIndices(key_field_list[0]->name());
      if (indices.size() < 1) {
        std::cout << "[ERROR] SortOnekeyKernel for string can't find key "
                  << key_field_list[0]->ToString() << " from " 
                  << result_schema->ToString() << std::endl;
        throw;
      }
      key_id_ = indices[0];
      col_num_ = result_schema->num_fields();
  }

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[key_id_]->length() : 0;
      auto in_batch =
          arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(outputs[0]));   
      nulls_total_ += outputs[0]->null_count(); 
    } else {
      cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(in[key_id_]));
      nulls_total_ += in[key_id_]->null_count();
    }

    items_total_ += in[key_id_]->length();
    length_list_.push_back(in[key_id_]->length());
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {    
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndex);
    RETURN_NOT_OK(arrow::AllocateBuffer(ctx_->memory_pool(), buf_size, &indices_buf));
    // start to partition not_null with null
    ArrayItemIndex* indices_begin = 
      reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total_;
    int64_t indices_i = 0;
    int64_t indices_null = 0;
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        if (nulls_first_) {
          if (!cached_key_[array_id]->IsNull(i)) {
            (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
            (indices_begin + nulls_total_ + indices_i)->id = i;
            indices_i++;
          } else {
            (indices_begin + indices_null)->array_id = array_id;
            (indices_begin + indices_null)->id = i;
            indices_null++;
          }
        } else {
          if (!cached_key_[array_id]->IsNull(i)) {
            (indices_begin + indices_i)->array_id = array_id;
            (indices_begin + indices_i)->id = i;
            indices_i++;
          } else {
            (indices_end - nulls_total_ + indices_null)->array_id = array_id;
            (indices_end - nulls_total_ + indices_null)->id = i;
            indices_null++;
          }
        }
      }
    }
    if (asc_) {
      auto comp = [this](ArrayItemIndex x, ArrayItemIndex y) {
        return cached_key_[x.array_id]->GetString(x.id) < cached_key_[y.array_id]->GetString(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    } else {
      auto comp = [this](ArrayItemIndex x, ArrayItemIndex y) {
        return cached_key_[x.array_id]->GetString(x.id) > cached_key_[y.array_id]->GetString(y.id);};
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    }
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndex) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_key = typename arrow::TypeTraits<DATATYPE>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_key>> cached_key_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;
  int col_num_;
  int key_id_;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::FunctionContext* ctx,
                         std::shared_ptr<arrow::Schema> schema,
                         std::shared_ptr<FixedSizeBinaryArray> indices_in,
                         std::vector<arrow::ArrayVector>& cached)
        : ctx_(ctx),
          schema_(schema),
          indices_in_cache_(indices_in),
          total_length_(indices_in->length()),
          cached_in_(cached) {
      col_num_ = schema->num_fields();
      indices_begin_ = (ArrayItemIndex*)indices_in->value_data();
      // appender_type won't be used
      AppenderBase::AppenderType appender_type = AppenderBase::left;
      for (int i = 0; i < col_num_; i++) {
        auto field = schema->field(i);
        std::shared_ptr<AppenderBase> appender;
        MakeAppender(ctx_, field->type(), appender_type, &appender);
        appender_list_.push_back(appender);
      }
      for (int i = 0; i < col_num_; i++) {
        arrow::ArrayVector array_vector = cached_in_[i];
        int array_num = array_vector.size();
        for (int array_id = 0; array_id < array_num; array_id++) {
          auto arr = array_vector[array_id];
          appender_list_[i]->AddArray(arr);
        }
      }
      batch_size_ = GetBatchSize();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      auto length = (total_length_ - offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - offset_);
      uint64_t count = 0;
      for (int i = 0; i < col_num_; i++) {
        while (count < length) {
          auto item = indices_begin_ + offset_ + count++;
          RETURN_NOT_OK(appender_list_[i]->Append(item->array_id, item->id));
        }
        count = 0;
      }
      offset_ += length;
      ArrayList arrays;
      for (int i = 0; i < col_num_; i++) {
        std::shared_ptr<arrow::Array> out_array;
        RETURN_NOT_OK(appender_list_[i]->Finish(&out_array));
        arrays.push_back(out_array);
        appender_list_[i]->Reset();
      }

      *out = arrow::RecordBatch::Make(schema_, length, arrays);
      return arrow::Status::OK();
    }

   private:
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> schema_;
    arrow::compute::FunctionContext* ctx_;
    uint64_t batch_size_;
    int col_num_;
    ArrayItemIndex* indices_begin_;
    std::vector<arrow::ArrayVector> cached_in_;
    std::vector<std::shared_ptr<arrow::DataType>> type_list_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    std::vector<std::shared_ptr<arrow::Array>> array_list_;
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
  };
};

arrow::Status SortArraysToIndicesKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, std::vector<bool> nulls_order,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SortArraysToIndicesKernel>(
    ctx, result_schema, sort_key_node, key_field_list, sort_directions, nulls_order);
  return arrow::Status::OK();
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
  PROCESS(arrow::Date64Type)
SortArraysToIndicesKernel::SortArraysToIndicesKernel(
    arrow::compute::FunctionContext* ctx,
    std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, 
    std::vector<bool> nulls_order) {
  // sort_key_node may need to do projection
  bool pre_processed_key_ = false;
  gandiva::NodePtr key_project;
  gandiva::ExpressionVector key_project_exprs;

  for (auto node : sort_key_node) {
    std::shared_ptr<TypedNodeVisitor> node_visitor;
    THROW_NOT_OK(MakeTypedNodeVisitor(node, &node_visitor));
    if (node_visitor->GetResultType() != TypedNodeVisitor::FieldNode) {
      pre_processed_key_ = true;
      break;
    }
  }
  // If not all key_node are FieldNode, we need to do projection
  std::shared_ptr<gandiva::Projector> key_projector;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types;
  if (pre_processed_key_) {
    key_project_exprs = GetGandivaKernel(sort_key_node);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    THROW_NOT_OK(gandiva::Projector::Make(result_schema, key_project_exprs,
                                          configuration, &key_projector));
    for (const auto& expr : key_project_exprs) {
      auto key_type = expr->root()->return_type();
      projected_types.push_back(key_type);
    }
  }

  if (key_field_list.size() == 1 && result_schema->num_fields() == 1 
      && key_field_list[0]->type()->id() != arrow::Type::STRING) {
    // Will use SortInplace when sorting for one non-string col
#ifdef DEBUG
    std::cout << "UseSortInplace" << std::endl;
#endif
    switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using CType = typename arrow::TypeTraits<InType>::CType;                  \
    impl_.reset(new SortInplaceKernel<InType, CType>(ctx, result_schema, key_projector, sort_directions, nulls_order)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "SortInplaceKernel type not supported, type is "
                  << key_field_list[0]->type() << std::endl;
      } break;
    }
  } else if (key_field_list.size() == 1 && result_schema->num_fields() >= 1) {
    // Will use SortOnekey when: 
    // 1. sorting for one col inside several cols 2. sorting for one string col
#ifdef DEBUG
    std::cout << "UseSortOneKey" << std::endl;
#endif
    if (pre_processed_key_) {
      // if needs projection, will use projected type for key col
      if (projected_types[0]->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType, std::string>(
          ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order));
      } else {
        switch (projected_types[0]->id()) {
#define PROCESS(InType)                                                       \
    case InType::type_id: {                                                   \
      using CType = typename arrow::TypeTraits<InType>::CType;                \
      impl_.reset(new SortOnekeyKernel<InType, CType>(ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order)); \
    } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    } else {
      // if no projection, will use the original type for key col
      if (key_field_list[0]->type()->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType, std::string>(
          ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order));
      } else {
        switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                       \
    case InType::type_id: {                                                   \
      using CType = typename arrow::TypeTraits<InType>::CType;                \
      impl_.reset(new SortOnekeyKernel<InType, CType>(ctx, result_schema, key_projector, key_field_list, sort_directions, nulls_order)); \
    } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    }
  } else {
    // Will use Sort Codegen when sorting for several cols
    impl_.reset(new Impl(ctx, result_schema, key_projector, projected_types, 
                         key_field_list, sort_directions, nulls_order));
    auto status = impl_->LoadJITFunction(key_field_list, result_schema);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }
  kernel_name_ = "SortArraysToIndicesKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status SortArraysToIndicesKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SortArraysToIndicesKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string SortArraysToIndicesKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
