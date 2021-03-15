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
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
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
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "precompile/array.h"
#include "precompile/builder.h"
#include "precompile/type.h"
#include "third_party/ska_sort.hpp"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using namespace sparkcolumnarplugin::precompile;

class AppenderBase {
 public:
  virtual ~AppenderBase() {}

  virtual arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    return arrow::Status::NotImplemented("AppenderBase AddArray is abstract.");
  }

  virtual arrow::Status Append(uint16_t& array_id, uint16_t& item_id) {
    return arrow::Status::NotImplemented("AppenderBase Append is abstract.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    return arrow::Status::NotImplemented("AppenderBase Finish is abstract.");
  }

  virtual arrow::Status Reset() {
    return arrow::Status::NotImplemented("AppenderBase Reset is abstract.");
  }
};

template <class DataType>
class ArrayAppender : public AppenderBase {
 public:
  ArrayAppender(arrow::compute::ExecContext* ctx) : ctx_(ctx) {
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType_*>(array_builder.release()));
  }
  ~ArrayAppender() {}

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType_>(arr);
    cached_arr_.emplace_back(typed_arr_);
    return arrow::Status::OK();
  }

  arrow::Status Append(uint16_t& array_id, uint16_t& item_id) {
    if (!cached_arr_[array_id]->IsNull(item_id)) {
      auto val = cached_arr_[array_id]->GetView(item_id);
      builder_->Append(cached_arr_[array_id]->GetView(item_id));
    } else {
      builder_->AppendNull();
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out_) {
    builder_->Finish(out_);
    return arrow::Status::OK();
  }

  arrow::Status Reset() {
    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using BuilderType_ = typename arrow::TypeTraits<DataType>::BuilderType;
  using ArrayType_ = typename arrow::TypeTraits<DataType>::ArrayType;
  std::unique_ptr<BuilderType_> builder_;
  std::vector<std::shared_ptr<ArrayType_>> cached_arr_;
  arrow::compute::ExecContext* ctx_;
};

///////////////  SortArraysToIndices  ////////////////
class WindowSortKernel::Impl {
 public:
  Impl() {}
  Impl(arrow::compute::ExecContext* ctx,
       std::vector<std::shared_ptr<arrow::Field>> key_field_list,
       std::shared_ptr<arrow::Schema> result_schema, bool nulls_first, bool asc)
      : ctx_(ctx), nulls_first_(nulls_first), asc_(asc) {
    for (auto field : key_field_list) {
      auto indices = result_schema->GetAllFieldIndices(field->name());
      if (indices.size() != 1) {
        std::cout << "[ERROR] WindowSortKernel::Impl can't find key " << field->ToString()
                  << " from " << result_schema->ToString() << std::endl;
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
    func_args_ss << "[Sorter]" << (nulls_first_ ? "nulls_first" : "nulls_last") << "|"
                 << (asc_ ? "asc" : "desc");
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
    RETURN_NOT_OK(sorter->Evaluate(in));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array> in,
                               std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(sorter->Finish(in, out));
    return arrow::Status::OK();
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::OK();
  }
  std::string GetSignature() { return signature_; }

 protected:
  std::shared_ptr<CodeGenBase> sorter;
  arrow::compute::ExecContext* ctx_;
  std::string signature_;
  bool nulls_first_;
  bool asc_;
  std::vector<int> key_index_list_;
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
    std::string cached_insert_str = GetCachedInsert(shuffle_typed_codegen_list.size());
    std::string comp_func_str = GetCompFunction(key_index_list_);

    std::string sort_func_str = GetSortFunction(key_index_list_);

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

    return BaseCodes() + R"(
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>

#include <algorithm>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "precompile/builder.h"
#include "precompile/type.h"
#include "third_party/ska_sort.hpp"
using namespace sparkcolumnarplugin::precompile;

class TypedSorterImpl : public CodeGenBase {
 public:
  TypedSorterImpl(arrow::compute::ExecContext* ctx) : ctx_(ctx) {}

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    )" + cached_insert_str +
           R"(
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<arrow::Array> in, std::shared_ptr<FixedSizeBinaryArray>* out) {
    )" + comp_func_str +
           R"(

    std::shared_ptr<arrow::UInt64Array> selected = std::dynamic_pointer_cast<arrow::UInt64Array>(in);
    int items_total = selected->length();

    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total * sizeof(ArrayItemIndex);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf = *std::move(maybe_buffer);

    // start to partition not_null with null
    ArrayItemIndex* indices_begin =
        reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total;

    int64_t indices_i = 0;

    // we should support nulls first and nulls last here
    // we should also support desc and asc here

    for (int i = 0; i < selected->length(); i++) {
      uint64_t encoded = selected->GetView(i);
      uint16_t array_id = (encoded & 0xFFFF0000U) >> 16U;
      uint16_t id = encoded & 0xFFFFU;
      (indices_begin + indices_i)->array_id = array_id;
      (indices_begin + indices_i)->id = id;
      indices_i++;
    }
    )" + sort_func_str +
           R"(
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndex) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array> in, std::shared_ptr<arrow::Array>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(in, &indices_out));
    arrow::UInt64Builder builder;
    auto *index = (ArrayItemIndex *) indices_out->value_data();
    for (int i = 0; i < indices_out->length(); i++) {
      uint64_t encoded = ((uint64_t) (index->array_id) << 16U) ^ ((uint64_t) (index->id));
      RETURN_NOT_OK(builder.Append(encoded));
      index++;
    }
    RETURN_NOT_OK(builder.Finish(out));
    return arrow::Status::OK();
  }

 private:
  )" + cached_variables_define_str +
           R"(
  arrow::compute::ExecContext* ctx_;
  uint64_t num_batches_ = 0;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::ExecContext* ctx,
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
    arrow::compute::ExecContext* ctx_;
  };
};

extern "C" void MakeCodeGen(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedSorterImpl>(ctx);
}

    )";
  }
  std::string GetCachedInsert(int shuffle_size) {
    std::stringstream ss;
    for (int i = 0; i < shuffle_size; i++) {
      ss << "cached_" << i << "_.push_back(std::make_shared<ArrayType_" << i << ">(in["
         << i << "]));" << std::endl;
    }
    return ss.str();
  }
  std::string GetCompFunction(std::vector<int> sort_key_index_list) {
    std::stringstream ss;
    ss << "auto comp = [this](const ArrayItemIndex& x, const ArrayItemIndex& "
          "y) {"
       << GetCompFunction_(0, sort_key_index_list) << "};";
    return ss.str();
  }
  std::string GetCompFunction_(int cur_key_index, std::vector<int> sort_key_index_list) {
    std::string comp_str;

    auto cur_key_id = sort_key_index_list[cur_key_index];
    // todo nulls last / nulls first
    auto x_value =
        "cached_" + std::to_string(cur_key_id) + "_[x.array_id]->GetView(x.id)";
    auto y_value =
        "cached_" + std::to_string(cur_key_id) + "_[y.array_id]->GetView(y.id)";

    auto is_x_null =
        "cached_" + std::to_string(cur_key_id) + "_[x.array_id]->IsNull(x.id)";
    auto is_y_null =
        "cached_" + std::to_string(cur_key_id) + "_[y.array_id]->IsNull(y.id)";

    if (asc_) {
      std::stringstream ss;
      ss << "return " << is_x_null << " && " << is_y_null << " ? "
         << "false"
         << " : "
         << "(" << is_x_null << " ? " << !nulls_first_ << " : "
         << "(" << is_y_null << " ? " << nulls_first_ << " : "
         << "(" << x_value << " < " << y_value << ")));\n";
      comp_str = ss.str();
    } else {
      std::stringstream ss;
      ss << "return " << is_x_null << " && " << is_y_null << " ? "
         << "false"
         << " : "
         << "(" << is_x_null << " ? " << !nulls_first_ << " : "
         << "(" << is_y_null << " ? " << nulls_first_ << " : "
         << "(" << x_value << " > " << y_value << ")));\n";
      comp_str = ss.str();
    }

    if (cur_key_index == sort_key_index_list.size() - 1) {
      return comp_str;
    }
    std::stringstream ss;
    ss << "if (" << x_value << " == " << y_value << " || (" << is_x_null << " && "
       << is_y_null << ")) {" << GetCompFunction_(cur_key_index + 1, sort_key_index_list)
       << "} else { " << comp_str << "}";
    return ss.str();
  }

  std::string GetSortFunction(std::vector<int>& key_index_list) {
    return "std::sort(indices_begin, indices_begin + "
           "items_total, "
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
};

///////////////  SortArraysOneKey  ////////////////
template <typename DATATYPE, typename CTYPE>
class WindowSortOnekeyKernel : public WindowSortKernel::Impl {
 public:
  WindowSortOnekeyKernel(arrow::compute::ExecContext* ctx,
                         std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                         std::shared_ptr<arrow::Schema> result_schema, bool nulls_first,
                         bool asc)
      : ctx_(ctx), nulls_first_(nulls_first), asc_(asc), result_schema_(result_schema) {
    auto indices = result_schema->GetAllFieldIndices(key_field_list[0]->name());
    key_id_ = indices[0];
    col_num_ = result_schema->num_fields();
  }

  arrow::Status Evaluate(const ArrayList& in) override {
    num_batches_++;
    cached_key_.push_back(std::dynamic_pointer_cast<ArrayType_key>(in[key_id_]));
    length_list_.push_back(in[key_id_]->length());
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<arrow::Array> in,
                               std::shared_ptr<FixedSizeBinaryArray>* out) {
    int items_total = 0;
    int nulls_total = 0;
    std::shared_ptr<arrow::UInt64Array> selected =
        std::dynamic_pointer_cast<arrow::UInt64Array>(in);
    for (int i = 0; i < selected->length(); i++) {
      uint64_t encoded = selected->GetView(i);
      uint16_t array_id = (encoded & 0xFFFF0000U) >> 16U;
      uint16_t id = encoded & 0xFFFFU;
      auto key_clip = cached_key_.at(array_id);
      if (key_clip->IsNull(id)) {
        nulls_total++;
        continue;
      }
      items_total++;
    }
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total * sizeof(ArrayItemIndex);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf = *std::move(maybe_buffer);
    ArrayItemIndex* indices_begin =
        reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total;
    int64_t indices_i = 0;
    int64_t indices_null = 0;
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    for (int i = 0; i < selected->length(); i++) {
      uint64_t encoded = selected->GetView(i);
      uint16_t array_id = (encoded & 0xFFFF0000U) >> 16U;
      uint16_t id = encoded & 0xFFFFU;
      auto key_clip = cached_key_.at(array_id);
      if (nulls_first_) {
        if (!key_clip->IsNull(id)) {
          (indices_begin + nulls_total + indices_i)->array_id = array_id;
          (indices_begin + nulls_total + indices_i)->id = id;
          indices_i++;
        } else {
          (indices_begin + indices_null)->array_id = array_id;
          (indices_begin + indices_null)->id = id;
          indices_null++;
        }
      } else {
        if (!key_clip->IsNull(id)) {
          (indices_begin + indices_i)->array_id = array_id;
          (indices_begin + indices_i)->id = id;
          indices_i++;
        } else {
          (indices_end - nulls_total + indices_null)->array_id = array_id;
          (indices_end - nulls_total + indices_null)->id = id;
          indices_null++;
        }
      }
    }
    if (asc_) {
      if (nulls_first_) {
        ska_sort(indices_begin + nulls_total, indices_begin + items_total,
                 [this](auto& x) -> decltype(auto) {
                   return cached_key_[x.array_id]->GetView(x.id);
                 });
      } else {
        ska_sort(indices_begin, indices_begin + items_total - nulls_total,
                 [this](auto& x) -> decltype(auto) {
                   return cached_key_[x.array_id]->GetView(x.id);
                 });
      }
    } else {
      auto comp = [this](const ArrayItemIndex& x, const ArrayItemIndex& y) {
        return cached_key_[x.array_id]->GetView(x.id) >
               cached_key_[y.array_id]->GetView(y.id);
      };
      if (nulls_first_) {
        std::sort(indices_begin + nulls_total, indices_begin + items_total, comp);
      } else {
        std::sort(indices_begin, indices_begin + items_total - nulls_total, comp);
      }
    }
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(
        MakeFixedSizeBinaryType(sizeof(ArrayItemIndex) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array> in,
                       std::shared_ptr<arrow::Array>* out) override {
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(in, &indices_out));
    arrow::UInt64Builder builder;
    auto* index = (ArrayItemIndex*)indices_out->value_data();
    for (int i = 0; i < indices_out->length(); i++) {
      uint64_t encoded = ((uint64_t)(index->array_id) << 16U) ^ ((uint64_t)(index->id));
      RETURN_NOT_OK(builder.Append(encoded));
      index++;
    }
    RETURN_NOT_OK(builder.Finish(out));
    return arrow::Status::OK();
  }

 private:
  using ArrayType_key = typename arrow::TypeTraits<DATATYPE>::ArrayType;
  // using ArrayType_key = arrow::UInt32Array;
  std::vector<std::shared_ptr<ArrayType_key>> cached_key_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  bool nulls_first_;
  bool asc_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t col_num_;
  int key_id_;
};

arrow::Status WindowSortKernel::Make(
    arrow::compute::ExecContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::shared_ptr<arrow::Schema> result_schema, std::shared_ptr<KernalBase>* out,
    bool nulls_first, bool asc) {
  *out = std::make_shared<WindowSortKernel>(ctx, key_field_list, result_schema,
                                            nulls_first, asc);
  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES_WINDOW_SORT(PROC)                   \
  PROC(arrow::UInt8Type, arrow::UInt8Builder, arrow::UInt8Array)    \
  PROC(arrow::Int8Type, arrow::Int8Builder, arrow::Int8Array)       \
  PROC(arrow::UInt16Type, arrow::UInt16Builder, arrow::UInt16Array) \
  PROC(arrow::Int16Type, arrow::Int16Builder, arrow::Int16Array)    \
  PROC(arrow::UInt32Type, arrow::UInt32Builder, arrow::UInt32Array) \
  PROC(arrow::Int32Type, arrow::Int32Builder, arrow::Int32Array)    \
  PROC(arrow::UInt64Type, arrow::UInt64Builder, arrow::UInt64Array) \
  PROC(arrow::Int64Type, arrow::Int64Builder, arrow::Int64Array)    \
  PROC(arrow::FloatType, arrow::FloatBuilder, arrow::FloatArray)    \
  PROC(arrow::DoubleType, arrow::DoubleBuilder, arrow::DoubleArray) \
  PROC(arrow::Decimal128Type, arrow::Decimal128Builder, arrow::Decimal128Array)

WindowSortKernel::WindowSortKernel(
    arrow::compute::ExecContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::shared_ptr<arrow::Schema> result_schema, bool nulls_first, bool asc) {
  if (key_field_list.size() == 1) {
#ifdef DEBUG
    std::cout << "UseSortOneKey" << std::endl;
#endif
    if (key_field_list[0]->type()->id() == arrow::Type::STRING) {
      impl_.reset(new WindowSortOnekeyKernel<arrow::StringType, std::string>(
          ctx, key_field_list, result_schema, nulls_first, asc));
    } else {
      switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType, BUILDER_TYPE, ARRAY_TYPE)               \
  case InType::type_id: {                                       \
    using CType = typename TypeTraits<InType>::CType;           \
    impl_.reset(new WindowSortOnekeyKernel<InType, CType>(      \
        ctx, key_field_list, result_schema, nulls_first, asc)); \
  } break;
        PROCESS_SUPPORTED_TYPES_WINDOW_SORT(PROCESS)
#undef PROCESS
        default: {
          std::cout << "WindowSortOnekeyKernel type not supported, type is "
                    << key_field_list[0]->type() << std::endl;
        } break;
      }
    }
  } else {
    impl_.reset(new Impl(ctx, key_field_list, result_schema, nulls_first, asc));
    auto status = impl_->LoadJITFunction(key_field_list, result_schema);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }
  kernel_name_ = "WindowSortKernel";
}
#undef PROCESS_SUPPORTED_TYPES_WINDOW_SORT

arrow::Status WindowSortKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

std::string WindowSortKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
