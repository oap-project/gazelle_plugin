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

#include "codegen/arrow_compute/ext/actions_impl.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/window_sort_kernel.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

class WindowAggregateFunctionKernel::ActionFactory {
 public:
  ActionFactory(std::shared_ptr<ActionBase> action) {
    action_ = action;
  }

  static arrow::Status Make(std::string action_name,
                            arrow::compute::ExecContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionFactory> *out) {
    std::shared_ptr<ActionBase> action;
    if (action_name == "sum") {
      RETURN_NOT_OK(MakeSumAction(ctx, type, {type}, &action));
    } else if (action_name == "avg") {
      RETURN_NOT_OK(MakeAvgAction(ctx, type, {type}, &action));
    } else {
      return arrow::Status::Invalid("window aggregate function: unsupported action name: " + action_name);
    }
    *out = std::make_shared<ActionFactory>(action);
    return arrow::Status::OK();
  }

  std::shared_ptr<ActionBase> Get() {
    return action_;
  }

 private:
  std::shared_ptr<ActionBase> action_;
};

arrow::Status WindowAggregateFunctionKernel::Make(arrow::compute::ExecContext *ctx,
                                                  std::string function_name,
                                                  std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                                  std::shared_ptr<arrow::DataType> result_type,
                                                  std::shared_ptr<KernalBase> *out) {
  if (type_list.size() != 1) {
    return arrow::Status::Invalid("given more than 1 input argument for window function: " + function_name);
  }
  std::shared_ptr<ActionFactory> action;

  if (function_name == "sum" || function_name == "avg") {
    RETURN_NOT_OK(ActionFactory::Make(function_name, ctx, type_list[0], &action));
  } else {
    return arrow::Status::Invalid("window function not supported: " + function_name);
  }
  auto accumulated_group_ids = std::vector<std::shared_ptr<arrow::Int32Array >>();
  *out = std::make_shared<WindowAggregateFunctionKernel>(ctx, type_list, result_type, accumulated_group_ids, action);
  return arrow::Status::OK();
}

WindowAggregateFunctionKernel::WindowAggregateFunctionKernel(arrow::compute::ExecContext *ctx,
                                                             std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                                             std::shared_ptr<arrow::DataType> result_type,
                                                             std::vector<std::shared_ptr<arrow::Int32Array>> accumulated_group_ids,
                                                             std::shared_ptr<ActionFactory> action) {
  ctx_ = ctx;
  type_list_ = type_list;
  result_type_ = result_type;
  accumulated_group_ids_ = accumulated_group_ids;
  action_ = action;
  kernel_name_ = "WindowAggregateFunctionKernel";
}

/**
 * | a | group |   | group | sum |          | result |
 * | 2 |     0 | + |     0 |   8 |   --->   |      8 |
 * | 3 |     1 |   |     1 |   3 |          |      3 |
 * | 6 |     0 |                            |      8 |
 */
arrow::Status WindowAggregateFunctionKernel::Evaluate(const ArrayList &in) {
  // abstract following code to do common inter-window processing

  int32_t max_group_id = 0;
  std::shared_ptr<arrow::Array> group_id_array = in[1];
  auto group_ids = std::dynamic_pointer_cast<arrow::Int32Array>(group_id_array);
  accumulated_group_ids_.push_back(group_ids);
  for (int i = 0; i < group_ids->length(); i++) {
    if (group_ids->IsNull(i)) {
      continue;
    }
    if (group_ids->GetView(i) > max_group_id) {
      max_group_id = group_ids->GetView(i);
    }
  }

  ArrayList action_input_data;
  action_input_data.push_back(in[0]);
  std::function<arrow::Status(int)> func;
  std::function<arrow::Status()> null_func;
  RETURN_NOT_OK(action_->Get()->Submit(action_input_data, max_group_id, &func, &null_func));

  for (int row_id = 0; row_id < group_id_array->length(); row_id++) {
    if (group_ids->IsNull(row_id)) {
      RETURN_NOT_OK(null_func());
      continue;
    }
    auto group_id = group_ids->GetView(row_id);
    RETURN_NOT_OK(func(group_id));
  }

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
  PROCESS(arrow::DoubleType)

arrow::Status WindowAggregateFunctionKernel::Finish(ArrayList *out) {
  std::shared_ptr<arrow::DataType> value_type = result_type_;
  switch (value_type->id()) {
#define PROCESS(NUMERIC_TYPE)                                                      \
  case NUMERIC_TYPE::type_id: {                                                    \
    RETURN_NOT_OK(Finish0<NUMERIC_TYPE>(out));                                     \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:return arrow::Status::Invalid("window function: unsupported input type");
  }
  return arrow::Status::OK();
}

template<typename ArrowType>
arrow::Status WindowAggregateFunctionKernel::Finish0(ArrayList *out) {
  ArrayList action_output;
  RETURN_NOT_OK(action_->Get()->Finish(&action_output));
  if (action_output.size() != 1) {
    return arrow::Status::Invalid("window function: got invalid result from corresponding action");
  }

  auto action_output_values = std::dynamic_pointer_cast<arrow::NumericArray<ArrowType >>(action_output.at(0));

  for (const auto &accumulated_group_ids_single_part : accumulated_group_ids_) {
    std::unique_ptr<arrow::NumericBuilder<ArrowType>> output_builder
        = std::make_unique<arrow::NumericBuilder<ArrowType >>(ctx_->memory_pool());

    for (int i = 0; i < accumulated_group_ids_single_part->length(); i++) {
      if (accumulated_group_ids_single_part->IsNull(i)) {
        RETURN_NOT_OK(output_builder->AppendNull());
        continue;
      }
      int32_t group_id = accumulated_group_ids_single_part->GetView(i);
      RETURN_NOT_OK(output_builder->Append(action_output_values->GetView(group_id)));
    }
    std::shared_ptr<arrow::Array> out_array;
    RETURN_NOT_OK(output_builder->Finish(&out_array));
    (*out).push_back(out_array);
  }
  return arrow::Status::OK();
}

WindowRankKernel::WindowRankKernel(arrow::compute::ExecContext *ctx,
                                   std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                   std::shared_ptr<WindowSortKernel::Impl> sorter,
                                   bool desc) {
  ctx_ = ctx;
  type_list_ = type_list;
  sorter_ = sorter;
  desc_ = desc;
}

arrow::Status WindowRankKernel::Make(arrow::compute::ExecContext *ctx,
                                     std::string function_name,
                                     std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                     std::shared_ptr<KernalBase> *out,
                                     bool desc) {
  std::vector<std::shared_ptr<arrow::Field>> key_fields;
  for (int i = 0; i < type_list.size(); i++) {
    key_fields.push_back(std::make_shared<arrow::Field>("sort_key" + std::to_string(i), type_list.at(i)));
  }
  std::shared_ptr<arrow::Schema> result_schema = std::make_shared<arrow::Schema>(key_fields);

  std::shared_ptr<WindowSortKernel::Impl> sorter;
  // fixme null ordering flag and collation flag
  bool nulls_first = false;
  bool asc = !desc;
  if (key_fields.size() == 1) {
    std::shared_ptr<arrow::Field> key_field = key_fields[0];
    if (key_field->type()->id() == arrow::Type::STRING) {
      sorter.reset(
          new WindowSortOnekeyKernel<arrow::StringType, std::string>(ctx, key_fields,
                                                                     result_schema, nulls_first, asc));
    } else {
      switch (key_field->type()->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using CType = typename arrow::TypeTraits<InType>::CType;                  \
    sorter.reset(new WindowSortOnekeyKernel<InType, CType>(ctx, key_fields, result_schema, nulls_first, asc));  \
  } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
        default: {
          std::cout << "WindowSortOnekeyKernel type not supported, type is "
                    << key_field->type() << std::endl;
        }
          break;
      }
    }
  } else {
    sorter.reset(new WindowSortKernel::Impl(ctx, key_fields, result_schema, nulls_first, asc));
    auto status = sorter->LoadJITFunction(key_fields, result_schema);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }
  *out = std::make_shared<WindowRankKernel>(ctx, type_list, sorter, desc);
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::Evaluate(const ArrayList &in) {
  input_cache_.push_back(in);
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::Finish(ArrayList *out) {
  std::vector<ArrayList> values;
  std::vector<std::shared_ptr<arrow::Int32Array>> group_ids;

#ifdef DEBUG
  std::cout << "[window kernel] Entering Rank Kernel's finish method... " << std::endl;
#endif
#ifdef DEBUG
  std::cout << "[window kernel] Splitting all input batches to key/value batches... " << std::endl;
#endif
  for (auto batch : input_cache_) {
    ArrayList values_batch;
    for (int i = 0; i < type_list_.size() + 1; i++) {
      auto column_slice = batch.at(i);
      if (i == type_list_.size()) {
        // we are at the column of partition ids
        group_ids.push_back(std::dynamic_pointer_cast<arrow::Int32Array>(column_slice));
        continue;
      }
      values_batch.push_back(column_slice);
    }
    values.push_back(values_batch);
  }
#ifdef DEBUG
  std::cout << "[window kernel] Finished. " << std::endl;
#endif

#ifdef DEBUG
  std::cout << "[window kernel] Calculating max group ID... " << std::endl;
#endif
  int32_t max_group_id = 0;
  for (int i = 0; i < group_ids.size(); i++) {
    auto slice = group_ids.at(i);
    for (int j = 0; j < slice->length(); j++) {
      if (slice->IsNull(j)) {
        continue;
      }
      if (slice->GetView(j) > max_group_id) {
        max_group_id = slice->GetView(j);
      }
    }
  }
#ifdef DEBUG
  std::cout << "[window kernel] Finished. " << std::endl;
#endif

  // initialize partitions to be sorted
  std::vector<std::vector<std::shared_ptr<ArrayItemIndex>>> partitions_to_sort;
  for (int i = 0; i <= max_group_id; i++) {
    partitions_to_sort.emplace_back();
  }

#ifdef DEBUG
  std::cout << "[window kernel] Creating indexed array based on group IDs... " << std::endl;
#endif
  for (int i = 0; i < group_ids.size(); i++) {
    auto slice = group_ids.at(i);
    for (int j = 0; j < slice->length(); j++) {
      if (slice->IsNull(j)) {
        continue;
      }
      uint64_t partition_id = slice->GetView(j);
      partitions_to_sort.at(partition_id).push_back(std::make_shared<ArrayItemIndex>(i, j));
    }
  }
#ifdef DEBUG
  std::cout << "[window kernel] Finished. " << std::endl;
#endif

  std::vector<std::vector<std::shared_ptr<ArrayItemIndex>>> sorted_partitions;
  RETURN_NOT_OK(SortToIndicesPrepare(values));
  for (int i = 0; i <= max_group_id; i++) {
    std::vector<std::shared_ptr<ArrayItemIndex>> partition = partitions_to_sort.at(i);
    std::vector<std::shared_ptr<ArrayItemIndex>> sorted_partition;
#ifdef DEBUG
    std::cout << "[window kernel] Sorting a single partition... " << std::endl;
#endif
    RETURN_NOT_OK(SortToIndicesFinish(partition, &sorted_partition));
#ifdef DEBUG
    std::cout << "[window kernel] Finished. " << std::endl;
#endif
    sorted_partitions.push_back(std::move(sorted_partition));
  }
  int32_t **rank_array = new int32_t*[group_ids.size()];
  for (int i = 0; i < group_ids.size(); i++) {
    *(rank_array + i) = new int32_t[group_ids.at(i)->length()];
  }
  for (int i = 0; i <= max_group_id; i++) {
#ifdef DEBUG
    std::cout << "[window kernel] Generating rank result on a single partition... " << std::endl;
#endif
    std::vector<std::shared_ptr<ArrayItemIndex>> sorted_partition = sorted_partitions.at(i);
    int assumed_rank = 0;
    for (int j = 0; j < sorted_partition.size(); j++) {
      ++assumed_rank; // rank value starts from 1
      std::shared_ptr<ArrayItemIndex> index = sorted_partition.at(j);
      if (j == 0) {
        rank_array[index->array_id][index->id] = 1; // rank value starts from 1
        continue;
      }
      std::shared_ptr<ArrayItemIndex> last_index = sorted_partition.at(j - 1);
      bool same = true;
      for (int column_id = 0; column_id < type_list_.size(); column_id++) {
        bool s;
        std::shared_ptr<arrow::DataType> type = type_list_.at(column_id);
        switch (type->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;                  \
      RETURN_NOT_OK(AreTheSameValue<ArrayType>(values, column_id, index, last_index, &s));  \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "WindowRankKernel: type not supported: "
                      << type->ToString() << std::endl; // todo use arrow::Status
          }
            break;
        }
        if (!s) {
          same = false;
          break;
        }
      }
      if (same) {
        rank_array[index->array_id][index->id] = rank_array[last_index->array_id][last_index->id];
        continue;
      }
      rank_array[index->array_id][index->id] = assumed_rank;
    }
#ifdef DEBUG
    std::cout << "[window kernel] Finished. " << std::endl;
#endif
  }

#ifdef DEBUG
  std::cout << "[window kernel] Building overall associated rank results... " << std::endl;
#endif
  for (int i = 0; i < input_cache_.size(); i++) {
    auto batch = input_cache_.at(i);
    auto group_id_column_slice = batch.at(type_list_.size());
    int slice_length = group_id_column_slice->length();
    std::shared_ptr<arrow::Int32Builder> rank_builder = std::make_shared<arrow::Int32Builder>(ctx_->memory_pool());
    for (int j = 0; j < slice_length; j++) {
      RETURN_NOT_OK(rank_builder->Append(rank_array[i][j]));
    }
    std::shared_ptr<arrow::Int32Array> rank_slice;
    RETURN_NOT_OK(rank_builder->Finish(&rank_slice));
    out->push_back(rank_slice);
  }
#ifdef DEBUG
  std::cout << "[window kernel] Finished. " << std::endl;
#endif
  for (int i = 0; i < group_ids.size(); i++) {
    delete[] *(rank_array + i);
  }
  delete[] rank_array;
  return arrow::Status::OK();
}

static arrow::Status EncodeIndices( std::vector<std::shared_ptr<ArrayItemIndex>> in, std::shared_ptr<arrow::Array> *out){
  arrow::UInt64Builder builder;
  for (const auto& each : in) {
    uint64_t encoded = ((uint64_t) (each->array_id) << 16U) ^ ((uint64_t) (each->id));
    RETURN_NOT_OK(builder.Append(encoded));
  }
  RETURN_NOT_OK(builder.Finish(out));
  return arrow::Status::OK();
}

static arrow::Status DecodeIndices(std::shared_ptr<arrow::Array> in, std::vector<std::shared_ptr<ArrayItemIndex>> *out){
  std::vector<std::shared_ptr<ArrayItemIndex>> v;
  std::shared_ptr<arrow::UInt64Array> selected = std::dynamic_pointer_cast<arrow::UInt64Array>(in);
  for (int i = 0; i < selected->length(); i++) {
    uint64_t encoded = selected->GetView(i);
    uint16_t array_id = (encoded & 0xFFFF0000U) >> 16U;
    uint16_t id = encoded & 0xFFFFU;
    v.push_back(std::make_shared<ArrayItemIndex>(array_id, id));
  }
  *out = v;
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::SortToIndicesPrepare(std::vector<ArrayList> values) {
  for (auto each_batch : values) {
    RETURN_NOT_OK(sorter_->Evaluate(each_batch));
  }
  return arrow::Status::OK();
  // todo sort algorithm
}

arrow::Status WindowRankKernel::SortToIndicesFinish(std::vector<std::shared_ptr<ArrayItemIndex>> elements_to_sort,
                                                    std::vector<std::shared_ptr<ArrayItemIndex>> *offsets) {
  std::shared_ptr<arrow::Array> in;
  std::shared_ptr<arrow::Array> out;
  RETURN_NOT_OK(EncodeIndices(elements_to_sort, &in));
  RETURN_NOT_OK(sorter_->Finish(in, &out));
  std::vector<std::shared_ptr<ArrayItemIndex>> decoded_out;
  RETURN_NOT_OK(DecodeIndices(out, &decoded_out));
  *offsets = decoded_out;
  return arrow::Status::OK();
  // todo sort algorithm
}

template<typename ArrayType>
arrow::Status WindowRankKernel::AreTheSameValue(const std::vector<ArrayList>& values, int column, std::shared_ptr<ArrayItemIndex> i, std::shared_ptr<ArrayItemIndex> j, bool* out) {
  auto typed_array_i = std::dynamic_pointer_cast<ArrayType>(values.at(i->array_id).at(column));
  auto typed_array_j = std::dynamic_pointer_cast<ArrayType>(values.at(j->array_id).at(column));
  *out = (typed_array_i->GetView(i->id) == typed_array_j->GetView(j->id));
  return arrow::Status::OK();
}

}
}
}
}