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
                            arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionFactory> *out) {
    std::shared_ptr<ActionBase> action;
    if (action_name == "sum") {
      RETURN_NOT_OK(MakeSumAction(ctx, type, &action));
    } else if (action_name == "avg") {
      RETURN_NOT_OK(MakeAvgAction(ctx, type, &action));
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

arrow::Status WindowAggregateFunctionKernel::Make(arrow::compute::FunctionContext *ctx,
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

WindowAggregateFunctionKernel::WindowAggregateFunctionKernel(arrow::compute::FunctionContext *ctx,
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

WindowRankKernel::WindowRankKernel(arrow::compute::FunctionContext *ctx,
                                   std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                   std::shared_ptr<WindowSortKernel::Impl> sorter,
                                   bool desc) {
  ctx_ = ctx;
  type_list_ = type_list;
  sorter_ = sorter;
  desc_ = desc;
}

arrow::Status WindowRankKernel::Make(arrow::compute::FunctionContext *ctx,
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
  ArrayList values_concatenated;
  std::shared_ptr<arrow::Int32Array> group_ids_concatenated;
  for (int i = 0; i < type_list_.size() + 1; i++) {
    ArrayList column_builder;
    for (auto batch : input_cache_) {
      auto column_slice = batch.at(i);
      column_builder.push_back(column_slice);
    }
    std::shared_ptr<arrow::Array> column;
    RETURN_NOT_OK(arrow::Concatenate(column_builder, ctx_->memory_pool(), &column));
    if (i == type_list_.size()) {
      // we are at the column of partition ids
      group_ids_concatenated = std::dynamic_pointer_cast<arrow::Int32Array>(column);
      continue;
    }
    values_concatenated.push_back(column);
  }
  int32_t max_group_id = 0;
  for (int i = 0; i < group_ids_concatenated->length(); i++) {
    if (group_ids_concatenated->IsNull(i)) {
      continue;
    }
    if (group_ids_concatenated->GetView(i) > max_group_id) {
      max_group_id = group_ids_concatenated->GetView(i);
    }
  }
  // initialize partitions to be sorted
  std::vector<std::shared_ptr<arrow::UInt64Builder>> partitions_to_sort;
  for (int i = 0; i <= max_group_id; i++) {
    partitions_to_sort.push_back(std::make_shared<arrow::UInt64Builder>(ctx_->memory_pool()));
  }

  for (int i = 0; i < group_ids_concatenated->length(); i++) {
    if (group_ids_concatenated->IsNull(i)) {
      continue;
    }
    uint64_t partition_id = group_ids_concatenated->GetView(i);
    RETURN_NOT_OK(partitions_to_sort.at(partition_id)->Append(i));
  }

  std::vector<std::shared_ptr<arrow::UInt64Array>> sorted_partitions;
  RETURN_NOT_OK(SortToIndicesPrepare(values_concatenated));
  for (int i = 0; i <= max_group_id; i++) {
    std::shared_ptr<arrow::UInt64Array> partition;
    RETURN_NOT_OK(partitions_to_sort.at(i)->Finish(&partition));
    std::shared_ptr<arrow::UInt64Array> sorted_partition;
    RETURN_NOT_OK(SortToIndicesFinish(partition, &sorted_partition));
    sorted_partitions.push_back(sorted_partition);
  }
  int64_t length = group_ids_concatenated->length();
  int32_t *rank_array = new int32_t[length];
  for (int i = 0; i <= max_group_id; i++) {
    std::shared_ptr<arrow::UInt64Array> sorted_partition = sorted_partitions.at(i);
    int assumed_rank = 0;
    for (int j = 0; j < sorted_partition->length(); j++) {
      ++assumed_rank; // rank value starts from 1
      uint64_t index = sorted_partition->GetView(j);
      if (j == 0) {
        rank_array[index] = 1; // rank value starts from 1
        continue;
      }
      uint64_t last_index = sorted_partition->GetView(j - 1);
      bool same = true;
      for (int i = 0; i < type_list_.size(); i++) {
        bool s;
        std::shared_ptr<arrow::DataType> type = type_list_.at(i);
        switch (type->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;                  \
      RETURN_NOT_OK(AreTheSameValue<ArrayType>(values_concatenated.at(i), index, last_index, &s));  \
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
        rank_array[index] = rank_array[last_index];
        continue;
      }
      rank_array[index] = assumed_rank;
    }
  }
  int offset_in_rank_array = 0;
  for (auto batch : input_cache_) {
    auto group_id_column_slice = batch.at(type_list_.size());
    int slice_length = group_id_column_slice->length();
    std::shared_ptr<arrow::Int32Builder> rank_builder = std::make_shared<arrow::Int32Builder>(ctx_->memory_pool());
    for (int i = 0; i < slice_length; i++) {
      RETURN_NOT_OK(rank_builder->Append(rank_array[offset_in_rank_array++]));
    }
    std::shared_ptr<arrow::Int32Array> rank_slice;
    RETURN_NOT_OK(rank_builder->Finish(&rank_slice));
    out->push_back(rank_slice);
  }
  // make sure offset hits bound
  if (offset_in_rank_array != length) {
    return arrow::Status::Invalid("window: fatal error, this should not happen");
  }
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::SortToIndicesPrepare(ArrayList values) {
#ifdef DEBUG
  std::cout << "RANK: values to sort: " << values.at(0)->ToString() << std::endl;
#endif
  RETURN_NOT_OK(sorter_->Evaluate(values));
  return arrow::Status::OK();
  // todo sort algorithm
}

arrow::Status WindowRankKernel::SortToIndicesFinish(std::shared_ptr<arrow::UInt64Array> elements_to_sort,
                                                    std::shared_ptr<arrow::UInt64Array> *offsets) {
  std::vector<std::shared_ptr<arrow::Array>> elements_to_sort_list = {elements_to_sort};
#ifdef DEBUG
  std::cout << "RANK: partition: " << elements_to_sort->ToString() << std::endl;
#endif
  std::shared_ptr<arrow::Array> out;
  RETURN_NOT_OK(sorter_->Finish(elements_to_sort_list, &out));
  *offsets = std::dynamic_pointer_cast<arrow::UInt64Array>(out);
#ifdef DEBUG
  std::cout << "RANK: partition sorted: " << out->ToString() << std::endl;
#endif
  return arrow::Status::OK();
  // todo sort algorithm
}

template<typename ArrayType>
arrow::Status WindowRankKernel::AreTheSameValue(std::shared_ptr<arrow::Array> values, int i, int j, bool *out) {
  auto typed_array = std::dynamic_pointer_cast<ArrayType>(values);
  *out = (typed_array->GetView(i) == typed_array->GetView(j));
  return arrow::Status::OK();
}

}
}
}
}