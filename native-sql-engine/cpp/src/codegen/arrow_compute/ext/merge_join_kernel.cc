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
#include <gandiva/tree_expr_builder.h>
#include <gandiva/projector.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>

#include "array_appender.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "codegen/arrow_compute/ext/codegen_register.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////Operator  SMJ  ////////////////
class ConditionedJoinKernel::Impl {
 public:
  Impl() {}
  Impl(arrow::compute::ExecContext* ctx,
       const gandiva::NodeVector& left_key_node_list,
       const gandiva::NodeVector& right_key_node_list,
       const gandiva::NodeVector& left_schema_node_list,
       const gandiva::NodeVector& right_schema_node_list,
       const gandiva::NodePtr& condition, int join_type,
       const gandiva::NodeVector& result_node_list,
       const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx): ctx_(ctx),
        join_type_(join_type), condition_(condition) {
       }
  virtual ~Impl() {}
  virtual arrow::Status Evaluate( ArrayList& in) {
    return arrow::Status::OK();
  }

  std::string GetSignature() { return ""; }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }

   arrow::Status DoCodeGen(
      int level,
      std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
          input,
      std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
    return arrow::Status::OK();
  }

 private:
  arrow::compute::ExecContext* ctx_;
  arrow::MemoryPool* pool_;
  std::string signature_;
  int join_type_;
  gandiva::NodePtr condition_;

};

arrow::Status ConditionedJoinKernel::Make(
    arrow::compute::ExecContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedJoinKernel>(
      ctx, left_key_list, right_key_list, left_schema_list, right_schema_list, condition,
      join_type, result_schema, hash_configuration_list, hash_relation_idx);
  return arrow::Status::OK();
}

//TODO: implement multiple keys
template<typename... ArrowType>
class TypedJoinKernel : public ConditionedJoinKernel::Impl {
 public:
  TypedJoinKernel(arrow::compute::ExecContext* ctx,
                  const gandiva::NodeVector& left_key_node_list,
                  const gandiva::NodeVector& right_key_node_list,
                  const gandiva::NodeVector& left_schema_node_list,
                  const gandiva::NodeVector& right_schema_node_list,
                  const gandiva::NodePtr& condition, int join_type,
                  const gandiva::NodeVector& result_node_list,
                  const gandiva::NodeVector& hash_configuration_list,
                  int hash_relation_idx)
      : ctx_(ctx), join_type_(join_type), condition_(condition) {
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
    result_schema = std::make_shared<arrow::Schema>(result_schema_);
    //TODO(): fix with multiple keys
    if (join_type == 4) {
      exist_index_ = 1;
    }
  }

  arrow::Status Evaluate( ArrayList& in) override {
    col_num_ = left_field_list_.size();

    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    //TODO: fix key col
    auto typed_array_0 = std::dynamic_pointer_cast<ArrayType>(in[0]);
    left_list_.emplace_back(typed_array_0);
    idx_to_arrarid_.push_back(typed_array_0->length());
    return arrow::Status::OK();
  }

  std::string GetSignature() { return ""; }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    *out = std::make_shared<ProberResultIterator>(
        ctx_, schema, join_type_, &left_list_, &idx_to_arrarid_, cached_, exist_index_);

    return arrow::Status::OK();
  }

  arrow::Status DoCodeGen(
      int level,
      std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
          input,
      std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<typename std::tuple_element<0, std::tuple<ArrowType...> >::type>::ArrayType;
  using ArrayType1 = typename arrow::TypeTraits<typename std::tuple_element<1, std::tuple<ArrowType...> >::type>::ArrayType;
  arrow::compute::ExecContext* ctx_;
  arrow::MemoryPool* pool_;
  std::string signature_;
  int join_type_;
  gandiva::NodePtr condition_;
  gandiva::FieldVector left_field_list_;
  gandiva::FieldVector right_field_list_;
  gandiva::FieldVector result_schema_;
  std::vector<int> right_key_index_list_;
  std::vector<int> left_shuffle_index_list_;
  std::vector<int> right_shuffle_index_list_;
  std::vector<std::shared_ptr<ArrayType>> left_list_;
  std::vector<int64_t> idx_to_arrarid_;
  int col_num_;
  std::vector<arrow::ArrayVector> cached_;
  std::shared_ptr<arrow::Schema> result_schema;
  int exist_index_ = -1;

  class FVector {
    using ViewType = decltype(std::declval<ArrayType>().GetView(0));

   public:
    FVector(std::vector<std::shared_ptr<ArrayType>>* left_list,
            std::vector<int64_t> segment_info) {
      left_list_ = left_list;
      total_len_ = std::accumulate(segment_info.begin(), segment_info.end(), 0);
      segment_info_ = segment_info;
      it = (*left_list_)[0];
      segment_len = 0;
    }

    void getpos(int64_t* cur_idx, int64_t* cur_segmeng_len, int64_t* passlen) {
      *cur_idx = idx;
      *cur_segmeng_len = segment_len;
      *passlen = passed_len;
    }

    void setpos(int64_t cur_idx, int64_t cur_segment_len, int64_t cur_passed_len) {
      idx = cur_idx;
      it = (*left_list_)[idx];
      segment_len = cur_segment_len;
      passed_len = cur_passed_len;
    }

    ViewType value() { return it->GetView(segment_len); }

    bool hasnext() { return (passed_len <= total_len_ - 1); }

    void next() {
      segment_len++;
      if (segment_len == segment_info_[idx] && passed_len != total_len_ - 1) {
        idx++;
        segment_len = 0;
        it = (*left_list_)[idx];
      }
      passed_len++;
    }
    ArrayItemIndex GetArrayItemIdex() {
      return ArrayItemIndex(idx, segment_len);
    }

   public:
    std::vector<std::shared_ptr<ArrayType>>* left_list_;
    int64_t total_len_;
    std::vector<int64_t> segment_info_;

    int64_t idx = 0;
    int64_t passed_len = 0;
    int64_t segment_len = 0;
    std::shared_ptr<ArrayType> it;
  };
  class ProberResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ProberResultIterator(arrow::compute::ExecContext* ctx,
                         std::shared_ptr<arrow::Schema> schema,
                         int join_type,
                         std::vector<std::shared_ptr<ArrayType>>* left_list,
                         std::vector<int64_t>* idx_to_arrarid,
                         std::vector<arrow::ArrayVector> cached_in,
                         int exist_index)
        : ctx_(ctx),
          result_schema_(schema),
          join_type_(join_type),
          left_list_(left_list),
          last_pos(0),
          idx_to_arrarid_(idx_to_arrarid),
          cached_in_(cached_in), exist_index_(exist_index) {

      int result_col_num = schema->num_fields();

      left_result_col_num = cached_in.size();

      if (join_type_ == 2 || join_type_ == 3 || join_type_ == 4) {
        // for semi/anti/existence only return right batch
        left_result_col_num = 0;
      }

      for (int i = 0; i < result_col_num; i++) {
        auto appender_type = AppenderBase::left;
        auto field = schema->field(i);
        auto type = field->type();
        if (left_result_col_num == 0 || i >= left_result_col_num) {
          appender_type = AppenderBase::right;
        }
        if (i == exist_index_) {
          appender_type = AppenderBase::exist;
          type = arrow::boolean();
        }

        std::shared_ptr<AppenderBase> appender;
        MakeAppender(ctx_, type, appender_type, &appender);
        appender_list_.push_back(appender);
      }

      for (int i = 0; i < left_result_col_num; i++) {
        arrow::ArrayVector array_vector = cached_in_[i];
        int array_num = array_vector.size();
        for (int array_id = 0; array_id < array_num; array_id++) {
          auto arr = array_vector[array_id];
          appender_list_[i]->AddArray(arr);
        }
      }
      left_it = std::make_shared<FVector>(left_list_, *idx_to_arrarid_);

      switch (join_type_) {
          case 0: { /*Inner Join*/
            auto func = std::make_shared<InnerProbeFunction>(left_it, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 1: { /*Outer Join*/
            auto func = std::make_shared<OuterProbeFunction>(left_it, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 2: { /*Anti Join*/
            auto func =
                std::make_shared<AntiProbeFunction>(left_it, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 3: { /*Semi Join*/
            auto func =
                std::make_shared<SemiProbeFunction>(left_it, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          case 4: { /*Existence Join*/
            auto func = std::make_shared<ExistenceProbeFunction>(left_it, appender_list_);
            probe_func_ = std::dynamic_pointer_cast<ProbeFunctionBase>(func);
          } break;
          default:
            throw;
        }
    }

    arrow::Status Process(const ArrayList& in, std::shared_ptr<arrow::RecordBatch>* out,
                          const std::shared_ptr<arrow::Array>& selection) override {

      std::shared_ptr<arrow::Array> key_array = in[0];
      arrow::ArrayVector projected_keys_outputs;

      //update right appender
      if (join_type_ == 4) {
        // do not touch the middle existence column
        for (int i = 0; i < appender_list_.size(); i++) {
          if (i == exist_index_) {
            continue;
          }
          if (i > exist_index_) {
            appender_list_[i]->AddArray(in[i-1]);
          } else {
            appender_list_[i]->AddArray(in[i]);
          }
        }
      } else {
        for (int i = 0; i < in.size(); i++) {
          appender_list_[i + left_result_col_num]->AddArray(in[i]);
        }
      }

      uint64_t out_length = 0;
      out_length = probe_func_->Evaluate(key_array);

      ArrayList arrays;
      for (auto& appender : appender_list_) {
        std::shared_ptr<arrow::Array> out_array;
        RETURN_NOT_OK(appender->Finish(&out_array));
        arrays.push_back(out_array);
        if (appender->GetType() == AppenderBase::right) {
          RETURN_NOT_OK(appender->PopArray());
        }
        appender->Reset();
      }

      *out = arrow::RecordBatch::Make(result_schema_, out_length, arrays);
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
    class InnerProbeFunction : public ProbeFunctionBase {
      public:
        InnerProbeFunction(std::shared_ptr<FVector> left_it,
                           std::vector<std::shared_ptr<AppenderBase>> appender_list):
        left_it(left_it), appender_list_(appender_list) {}

        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          auto right_content = typed_key_array->GetView(i);
          while (left_it->hasnext() && left_it->value() < right_content) {
            left_it->next();
          }
          int64_t cur_idx, seg_len, pl; left_it->getpos(&cur_idx, &seg_len, &pl);
          while (left_it->hasnext() && left_it->value() == right_content) {
            auto item = left_it->GetArrayItemIdex();
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                appender->Append(item.array_id, item.id);
              } else {
                appender->Append(0, i);
              }
            }
            out_length += 1;
            left_it->next();
          }
          left_it->setpos(cur_idx, seg_len, pl);
        }
        return out_length;
      }

      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

    class OuterProbeFunction : public ProbeFunctionBase {
      public:
        OuterProbeFunction(std::shared_ptr<FVector> left_it, std::vector<std::shared_ptr<AppenderBase>> appender_list): left_it(left_it), appender_list_(appender_list){
        }
        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        int last_match_idx = -1;
        for (int i = 0; i < key_array->length(); i++) {
          auto right_content = typed_key_array->GetView(i);
          while (left_it->hasnext() && left_it->value() < right_content) {
            left_it->next();
          }
          int64_t cur_idx, seg_len, pl; left_it->getpos(&cur_idx, &seg_len, &pl);
          while (left_it->hasnext() && left_it->value() == right_content) {
            auto item = left_it->GetArrayItemIdex();
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                appender->Append(item.array_id, item.id);
              } else {
                appender->Append(0, i);
              }
            }
            last_match_idx = i;
            out_length += 1;
            left_it->next();
          }
          left_it->setpos(cur_idx, seg_len, pl);
          if(left_it->value() > right_content && left_it->hasnext() ) {
            if (last_match_idx == i) {
              continue;
            }
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                appender->AppendNull();
              } else {
                appender->Append(0, i);
              }
            }
            out_length += 1;
          }
          if (!left_it->hasnext()) {
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::left) {
                appender->AppendNull();
              } else {
                appender->Append(0, i);
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }
      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

    //TODO(): implement full outer
    class FullOuterProbeFunction : public ProbeFunctionBase {
      public:
        FullOuterProbeFunction(std::shared_ptr<FVector> left_it, std::vector<std::shared_ptr<AppenderBase>> appender_list): left_it(left_it), appender_list_(appender_list){
        }
        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {

        }
        return out_length;
      }
      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

    class SemiProbeFunction : public ProbeFunctionBase {
      public:
        SemiProbeFunction(std::shared_ptr<FVector> left_it, std::vector<std::shared_ptr<AppenderBase>> appender_list):left_it(left_it), appender_list_(appender_list){
        }
        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        for (int i = 0; i < key_array->length(); i++) {
          auto right_content = typed_key_array->GetView(i);
          while (left_it->hasnext() && left_it->value() < right_content) {
            left_it->next();
          }
          int64_t cur_idx, seg_len, pl; left_it->getpos(&cur_idx, &seg_len, &pl);
          if (left_it->value() == right_content && left_it->hasnext()) {
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              }
            }
            out_length += 1;
            left_it->next();
          }
          left_it->setpos(cur_idx, seg_len, pl);
        }
        return out_length;
      }
      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

    class AntiProbeFunction : public ProbeFunctionBase {
      public:
        AntiProbeFunction(std::shared_ptr<FVector> left_it, std::vector<std::shared_ptr<AppenderBase>> appender_list):left_it(left_it), appender_list_(appender_list){
        }
        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        int last_match_idx = -1;
        for (int i = 0; i < key_array->length(); i++) {
          auto right_content = typed_key_array->GetView(i);
          while (left_it->hasnext() && left_it->value() < right_content) {
            left_it->next();
          }
          int64_t cur_idx, seg_len, pl; left_it->getpos(&cur_idx, &seg_len, &pl);
          while (left_it->hasnext() && left_it->value() == right_content) {
            last_match_idx = i;
            left_it->next();
          }
          left_it->setpos(cur_idx, seg_len, pl);
          if (left_it->value() > right_content && left_it->hasnext()) {
            if (last_match_idx == i) {
              continue;
            }
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              }
            }
            out_length += 1;
          }
          if (!left_it->hasnext()) {
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }
      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

    class ExistenceProbeFunction : public ProbeFunctionBase {
      public:
        ExistenceProbeFunction(std::shared_ptr<FVector> left_it, std::vector<std::shared_ptr<AppenderBase>> appender_list): left_it(left_it), appender_list_(appender_list){
        }
        uint64_t Evaluate(std::shared_ptr<arrow::Array> key_array) override {
        auto typed_key_array = std::dynamic_pointer_cast<ArrayType>(key_array);

        uint64_t out_length = 0;
        int last_match_idx = -1;
        for (int i = 0; i < key_array->length(); i++) {
          auto right_content = typed_key_array->GetView(i);
          while (left_it->hasnext() && left_it->value() < right_content) {
            left_it->next();
          }
          int64_t cur_idx, seg_len, pl; left_it->getpos(&cur_idx, &seg_len, &pl);
          if (left_it->hasnext() && left_it->value() == right_content) {
                        for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              } else { // should be existence column
                appender->AppendExistence(true);
              }
            }
            out_length += 1;
            last_match_idx = i;
            while (left_it->hasnext() && left_it->value() == right_content) {
               left_it->next();
            }
          }
          left_it->setpos(cur_idx, seg_len, pl);
          if (left_it->value() > right_content && left_it->hasnext()) {
            if (last_match_idx == i) {
              continue;
            }
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              } else { // should be existence column
                appender->AppendExistence(false);
              }
            }
            out_length += 1;
          }
          if (!left_it->hasnext()) {
            for (auto& appender : appender_list_) {
              if (appender->GetType() == AppenderBase::right) {
                appender->Append(0, i);
              } else { // should be existence column
                appender->AppendExistence(false);
              }
            }
            out_length += 1;
          }
        }
        return out_length;
      }
      private:
        std::vector<std::shared_ptr<AppenderBase>> appender_list_;
        std::shared_ptr<FVector> left_it;
    };

   private:
    arrow::compute::ExecContext* ctx_;
    std::shared_ptr<arrow::Schema> result_schema_;
    std::vector<std::shared_ptr<ArrayType>>* left_list_;
    std::shared_ptr<FVector> left_it;
    int64_t last_pos;
    int64_t last_idx = 0;
    int64_t last_seg = 0;
    int64_t last_pl = 0;
    int left_result_col_num = 0;
    int join_type_;
    int exist_index_;
    std::vector<int64_t>* idx_to_arrarid_;
    std::vector<arrow::ArrayVector> cached_in_;
    std::vector<std::shared_ptr<AppenderBase>> appender_list_;
    std::shared_ptr<ProbeFunctionBase> probe_func_;
  };
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
  PROCESS(arrow::Date64Type)
ConditionedJoinKernel::ConditionedJoinKernel(
    arrow::compute::ExecContext* ctx, const gandiva::NodeVector& left_key_list,
    const gandiva::NodeVector& right_key_list,
    const gandiva::NodeVector& left_schema_list,
    const gandiva::NodeVector& right_schema_list, const gandiva::NodePtr& condition,
    int join_type, const gandiva::NodeVector& result_schema,
    const gandiva::NodeVector& hash_configuration_list, int hash_relation_idx) {
  if (left_key_list.size() == 1 && right_key_list.size() == 1) {
    auto key_node = left_key_list[0];
    std::shared_ptr<TypedNodeVisitor> node_visitor;
    std::shared_ptr<gandiva::FieldNode> field_node;
    THROW_NOT_OK(MakeTypedNodeVisitor(key_node, &node_visitor));
    if (node_visitor->GetResultType() == TypedNodeVisitor::FieldNode) {
        node_visitor->GetTypedNode(&field_node);
    }
    switch (field_node->field()->type()->id()) {
#define PROCESS(InType)                                                          \
  case InType::type_id: {                                                        \
    impl_.reset(new TypedJoinKernel<InType, InType>(                             \
        ctx, left_key_list, right_key_list, left_schema_list, right_schema_list, \
        condition, join_type, result_schema, hash_configuration_list,            \
        hash_relation_idx));                                                     \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "TypedjoinKernel type not supported, type is "
                  << field_node->field()->type() << std::endl;
      } break;
    }
    //using type = decltype(field_node->field()->type()->id());
    //constexpr auto type = field_node->field()->type()->id();
    //using keytype = precompile::TypeIdTraits<type>::Type;
    //using keytype = precompile::TypeIdTraits<arrow::Type::UINT32>::Type;
    //using keytype = typename precompile::TypeTraits<InType>::ArrayType; 
    //impl_.reset(new TypedJoinKernel<arrow::UInt32Type>(
      // impl_.reset(new TypedJoinKernel<keytype>(
      //   ctx, left_key_list, right_key_list, left_schema_list,
      //                  right_schema_list, condition, join_type, result_schema,
      //                  hash_configuration_list, hash_relation_idx));
  } else {
  impl_.reset(new Impl(ctx, left_key_list, right_key_list, left_schema_list,
                       right_schema_list, condition, join_type, result_schema,
                       hash_configuration_list, hash_relation_idx));
  }
  kernel_name_ = "ConditionedJoinKernel";
}

arrow::Status ConditionedJoinKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

std::string ConditionedJoinKernel::GetSignature() { return impl_->GetSignature(); }

arrow::Status ConditionedJoinKernel::DoCodeGen(
    int level,
    std::vector<std::pair<std::pair<std::string, std::string>, gandiva::DataTypePtr>>
        input,
    std::shared_ptr<CodeGenContext>* codegen_ctx_out, int* var_id) {
  return impl_->DoCodeGen(level, input, codegen_ctx_out, var_id);
}

arrow::Status ConditionedJoinKernel::Evaluate( ArrayList& in) {
  return impl_->Evaluate(in);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
