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
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/checked_cast.h>
#include <math.h>

#include <iostream>
#include <limits>
#include <memory>
#include <sstream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

class ActionBase {
 public:
  virtual ~ActionBase() {}

  virtual arrow::Status Submit(ArrayList in, int max_group_id,
                               std::function<arrow::Status(int)>* on_valid,
                               std::function<arrow::Status()>* on_null);
  virtual arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                               std::function<arrow::Status(uint64_t, uint64_t)>* on_valid,
                               std::function<arrow::Status()>* on_null);
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::stringstream* ss,
                               std::function<arrow::Status(int)>* out);
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::function<arrow::Status(uint32_t)>* on_valid,
                               std::function<arrow::Status()>* on_null);
  virtual arrow::Status EvaluateCountLiteral(const int& len);
  virtual arrow::Status Evaluate(const arrow::ArrayVector& in);
  virtual arrow::Status Evaluate(int dest_group_id);
  virtual arrow::Status Evaluate(int dest_group_id, void* data);
  virtual arrow::Status Evaluate(int dest_group_id, void* data1, void* data2);
  virtual arrow::Status Evaluate(int dest_group_id, void* data1, void* data2,
                                 void* data3);
  virtual arrow::Status EvaluateNull(int dest_group_id);
  virtual arrow::Status Finish(ArrayList* out);
  virtual arrow::Status Finish(uint64_t offset, uint64_t length, ArrayList* out);
  virtual arrow::Status FinishAndReset(ArrayList* out);
  virtual uint64_t GetResultLength();
};

arrow::Status MakeUniqueAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeCountAction(arrow::compute::ExecContext* ctx,
                              std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                              std::shared_ptr<ActionBase>* out);

arrow::Status MakeCountLiteralAction(
    arrow::compute::ExecContext* ctx, int arg,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeSumAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out);

arrow::Status MakeSumActionPartial(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeAvgAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out);

arrow::Status MakeMinAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out);

arrow::Status MakeMaxAction(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
                            std::shared_ptr<ActionBase>* out);

arrow::Status MakeSumCountAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeSumCountMergeAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeAvgByCountAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeStddevSampPartialAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);

arrow::Status MakeStddevSampFinalAction(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::DataType> type,
    std::vector<std::shared_ptr<arrow::DataType>> res_type_list,
    std::shared_ptr<ActionBase>* out);
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
