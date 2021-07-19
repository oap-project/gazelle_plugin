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
#include "codegen/common/result_iterator.h"
#include "precompile/array.h"

class SortRelation;
class GandivaProjector;

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class CodeGenBase {
 public:
  virtual arrow::Status Evaluate(ArrayList& in) {
    return arrow::Status::NotImplemented(
        "CodeGenBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented(
        "CodeGenBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Evaluate(const ArrayList& in, const ArrayList& projected_batch) {
    return arrow::Status::NotImplemented(
        "CodeGenBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("CodeGenBase Finish is an abstract interface.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array> in,
                               std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("CodeGenBase Finish is an abstract interface.");
  }

  virtual arrow::Status FinishInternal(
      std::shared_ptr<precompile::FixedSizeBinaryArray>* out) {
    return arrow::Status::NotImplemented(
        "CodeGenBase FinishInternal is an abstract interface.");
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "CodeGenBase MakeResultIterator is an abstract interface.");
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::vector<std::shared_ptr<GandivaProjector>> gandiva_projector_list,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "CodeGenBase MakeResultIterator is an abstract interface.");
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<SortRelation>>* out) {
    return arrow::Status::NotImplemented(
        "CodeGenBase MakeResultIterator is an abstract interface.");
  }
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
