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
#include "precompile/gandiva_projector.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <gandiva/projector.h>

#include "utils/macros.h"

class GandivaProjector::Impl {
 public:
  Impl(arrow::compute::ExecContext* ctx, gandiva::SchemaPtr input_schema,
       gandiva::ExpressionVector exprs)
      : ctx_(ctx) {
    THROW_NOT_OK(Make(input_schema, exprs));
  }
  arrow::Status Make(gandiva::SchemaPtr input_schema,
                     gandiva::ExpressionVector exprs) {
    schema_ = input_schema;
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    RETURN_NOT_OK(
        gandiva::Projector::Make(schema_, exprs, configuration, &projector_));
    return arrow::Status::OK();
  }

  arrow::ArrayVector Evaluate(const arrow::ArrayVector& in) {
    arrow::ArrayVector outputs;
    if (in.size() > 0) {
      auto length = in[0]->length();
      auto in_batch = arrow::RecordBatch::Make(schema_, length, in);
      THROW_NOT_OK(
          projector_->Evaluate(*in_batch.get(), ctx_->memory_pool(), &outputs));
    }
    return outputs;
  }

  arrow::Status Evaluate(arrow::ArrayVector* in) {
    if ((*in).size() > 0) {
      arrow::ArrayVector outputs;
      auto length = (*in)[0]->length();
      auto in_batch = arrow::RecordBatch::Make(schema_, length, (*in));
      RETURN_NOT_OK(
          projector_->Evaluate(*in_batch.get(), ctx_->memory_pool(), &outputs));
      *in = outputs;
    }
    return arrow::Status::OK();
  }

 private:
  arrow::compute::ExecContext* ctx_;
  gandiva::SchemaPtr schema_;
  std::shared_ptr<gandiva::Projector> projector_;
};

GandivaProjector::GandivaProjector(arrow::compute::ExecContext* ctx,
                                   gandiva::SchemaPtr input_schema,
                                   gandiva::ExpressionVector exprs) {
  impl_ = std::make_shared<Impl>(ctx, input_schema, exprs);
}

arrow::Status GandivaProjector::Evaluate(arrow::ArrayVector* in) {
  return impl_->Evaluate(in);
}

arrow::ArrayVector GandivaProjector::Evaluate(const arrow::ArrayVector& in) {
  return impl_->Evaluate(in);
}
