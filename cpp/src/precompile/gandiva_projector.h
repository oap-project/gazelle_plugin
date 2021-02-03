#pragma once

#include <arrow/compute/context.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <math.h>

#include <cstdint>
#include <type_traits>

class GandivaProjector {
 public:
  GandivaProjector(arrow::compute::FunctionContext* ctx, gandiva::SchemaPtr input_schema,
                   gandiva::ExpressionVector exprs);
  arrow::Status Evaluate(arrow::ArrayVector* in);
  arrow::ArrayVector Evaluate(const arrow::ArrayVector& in);

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
