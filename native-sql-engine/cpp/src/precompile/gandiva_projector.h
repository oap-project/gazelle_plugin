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

#include <arrow/compute/api.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <math.h>

#include <cstdint>
#include <type_traits>

class GandivaProjector {
 public:
  GandivaProjector(arrow::compute::ExecContext* ctx, gandiva::SchemaPtr input_schema,
                   gandiva::ExpressionVector exprs);
  arrow::Status Evaluate(arrow::ArrayVector* in);
  arrow::ArrayVector Evaluate(const arrow::ArrayVector& in);

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
