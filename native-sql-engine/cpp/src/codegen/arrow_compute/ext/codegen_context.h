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

#include <arrow/status.h>
#include <arrow/type_fwd.h>

class GandivaProjector;

struct CodeGenContext {
  std::vector<std::string> header_codes;
  std::string relation_prepare_codes;
  std::string prepare_codes;
  std::string unsafe_row_prepare_codes;
  std::string process_codes;
  std::string finish_codes;
  std::string definition_codes;
  std::string aggregate_prepare_codes;
  std::string aggregate_finish_condition_codes;
  std::string aggregate_finish_codes;
  std::vector<std::string> function_list;
  std::vector<std::pair<std::pair<std::string, std::string>,
                        std::shared_ptr<arrow::DataType>>>
      output_list;
  std::shared_ptr<GandivaProjector> gandiva_projector;
};