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

#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <deque>

namespace sparkcolumnarplugin {
namespace shuffle {

static constexpr int32_t kDefaultSplitterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;

struct BufferInfo {
  std::shared_ptr<arrow::Buffer> validity_buffer;
  std::shared_ptr<arrow::Buffer> value_buffer;
  uint8_t* validity_addr;
  uint8_t* value_addr;
};

struct BufferAddr {
  uint8_t* validity_addr;
  uint8_t* value_addr;
};

namespace Type {
/// \brief Data type enumeration for shuffle splitter
///
/// This enumeration maps the types of arrow::Type::type with same length
/// to identical type

enum typeId : int {
  SHUFFLE_1BYTE,
  SHUFFLE_2BYTE,
  SHUFFLE_4BYTE,
  SHUFFLE_8BYTE,
  SHUFFLE_DECIMAL128,
  SHUFFLE_BIT,
  SHUFFLE_BINARY,
  SHUFFLE_LARGE_BINARY,
  SHUFFLE_NULL,
  NUM_TYPES,
  SHUFFLE_NOT_IMPLEMENTED
};

static const typeId all[] = {
    SHUFFLE_1BYTE,  SHUFFLE_2BYTE,        SHUFFLE_4BYTE,
    SHUFFLE_8BYTE,  SHUFFLE_DECIMAL128,  SHUFFLE_BIT,
    SHUFFLE_BINARY, SHUFFLE_LARGE_BINARY, SHUFFLE_NULL,
};

}  // namespace Type

using BufferInfos = std::deque<std::unique_ptr<BufferInfo>>;
using TypeBufferInfos = std::vector<BufferInfos>;
using BinaryBuilders = std::deque<std::unique_ptr<arrow::BinaryBuilder>>;
using LargeBinaryBuilders = std::deque<std::unique_ptr<arrow::LargeBinaryBuilder>>;
using BufferPtr = std::shared_ptr<arrow::Buffer>;
using SrcBuffers = std::vector<BufferAddr>;
using SrcArrays = std::vector<std::shared_ptr<arrow::Array>>;
using SrcBinaryArrays = std::vector<std::shared_ptr<arrow::BinaryArray>>;
using SrcLargeBinaryArrays = std::vector<std::shared_ptr<arrow::LargeBinaryArray>>;

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
