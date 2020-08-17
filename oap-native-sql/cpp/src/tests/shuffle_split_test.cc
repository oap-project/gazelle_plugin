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

#include <arrow/io/api.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/util.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>
#include <iostream>
#include "shuffle/splitter.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

class SplitterTest : public ::testing::Test {
 protected:
  void SetUp() {
    auto f_na = field("f_na", arrow::null());
    auto f_int8_a = field("f_int8_a", arrow::int8());
    auto f_int8_b = field("f_int8_b", arrow::int8());
    auto f_uint64 = field("f_uint64", arrow::uint64());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());

    std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir1;
    std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir2;
    ARROW_ASSIGN_OR_THROW(tmp_dir1,
                          std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    ARROW_ASSIGN_OR_THROW(tmp_dir2,
                          std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    auto config_dirs = tmp_dir1->path().ToString() + "," + tmp_dir2->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", config_dirs.c_str(), 1);

    schema_ = arrow::schema({f_na, f_int8_a, f_int8_b, f_uint64, f_bool, f_string});
  }

  static const std::string tmp_dir_prefix;
  static const std::vector<std::string> input_data;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Splitter> splitter_;
};

const std::string SplitterTest::tmp_dir_prefix = "columnar-shuffle-test";
const std::vector<std::string> SplitterTest::input_data = {
    "[null, null, null, null]", "[1, 2, 3, null]",    "[1, -1, null, null]",
    "[null, null, null, null]", "[null, 1, 0, null]", R"(["alice", "bob", null, null])"};

TEST_F(SplitterTest, TestRoundRobinSplitter) {
  int32_t num_partitions = 3;
  int32_t buffer_size = 3;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, num_partitions))
  splitter_->set_buffer_size(buffer_size);

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema_, &input_batch);

  int split_times = 3;
  for (int i = 0; i < split_times; ++i) {
    ASSERT_NOT_OK(splitter_->Split(*input_batch))
  }
  ASSERT_NOT_OK(splitter_->Stop());

  auto file_info = splitter_->GetPartitionFileInfo();

  for (auto& info : file_info) {
    auto file_name = info.second;
    ASSERT_EQ(*arrow::internal::FileExists(
                  *arrow::internal::PlatformFilename::FromString(file_name)),
              true);
    ASSERT_NE(file_name.find(tmp_dir_prefix), std::string::npos);

    std::shared_ptr<arrow::io::ReadableFile> file_in;
    std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
    ARROW_ASSIGN_OR_THROW(file_in, arrow::io::ReadableFile::Open(file_name))

    ARROW_ASSIGN_OR_THROW(file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_in))
    ASSERT_EQ(*file_reader->schema(), *splitter_->schema());

    std::shared_ptr<arrow::RecordBatch> rb;
    ASSERT_NOT_OK(file_reader->ReadNext(&rb));
    ASSERT_EQ(rb->num_rows(), buffer_size);

    if (!file_in->closed()) {
      ASSERT_NOT_OK(file_in->Close());
    }
  }
}

TEST_F(SplitterTest, TestHashSplitter) {
  int32_t num_partitions = 3;
  int32_t buffer_size = 3;

  auto f_0 = TreeExprBuilder::MakeField(schema_->field(1));
  auto f_1 = TreeExprBuilder::MakeField(schema_->field(2));
  auto f_2 = TreeExprBuilder::MakeField(schema_->field(3));

  auto node_0 = TreeExprBuilder::MakeFunction("add", {f_0, f_1}, int8());
  auto expr_0 = TreeExprBuilder::MakeExpression(node_0, field("res0", int8()));
  auto expr_1 = TreeExprBuilder::MakeExpression(f_2, field("f_uint64", uint64()));

  ARROW_ASSIGN_OR_THROW(
      splitter_, Splitter::Make("hash", schema_, num_partitions, {expr_0, expr_1}))
  splitter_->set_buffer_size(buffer_size);

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema_, &input_batch);

  int split_times = 3;
  for (int i = 0; i < split_times; ++i) {
    ASSERT_NOT_OK(splitter_->Split(*input_batch))
  }
  ASSERT_NOT_OK(splitter_->Stop());

  auto file_info = splitter_->GetPartitionFileInfo();

  for (auto& info : file_info) {
    auto file_name = info.second;
    ASSERT_EQ(*arrow::internal::FileExists(
                  *arrow::internal::PlatformFilename::FromString(file_name)),
              true);
    ASSERT_NE(file_name.find(tmp_dir_prefix), std::string::npos);

    std::shared_ptr<arrow::io::ReadableFile> file_in;
    std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
    ARROW_ASSIGN_OR_THROW(file_in, arrow::io::ReadableFile::Open(file_name))

    ARROW_ASSIGN_OR_THROW(file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_in))
    ASSERT_EQ(*file_reader->schema(), *splitter_->schema());

    std::shared_ptr<arrow::RecordBatch> rb;
    ASSERT_NOT_OK(file_reader->ReadNext(&rb));
    ASSERT_EQ(rb->num_rows(), buffer_size);

    if (!file_in->closed()) {
      ASSERT_NOT_OK(file_in->Close());
    }
  }
}

TEST_F(SplitterTest, TestFallbackRangeSplitter) {
  int32_t num_partitions = 3;
  int32_t buffer_size = 3;

  std::shared_ptr<arrow::RecordBatch> input_batch_wo_pid;
  MakeInputBatch(input_data, schema_, &input_batch_wo_pid);

  std::shared_ptr<arrow::Array> pid_arr;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 0, 2]",
                                                          &pid_arr));

  std::shared_ptr<arrow::RecordBatch> input_batch;
  ARROW_ASSIGN_OR_THROW(input_batch, input_batch_wo_pid->AddColumn(0, "pid", pid_arr));
  auto new_schema = input_batch->schema();

  ARROW_ASSIGN_OR_THROW(splitter_,
                        Splitter::Make("range", std::move(new_schema), num_partitions))

  splitter_->set_buffer_size(buffer_size);

  int split_times = 3;
  for (int i = 0; i < split_times; ++i) {
    ASSERT_NOT_OK(splitter_->Split(*input_batch));
  }
  ASSERT_NOT_OK(splitter_->Stop());

  auto file_info = splitter_->GetPartitionFileInfo();

  for (auto& info : file_info) {
    auto file_name = info.second;
    ASSERT_EQ(*arrow::internal::FileExists(
                  *arrow::internal::PlatformFilename::FromString(file_name)),
              true);
    ASSERT_NE(file_name.find(tmp_dir_prefix), std::string::npos);

    std::shared_ptr<arrow::io::ReadableFile> file_in;
    std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
    ARROW_ASSIGN_OR_THROW(file_in, arrow::io::ReadableFile::Open(file_name))

    ARROW_ASSIGN_OR_THROW(file_reader, arrow::ipc::RecordBatchStreamReader::Open(file_in))
    ASSERT_EQ(*file_reader->schema(), **splitter_->schema()->RemoveField(0));

    std::shared_ptr<arrow::RecordBatch> rb;
    ASSERT_NOT_OK(file_reader->ReadNext(&rb));
    ASSERT_EQ(rb->num_rows(), buffer_size);

    if (!file_in->closed()) {
      ASSERT_NOT_OK(file_in->Close());
    }
  }
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
