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

#include <iostream>

#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/util.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>

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
    auto f_double = field("f_double", arrow::float64());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());
    auto f_nullable_string = field("f_nullable_string", arrow::utf8());
    auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

    ARROW_ASSIGN_OR_THROW(tmp_dir_1_,
                          std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    ARROW_ASSIGN_OR_THROW(tmp_dir_2_,
                          std::move(arrow::internal::TemporaryDir::Make(tmp_dir_prefix)))
    auto config_dirs =
        tmp_dir_1_->path().ToString() + "," + tmp_dir_2_->path().ToString();

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", config_dirs.c_str(), 1);

    schema_ = arrow::schema({f_na, f_int8_a, f_int8_b, f_uint64, f_double, f_bool,
                             f_string, f_nullable_string, f_decimal});

    MakeInputBatch(input_data_1, schema_, &input_batch_1_);
    MakeInputBatch(input_data_2, schema_, &input_batch_2_);

    split_options_ = SplitOptions::Defaults();
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      file_->Close();
    }
  }

  static void CheckFileExsists(const std::string& file_name) {
    ASSERT_EQ(*arrow::internal::FileExists(
                  *arrow::internal::PlatformFilename::FromString(file_name)),
              true);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> TakeRows(
      const std::shared_ptr<arrow::RecordBatch>& input_batch,
      const std::string& json_idx) {
    std::shared_ptr<arrow::Array> take_idx;
    ASSERT_NOT_OK(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), json_idx, &take_idx));

    auto cntx = arrow::compute::FunctionContext();
    std::shared_ptr<arrow::RecordBatch> res;
    ASSERT_NOT_OK(arrow::compute::Take(&cntx, *input_batch, *take_idx,
                                       arrow::compute::TakeOptions{}, &res));
    return res;
  }

  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>>
  GetRecordBatchStreamReader(const std::string& file_name) {
    if (file_ != nullptr && !file_->closed()) {
      RETURN_NOT_OK(file_->Close());
    }
    ARROW_ASSIGN_OR_RAISE(file_, arrow::io::ReadableFile::Open(file_name))
    ARROW_ASSIGN_OR_RAISE(auto file_reader,
                          arrow::ipc::RecordBatchStreamReader::Open(file_))
    return file_reader;
  }

  static const std::string tmp_dir_prefix;
  static const std::vector<std::string> input_data_1;
  static const std::vector<std::string> input_data_2;

  std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir_1_;
  std::shared_ptr<arrow::internal::TemporaryDir> tmp_dir_2_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Splitter> splitter_;
  SplitOptions split_options_;

  std::shared_ptr<arrow::RecordBatch> input_batch_1_;
  std::shared_ptr<arrow::RecordBatch> input_batch_2_;

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

const std::string SplitterTest::tmp_dir_prefix = "columnar-shuffle-test";
const std::vector<std::string> SplitterTest::input_data_1 = {
    "[null, null, null, null]",
    "[1, 2, 3, null]",
    "[1, -1, null, null]",
    "[null, null, null, null]",
    R"([-0.1234567, null, 0.1234567, null])",
    "[null, true, false, null]",
    R"(["alice", "bob", "alice", "bob"])",
    R"(["alice", "bob", null, null])",
    R"(["-1.01", "2.01", "-3.01", null])"};

const std::vector<std::string> SplitterTest::input_data_2 = {
    "[null, null, null, null]",
    "[null, null, null, 4]",
    "[1, -1, 1, -1]",
    "[1, 1, 1, 1]",
    R"([0.142857, -0.142857, 0.1234567, -0.1234567])",
    "[true, false, true, false]",
    R"(["bob", "alice", "bob", "alice"])",
    R"([null, null, null, null])",
    R"([null, null, "3.01", "4.01"])"};

TEST_F(SplitterTest, TestSingleSplitter) {
  split_options_.buffer_size = 2;
  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("rr", schema_, 1, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  // verify data file
  CheckFileExsists(splitter_->DataFile());

  // verify output temporary files
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 1);

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify schema
  ASSERT_EQ(*file_reader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 6);

  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    if (i == batches.size() - 1) {
      std::shared_ptr<arrow::RecordBatch> last_output_batch;
      ARROW_ASSIGN_OR_THROW(last_output_batch, TakeRows(input_batch_1_, "[2, 3]"));
      ASSERT_TRUE(rb->Equals(*last_output_batch));
    }
  }
}

TEST_F(SplitterTest, TestRoundRobinSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 2;
  ARROW_ASSIGN_OR_THROW(splitter_,
                        Splitter::Make("rr", schema_, num_partitions, split_options_));

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
  std::shared_ptr<arrow::RecordBatch> res_batch;
  ARROW_ASSIGN_OR_THROW(res_batch, TakeRows(input_batch_1_, "[0, 2]"))
  output_batches.push_back(std::move(res_batch));
  ARROW_ASSIGN_OR_THROW(res_batch, TakeRows(input_batch_1_, "[1, 3]"))
  output_batches.push_back(std::move(res_batch));

  // read first block
  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    if (i == batches.size() - 1) {
      ASSERT_TRUE(rb->Equals(*output_batches[0]));
    }
  }

  // read second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    if (i == batches.size() - 1) {
      ASSERT_TRUE(rb->Equals(*output_batches[1]));
    }
  }
}

TEST_F(SplitterTest, TestHashSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 2;

  auto f_0 = TreeExprBuilder::MakeField(schema_->field(1));
  auto f_1 = TreeExprBuilder::MakeField(schema_->field(2));
  auto f_2 = TreeExprBuilder::MakeField(schema_->field(3));

  auto node_0 = TreeExprBuilder::MakeFunction("add", {f_0, f_1}, int8());
  auto expr_0 = TreeExprBuilder::MakeExpression(node_0, field("res0", int8()));
  auto expr_1 = TreeExprBuilder::MakeExpression(f_2, field("f_uint64", uint64()));

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("hash", schema_, num_partitions,
                                                  {expr_0, expr_1}, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_));

  ASSERT_NOT_OK(splitter_->Stop());

  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);

  // verify data file
  CheckFileExsists(splitter_->DataFile());

  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify schema
  ASSERT_EQ(*file_reader->schema(), *schema_);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));

  for (const auto& rb : batches) {
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto i = 0; i < rb->num_columns(); ++i) {
      ASSERT_EQ(rb->column(i)->length(), rb->num_rows());
    }
  }
}

TEST_F(SplitterTest, TestFallbackRangeSplitter) {
  int32_t num_partitions = 2;
  split_options_.buffer_size = 2;

  std::shared_ptr<arrow::Array> pid_arr;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 0, 1]",
                                                          &pid_arr));

  std::shared_ptr<arrow::Schema> schema_w_pid;
  std::shared_ptr<arrow::RecordBatch> input_batch_1_w_pid;
  std::shared_ptr<arrow::RecordBatch> input_batch_2_w_pid;
  ARROW_ASSIGN_OR_THROW(schema_w_pid,
                        schema_->AddField(0, arrow::field("pid", arrow::int32())));
  ARROW_ASSIGN_OR_THROW(input_batch_1_w_pid,
                        input_batch_1_->AddColumn(0, "pid", pid_arr));
  ARROW_ASSIGN_OR_THROW(input_batch_2_w_pid,
                        input_batch_2_->AddColumn(0, "pid", pid_arr));

  ARROW_ASSIGN_OR_THROW(splitter_, Splitter::Make("range", std::move(schema_w_pid),
                                                  num_partitions, split_options_))

  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_w_pid));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_2_w_pid));
  ASSERT_NOT_OK(splitter_->Split(*input_batch_1_w_pid));

  ASSERT_NOT_OK(splitter_->Stop());

  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
  std::shared_ptr<arrow::RecordBatch> res_batch;
  ARROW_ASSIGN_OR_THROW(res_batch, TakeRows(input_batch_1_, "[0, 2]"))
  output_batches.push_back(std::move(res_batch));
  ARROW_ASSIGN_OR_THROW(res_batch, TakeRows(input_batch_1_, "[1, 3]"))
  output_batches.push_back(std::move(res_batch));

  // verify data file
  CheckFileExsists(splitter_->DataFile());

  // read first block
  std::shared_ptr<arrow::ipc::RecordBatchReader> file_reader;
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));

  // verify partition lengths
  const auto& lengths = splitter_->PartitionLengths();
  ASSERT_EQ(lengths.size(), 2);
  ASSERT_EQ(*file_->GetSize(), lengths[0] + lengths[1]);

  // verify schema
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    if (i == batches.size() - 1) {
      ASSERT_TRUE(rb->Equals(*output_batches[0]));
    }
  }

  // read second block
  batches.clear();
  ARROW_ASSIGN_OR_THROW(file_reader, GetRecordBatchStreamReader(splitter_->DataFile()));
  ASSERT_EQ(*file_reader->schema(), *schema_);
  ASSERT_NOT_OK(file_->Advance(lengths[0]));
  ASSERT_NOT_OK(file_reader->ReadAll(&batches));
  ASSERT_EQ(batches.size(), 3);
  for (auto i = 0; i < batches.size(); ++i) {
    const auto& rb = batches[i];
    ASSERT_EQ(rb->num_columns(), schema_->num_fields());
    ASSERT_EQ(rb->num_rows(), split_options_.buffer_size);
    for (auto j = 0; j < rb->num_columns(); ++j) {
      ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
    }
    if (i == batches.size() - 1) {
      ASSERT_TRUE(rb->Equals(*output_batches[1]));
    }
  }
}

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
