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

#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/util.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <jni.h>

#include <iostream>

#include "jni/jni_common.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace jniutils {

class JniUtilsTest : public ::testing::Test {
 protected:
};

TEST_F(JniUtilsTest, TestMakeRecordBatchWithList) {
  auto f_arr_str = field("f_arr", arrow::list(arrow::utf8()));
  auto f_arr_bool = field("f_bool", arrow::list(arrow::boolean()));
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::int32()));
  auto f_arr_double = field("f_double", arrow::list(arrow::float64()));
  auto f_arr_decimal = field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto rb_schema =
      arrow::schema({f_arr_str, f_arr_bool, f_arr_int32, f_arr_double, f_arr_decimal});

  const std::vector<std::string> input_data_arr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], null, [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], null])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], ["9.88"]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  std::shared_ptr<arrow::RecordBatch> res_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  auto num_rows = input_batch_arr->num_rows();
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  std::vector<int64_t> in_buf_addrs;
  std::vector<int64_t> in_buf_sizes;
  for (int i = 0; i < rb_schema->num_fields(); ++i) {
    ASSERT_NOT_OK(AppendBuffers(input_batch_arr->column(i), &buffers));
  }
  for (auto buffer : buffers) {
    if (buffer == nullptr) {
      in_buf_addrs.push_back(0);
      in_buf_sizes.push_back(0);
    } else {
      in_buf_addrs.push_back((int64_t)buffer->data());
      in_buf_sizes.push_back((int64_t)buffer->size());
    }
  }

  auto status = MakeRecordBatch(rb_schema, num_rows, &in_buf_addrs[0], &in_buf_sizes[0],
                                buffers.size(), &res_batch_arr);

  const auto& rb = res_batch_arr;
  ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
  for (auto j = 0; j < rb->num_columns(); ++j) {
    ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
  }
  ASSERT_TRUE(rb->Equals(*input_batch_arr.get()));
}

TEST_F(JniUtilsTest, TestRecordBatchConcatenate) {
  auto f_arr_str = field("f_arr", arrow::list(arrow::utf8()));
  auto f_arr_bool = field("f_bool", arrow::list(arrow::boolean()));
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::int32()));
  auto f_arr_double = field("f_double", arrow::list(arrow::float64()));
  auto f_arr_decimal = field("f_decimal", arrow::list(arrow::decimal(10, 2)));

  auto schema =
      arrow::schema({f_arr_str, f_arr_bool, f_arr_int32, f_arr_double, f_arr_decimal});

  const std::vector<std::string> input_data_arr = {
      R"([["alice0", "bob1"], ["alice2"], ["bob3"], ["Alice4", "Bob5", "AlicE6"], ["boB7"], ["ALICE8", "BOB9"]])",
      R"([[true, null], [true, true, true], [false], [true], [false], [false]])",
      R"([[1, 2, 3], [9, 8], null, [3, 1], [0], [1, 9, null]])",
      R"([[0.26121], [-9.12123, 6.111111], [8.121], [7.21, null], [3.2123, 6,1121], null])",
      R"([["0.26"], ["-9.12", "6.11"], ["8.12"], ["7.21", null], ["3.21", "6.11"], ["9.88"]])"};

  std::shared_ptr<arrow::RecordBatch> batch1;
  std::shared_ptr<arrow::RecordBatch> batch2;
  std::shared_ptr<arrow::RecordBatch> res_batch_arr;
  MakeInputBatch(input_data_arr, schema, &batch1);
  MakeInputBatch(input_data_arr, schema, &batch2);

  arrow::RecordBatchVector batches;
  batches.push_back(batch1);
  batches.push_back(batch2);

  for (int i = 0; i < 100; i++) {
    for (int i = 0; i < 10000; i++) {
      int total_num_rows = batch1->num_rows() + batch2->num_rows();

      // int num_columns = batches.at(0)->num_columns();
      int num_columns = schema->num_fields();
      arrow::ArrayVector arrayColumns;
      for (jint i = 0; i < num_columns; i++) {
        arrow::ArrayVector arrvec;
        for (const auto& batch : batches) {
          arrvec.push_back(batch->column(i));
        }
        std::shared_ptr<arrow::Array> bigArr;
        Concatenate(arrvec, default_memory_pool(), &bigArr);
        // ARROW_ASSIGN_OR_RAISE(auto bigArr, Concatenate(arrvec, pool));
        arrayColumns.push_back(bigArr);
      }
      auto out_batch = arrow::RecordBatch::Make(schema, total_num_rows, arrayColumns);

      std::cout << "out_batch->num_rows():" << out_batch->num_rows() << std::endl;
    }

    sleep(3);
  }
}

TEST_F(JniUtilsTest, TestMakeRecordBatchBuild_Int_Struct) {
  auto f_int32 = field("f_simple_int32", arrow::int32());
  auto f_struct_int32 =
      field("f_struct", struct_({field("a", int32()), field("b", int32())}));

  auto rb_schema = arrow::schema({f_int32, f_struct_int32});

  const std::vector<std::string> input_data_arr = {
      R"([1, 2])", R"([{"a": 1, "b": 6}, {"a": 2, "b": 7}])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  std::shared_ptr<arrow::RecordBatch> res_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  auto num_rows = input_batch_arr->num_rows();
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  std::vector<int64_t> in_buf_addrs;
  std::vector<int64_t> in_buf_sizes;
  for (int i = 0; i < rb_schema->num_fields(); ++i) {
    ASSERT_NOT_OK(AppendBuffers(input_batch_arr->column(i), &buffers));
  }

  for (auto buffer : buffers) {
    if (buffer == nullptr) {
      in_buf_addrs.push_back(0);
      in_buf_sizes.push_back(0);
    } else {
      in_buf_addrs.push_back((int64_t)buffer->data());
      in_buf_sizes.push_back((int64_t)buffer->size());
    }
  }

  auto status = MakeRecordBatch(rb_schema, num_rows, &in_buf_addrs[0], &in_buf_sizes[0],
                                buffers.size(), &res_batch_arr);

  const auto& rb = res_batch_arr;
  ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
  for (auto j = 0; j < rb->num_columns(); ++j) {
    ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
  }

  ASSERT_TRUE(rb->Equals(*input_batch_arr.get()));
}

TEST_F(JniUtilsTest, TestMakeRecordBatchBuild_map) {
  auto f_map = field("f_map", map(int32(), int32()));

  auto rb_schema = arrow::schema({f_map});

  const std::vector<std::string> input_data_arr = {R"([[[1, 2]], [[1, 2]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  std::shared_ptr<arrow::RecordBatch> res_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  auto num_rows = input_batch_arr->num_rows();
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  std::vector<int64_t> in_buf_addrs;
  std::vector<int64_t> in_buf_sizes;
  for (int i = 0; i < rb_schema->num_fields(); ++i) {
    ASSERT_NOT_OK(AppendBuffers(input_batch_arr->column(i), &buffers));
  }

  for (auto buffer : buffers) {
    if (buffer == nullptr) {
      in_buf_addrs.push_back(0);
      in_buf_sizes.push_back(0);
    } else {
      in_buf_addrs.push_back((int64_t)buffer->data());
      in_buf_sizes.push_back((int64_t)buffer->size());
    }
  }

  auto status = MakeRecordBatch(rb_schema, num_rows, &in_buf_addrs[0], &in_buf_sizes[0],
                                buffers.size(), &res_batch_arr);

  const auto& rb = res_batch_arr;
  ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
  for (auto j = 0; j < rb->num_columns(); ++j) {
    ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
  }

  ASSERT_TRUE(rb->Equals(*input_batch_arr.get()));
}

TEST_F(JniUtilsTest, TestMakeRecordBatchBuild_list_map) {
  auto f_arr_int32 = field("f_int32", arrow::list(arrow::list(arrow::int32())));
  auto f_arr_list_map = field("f_list_map", list(map(utf8(), utf8())));

  auto rb_schema = arrow::schema({f_arr_int32, f_arr_list_map});

  const std::vector<std::string> input_data_arr = {
      R"([[[1, 2, 3]], [[9, 8], [null]], [[3, 1], [0]], [[1, 9, null]]])",
      R"([[[["key1", "val_aa1"]]], [[["key1", "val_bb1"]], [["key2", "val_bb2"]]], [[["key1", "val_cc1"]]], [[["key1", "val_dd1"]]]])"};

  std::shared_ptr<arrow::RecordBatch> input_batch_arr;
  std::shared_ptr<arrow::RecordBatch> res_batch_arr;
  MakeInputBatch(input_data_arr, rb_schema, &input_batch_arr);

  auto num_rows = input_batch_arr->num_rows();
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  std::vector<int64_t> in_buf_addrs;
  std::vector<int64_t> in_buf_sizes;
  for (int i = 0; i < rb_schema->num_fields(); ++i) {
    ASSERT_NOT_OK(AppendBuffers(input_batch_arr->column(i), &buffers));
  }

  for (auto buffer : buffers) {
    if (buffer == nullptr) {
      in_buf_addrs.push_back(0);
      in_buf_sizes.push_back(0);
    } else {
      in_buf_addrs.push_back((int64_t)buffer->data());
      in_buf_sizes.push_back((int64_t)buffer->size());
    }
  }

  auto status = MakeRecordBatch(rb_schema, num_rows, &in_buf_addrs[0], &in_buf_sizes[0],
                                buffers.size(), &res_batch_arr);

  const auto& rb = res_batch_arr;
  ASSERT_EQ(rb->num_columns(), rb_schema->num_fields());
  for (auto j = 0; j < rb->num_columns(); ++j) {
    ASSERT_EQ(rb->column(j)->length(), rb->num_rows());
  }

  ASSERT_TRUE(rb->Equals(*input_batch_arr.get()));
}

}  // namespace jniutils
}  // namespace sparkcolumnarplugin
