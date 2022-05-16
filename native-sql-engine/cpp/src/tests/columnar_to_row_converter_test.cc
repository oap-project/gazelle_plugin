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

#include "operators/columnar_to_row_converter.h"

#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/util.h>
#include <arrow/record_batch.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>

#include <iostream>

#include "tests/test_utils.h"
#include "operators/row_to_columnar_converter.h"

namespace sparkcolumnarplugin {
namespace columnartorow {

class MyMemoryPool : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool(int64_t capacity) : capacity_(capacity) {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (bytes_allocated() + size > capacity_) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    return arrow::Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size > capacity_) {
      return Status::OutOfMemory("malloc of size ", new_size, " failed");
    }
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return pool_->max_memory(); }

  std::string backend_name() const override { return pool_->backend_name(); }

 private:
  MemoryPool* pool_ = arrow::default_memory_pool();
  int64_t capacity_;
  arrow::internal::MemoryPoolStats stats_;
};

class UnsaferowTest : public ::testing::Test {
 protected:
  void SetUp() {
  }
};

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultForInt_64) {

  const std::vector<std::string> input_data = {
                                                             "[1, 2, 3]"};

  auto f_int64 = field("f_int64", arrow::int64());                                                          
  auto schema = arrow::schema({f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                               arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t a[16] = {0,0,0,0,0,0,0,0,
                   1,0,0,0,0,0,0,0
                  };

  auto length_vec = columnarToRowConverter->GetLengths();

  long arr[length_vec.size()];
  for(int i=0; i<length_vec.size(); i++){
    arr[i] = length_vec[i];
  }
  long *lengthPtr = arr;

  std::shared_ptr<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter>(schema,
      num_cols, num_rows,
      lengthPtr, address, arrow::default_memory_pool());
  std::shared_ptr<arrow::RecordBatch> rb;
  row_to_columnar_converter->Init(&rb);
  std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
  ASSERT_TRUE(rb->Equals(*input_batch));
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResult) {
    auto f_int8 = field("f_int8_a", arrow::int8());
    auto f_int16 = field("f_int16", arrow::int16());
    auto f_int32 = field("f_int32", arrow::int32());
    auto f_int64 = field("f_int64", arrow::int64());
    auto f_double = field("f_double", arrow::float64());
    auto f_float = field("f_float", arrow::float32());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());
    auto f_binary = field("f_binary", arrow::binary());
    auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

	  auto schema = arrow::schema({f_int8, f_int16, f_int32, f_int64, f_float, f_double,
                             f_binary});
	
	
	  const std::vector<std::string> input_data = {
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[3.5, 3.5]",
                                                             "[1, 1]",
                                                             R"(["abc", "abc"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                               arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();
  auto length_vec = columnarToRowConverter->GetLengths();

  long arr[length_vec.size()];
  for(int i=0; i<length_vec.size(); i++){
    arr[i] = length_vec[i];
  }
  long *lengthPtr = arr;

  std::shared_ptr<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter>(schema,
      num_cols, num_rows,
      lengthPtr, address, arrow::default_memory_pool());
  std::shared_ptr<arrow::RecordBatch> rb;
  row_to_columnar_converter->Init(&rb);
  std::cout << "input_batch->ToString():\n" << input_batch->ToString() << std::endl;
  std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
  ASSERT_TRUE(rb->Equals(*input_batch));
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultForInt_64_twoColumn) {

  const std::vector<std::string> input_data = {
                                                             "[1, 2]",
                                                             "[1, 2]",
                                                             };

  auto f_int64_col0 = field("f_int64", arrow::int64());
  auto f_int64_col1 = field("f_int64", arrow::int64());                                                            
  auto schema = arrow::schema({f_int64_col0, f_int64_col1});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                               arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  auto length_vec = columnarToRowConverter->GetLengths();
  long arr[length_vec.size()];
  for(int i=0; i<length_vec.size(); i++){
    arr[i] = length_vec[i];
  }
  long *lengthPtr = arr;

  std::shared_ptr<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter>(schema,
      num_cols, num_rows,
      lengthPtr, address, arrow::default_memory_pool());
  std::shared_ptr<arrow::RecordBatch> rb;
  row_to_columnar_converter->Init(&rb);
  std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
  ASSERT_TRUE(rb->Equals(*input_batch));
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_int8_int16) {
  auto f_int8 = field("f_int8_a", arrow::int8());
  auto f_int16 = field("f_int16", arrow::int16());

  std::cout << "---------verify f_int8, f_int16---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[1, 2]",
                                                "[1, 2]",
                                                };
                                                                                                                
  auto schema = arrow::schema({f_int8, f_int16});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_int32_int64) {
  auto f_int32 = field("f_int32", arrow::int32());
  auto f_int64 = field("f_int64", arrow::int64());

   
  std::cout << "---------verify f_int32, f_int64---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[1, 2]",
                                                "[1, 2]",
                                                };
                                                                                                              
  auto schema = arrow::schema({f_int32, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_float_double) {
  auto f_float = field("f_float", arrow::float32());
  auto f_double = field("f_double", arrow::float64());

  std::cout << "---------verify f_float, f_double---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[1.0, 2.0]",
                                                "[1.0, 2.0]",
                                                };
                                                                                                              
  auto schema = arrow::schema({f_float, f_double});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          0,0,128,63,0,0,0,0,
                          0,0,0,0,0,0,240,63,
                          0,0,0,0,0,0,0,0,
                          0,0,0,64,0,0,0,0,
                          0,0,0,0,0,0,0,64
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
  

}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_bool_binary) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());
  
  std::cout << "---------verify f_bool, f_binary---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[false, true]",
                                                R"(["aa", "bb"])",
                                                };
                                                                                                              
  auto schema = arrow::schema({f_bool, f_binary});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,24,0,0,0,
                          97,97,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          2,0,0,0,24,0,0,0,
                          98,98,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }

}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_decimal_string) {
  auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));
  auto f_string = field("f_string", arrow::utf8());
  
  std::cout << "---------verify f_decimal, f_string---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                R"(["1.00", "2.00"])",
                                                R"(["aa", "bb"])"
                                                };
                                                                                                              
  auto schema = arrow::schema({f_decimal, f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          100,0,0,0,0,0,0,0,
                          2,0,0,0,24,0,0,0,
                          97,97,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          200,0,0,0,0,0,0,0,
                          2,0,0,0,24,0,0,0,
                          98,98,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_int64_int64_with_null) {
  auto f_int64 = field("f_int64", arrow::int64());

  std::cout << "---------verify f_int64, f_int64 with null ---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[null,2]",
                                                "[null,2]",
                                                };
                                                                                                              
  auto schema = arrow::schema({f_int64, f_int64});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {3,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          2,0,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_string) {
  auto f_binary = field("f_binary", arrow::binary());
  auto f_string = field("f_string", arrow::utf8());
  
  std::cout << "---------verify f_string---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                R"(["aa", "bb"])"
                                                };
                                                                                                              
  auto schema = arrow::schema({f_string});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,16,0,0,0,
                          97,97,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          2,0,0,0,16,0,0,0,
                          98,98,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResultBuffer_bool) {
  auto f_bool = field("f_bool", arrow::boolean());
  auto f_binary = field("f_binary", arrow::binary());
  
  std::cout << "---------verify f_bool---------" << std::endl;
  const std::vector<std::string> input_data = {
                                                "[false, true]",
                                               
                                                };
                                                                                                              
  auto schema = arrow::schema({f_bool});
  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                              arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();

  uint8_t expect_arr[] = {0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          1,0,0,0,0,0,0,0,
                          };
  for(int i=0; i< sizeof(expect_arr); i++) {
    std::cout << "*(address+" << i << "): " << (uint16_t)*(address+i) << std::endl;
    std::cout << "*(expect_arr+" << i << "): " << (uint16_t)*(expect_arr+i) << std::endl;
    ASSERT_EQ(*(address+i), *(expect_arr+i));
  }

}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResult_allTypes) {
    auto f_int8 = field("f_int8_a", arrow::int8());
    auto f_int16 = field("f_int16", arrow::int16());
    auto f_int32 = field("f_int32", arrow::int32());
    auto f_int64 = field("f_int64", arrow::int64());
    auto f_double = field("f_double", arrow::float64());
    auto f_float = field("f_float", arrow::float32());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());
    auto f_binary = field("f_binary", arrow::binary());
    auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

	  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double,
                             f_binary, f_decimal});
	
	
	  const std::vector<std::string> input_data = {"[true, true]",
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[1, 1]",
                                                             "[3.5, 3.5]",
                                                             "[1, 1]",
                                                             R"(["abc", "abc"])",
                                                             R"(["100.00", "100.00"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                               arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();
  auto length_vec = columnarToRowConverter->GetLengths();
  for(int i=0; i< length_vec.size(); i++)
  {
	std::cout << "length_vec[" << i << "]:" << length_vec[i] << std::endl;
  }

  long arr[length_vec.size()];
  for(int i=0; i<length_vec.size(); i++){
    arr[i] = length_vec[i];
  }
  long *lengthPtr = arr;

  std::shared_ptr<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter>(schema,
      num_cols, num_rows,
      lengthPtr, address, arrow::default_memory_pool());
  std::shared_ptr<arrow::RecordBatch> rb;
  row_to_columnar_converter->Init(&rb);
  std::cout << "input_batch->ToString():\n" << input_batch->ToString() << std::endl;
  std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
  ASSERT_TRUE(rb->Equals(*input_batch));
}

TEST_F(UnsaferowTest, TestColumnarToRowConverterResult_allTypes_18rows) {
    auto f_int8 = field("f_int8_a", arrow::int8());
    auto f_int16 = field("f_int16", arrow::int16());
    auto f_int32 = field("f_int32", arrow::int32());
    auto f_int64 = field("f_int64", arrow::int64());
    auto f_double = field("f_double", arrow::float64());
    auto f_float = field("f_float", arrow::float32());
    auto f_bool = field("f_bool", arrow::boolean());
    auto f_string = field("f_string", arrow::utf8());
    auto f_binary = field("f_binary", arrow::binary());
    auto f_decimal = field("f_decimal128", arrow::decimal(10, 2));

	  auto schema = arrow::schema({f_bool, f_int8, f_int16, f_int32, f_int64, f_float, f_double,
                             f_binary, f_decimal});
	
	
	  const std::vector<std::string> input_data = {
    "[true, true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]",
    "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    "[3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5]",
    "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    R"(["abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc"])",
    R"(["100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00"])"};

  std::shared_ptr<arrow::RecordBatch> input_batch;
  MakeInputBatch(input_data, schema, &input_batch);

  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      std::make_shared<ColumnarToRowConverter>(
                                               arrow::default_memory_pool());

  columnarToRowConverter->Init(input_batch);
  columnarToRowConverter->Write();

  int64_t num_rows = input_batch->num_rows();
  int64_t num_cols = input_batch->num_columns();
  uint8_t* address = columnarToRowConverter->GetBufferAddress();
  auto length_vec = columnarToRowConverter->GetLengths();
  for(int i=0; i< length_vec.size(); i++)
  {
	std::cout << "length_vec[" << i << "]:" << length_vec[i] << std::endl;
  }

  long arr[length_vec.size()];
  for(int i=0; i<length_vec.size(); i++){
    arr[i] = length_vec[i];
  }
  long *lengthPtr = arr;

  std::shared_ptr<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter>(schema,
      num_cols, num_rows,
      lengthPtr, address, arrow::default_memory_pool());
  std::shared_ptr<arrow::RecordBatch> rb;
  row_to_columnar_converter->Init(&rb);
  std::cout << "input_batch->ToString():\n" << input_batch->ToString() << std::endl;
  std::cout << "From rowbuffer to Column, rb->ToString():\n" << rb->ToString() << std::endl;
  ASSERT_TRUE(rb->Equals(*input_batch));
}

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin
