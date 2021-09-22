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

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <shuffle/splitter.h>

#include <chrono>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "operators/columnar_to_row_converter.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace columnartorow {

std::vector<std::string> input_files;
const int num_partitions = 336;
const int buffer_size = 20480;

class BenchmarkColumnarToRow : public ::testing::Test {
 public:
  void GetRecordBatchReader(const std::string& input_file) {
    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string file_name;
    ARROW_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(input_file, &file_name))

    ARROW_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

    parquet::ArrowReaderProperties properties(true);
    properties.set_batch_size(buffer_size);

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file),
        properties, &parquet_reader));

    ASSERT_NOT_OK(parquet_reader->GetSchema(&schema));

    auto num_rowgroups = parquet_reader->num_row_groups();
    std::vector<int> row_group_indices;
    for (int i = 0; i < num_rowgroups; ++i) {
      row_group_indices.push_back(i);
    }

    auto num_columns = schema->num_fields();
    std::vector<int> column_indices;
    for (int i = 0; i < num_columns; ++i) {
      column_indices.push_back(i);
    }

    ASSERT_NOT_OK(parquet_reader->GetRecordBatchReader(row_group_indices, column_indices,
                                                       &record_batch_reader));
  }
  void SetUp() override {
    // read input from parquet file
    if (input_files.empty()) {
      std::cout << "No input file." << std::endl;
      std::exit(0);
    }
    std::cout << "Input file: " << std::endl;
    for (const auto& file_name : input_files) {
      std::cout << file_name << std::endl;
    }
    GetRecordBatchReader(input_files[0]);
    std::cout << schema->ToString() << std::endl;

    const auto& fields = schema->fields();
    for (const auto& field : fields) {
      if (field->name() == "l_partkey") {
        auto node = gandiva::TreeExprBuilder::MakeField(field);
        expr_vector.push_back(gandiva::TreeExprBuilder::MakeExpression(
            std::move(node), arrow::field("res_" + field->name(), field->type())));
      }
    }
  }

  void TearDown() override {}

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
  std::shared_ptr<RecordBatchReader> record_batch_reader;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
};
TEST_F(BenchmarkColumnarToRow, test) {
  int64_t elapse_read = 0;
  int64_t elapse_init = 0;
  int64_t elapse_write = 0;
  std::shared_ptr<arrow::RecordBatch> record_batch;
  TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));

  if (record_batch) {
    std::shared_ptr<ColumnarToRowConverter> unsafe_row_writer_reader =
        std::make_shared<ColumnarToRowConverter>(record_batch, arrow::default_memory_pool());

    TIME_NANO_OR_THROW(elapse_init, unsafe_row_writer_reader->Init());
    TIME_NANO_OR_THROW(elapse_write, unsafe_row_writer_reader->Write());

    std::cout << "Took " << TIME_NANO_TO_STRING(elapse_read) << " to read data"
              << std::endl
              << "Took " << TIME_NANO_TO_STRING(elapse_init) << " to init" << std::endl
              << "Took " << TIME_NANO_TO_STRING(elapse_write) << " to write" << std::endl;
  }
}

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    for (int i = 1; i < argc; ++i) {
      sparkcolumnarplugin::columnartorow::input_files.emplace_back(argv[i]);
    }
  }
  return RUN_ALL_TESTS();
}
