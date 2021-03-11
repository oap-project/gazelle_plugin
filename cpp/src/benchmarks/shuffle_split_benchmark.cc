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
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

std::vector<std::string> input_files;
const int num_partitions = 336;
const int buffer_size = 20480;

class BenchmarkShuffleSplit : public ::testing::Test {
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

  std::shared_ptr<Splitter> splitter;

  void DoSplit(arrow::Compression::type compression_type) {
    auto options = SplitOptions::Defaults();
    options.compression_type = compression_type;
    options.buffer_size = buffer_size;
    if (!expr_vector.empty()) {
      ARROW_ASSIGN_OR_THROW(splitter, Splitter::Make("hash", schema, num_partitions,
                                                     expr_vector, std::move(options)));
    } else {
      ARROW_ASSIGN_OR_THROW(
          splitter, Splitter::Make("rr", schema, num_partitions, std::move(options)));
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;
    int64_t elapse_read = 0;
    int64_t num_batches = 0;
    int64_t num_rows = 0;
    int64_t split_time = 0;

    do {
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      if (record_batch) {
        TIME_NANO_OR_THROW(split_time, splitter->Split(*record_batch));
        num_batches += 1;
        num_rows += record_batch->num_rows();
      }
    } while (record_batch);
    std::cout << "Done " << input_files[0] << std::endl;

    for (int i = 1; i < input_files.size(); ++i) {
      GetRecordBatchReader(input_files[i]);
      do {
        TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
        if (record_batch) {
          TIME_NANO_OR_THROW(split_time, splitter->Split(*record_batch));
          num_batches += 1;
          num_rows += record_batch->num_rows();
        }
      } while (record_batch);
      std::cout << "Done " << input_files[i] << std::endl;
    }

    TIME_NANO_OR_THROW(split_time, splitter->Stop());

    std::cout << "Setting num_partitions to " << num_partitions << ", buffer_size to "
              << buffer_size << std::endl;
    std::cout << "Total batches read:  " << num_batches << ", total rows: " << num_rows
              << std::endl;

#define BYTES_TO_STRING(bytes)                                              \
  (bytes > 1 << 20 ? (bytes * 1.0 / (1 << 20))                              \
                   : (bytes > 1 << 10 ? (bytes * 1.0 / (1 << 10)) : bytes)) \
      << (bytes > 1 << 20 ? "MiB" : (bytes > 1 << 10) ? "KiB" : "B")
    auto bytes_spilled = splitter->TotalBytesSpilled();
    auto bytes_written = splitter->TotalBytesWritten();
    std::cout << "Total bytes spilled: " << BYTES_TO_STRING(bytes_spilled) << std::endl;
    std::cout << "Total bytes written: " << BYTES_TO_STRING(bytes_written) << std::endl;
#undef BYTES_TO_STRING

    auto compute_pid_time = splitter->TotalComputePidTime();
    auto write_time = splitter->TotalWriteTime();
    auto spill_time = splitter->TotalSpillTime();
    auto compress_time = splitter->TotalCompressTime();
    split_time = split_time - spill_time - compute_pid_time - compress_time - write_time;
    std::cout << "Took " << TIME_NANO_TO_STRING(elapse_read) << " to read data"
              << std::endl
              << "Took " << TIME_NANO_TO_STRING(compute_pid_time) << " to compute pid"
              << std::endl
              << "Took " << TIME_NANO_TO_STRING(split_time) << " to split" << std::endl
              << "Took " << TIME_NANO_TO_STRING(spill_time) << " to spill" << std::endl
              << "Took " << TIME_NANO_TO_STRING(write_time) << " to write" << std::endl
              << "Took " << TIME_NANO_TO_STRING(compress_time) << " to compress"
              << std::endl;
  }
};

TEST_F(BenchmarkShuffleSplit, LZ4) { DoSplit(arrow::Compression::LZ4_FRAME); }
TEST_F(BenchmarkShuffleSplit, FASTPFOR) { DoSplit(arrow::Compression::FASTPFOR); }

}  // namespace shuffle
}  // namespace sparkcolumnarplugin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    for (int i = 1; i < argc; ++i) {
      sparkcolumnarplugin::shuffle::input_files.emplace_back(argv[i]);
    }
  }
  return RUN_ALL_TESTS();
}
