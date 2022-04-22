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
#include <benchmark/benchmark.h>

#include <chrono>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "operators/columnar_to_row_converter.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace columnartorow {

const int batch_buffer_size = 32768;

class GoogleBenchmarkColumnarToRow : public ::benchmark::Fixture {
 public:
 GoogleBenchmarkColumnarToRow()
  {
    file_name = "/mnt/DP_disk1/lineitem/part-00025-356249a2-c285-42b9-8a18-5b10be61e0c4-c000.snappy.parquet";
    GetRecordBatchReader(file_name);
  }


  void GetRecordBatchReader(const std::string& input_file) {

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string file_name;
    ARROW_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(input_file, &file_name))

    ARROW_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

    properties.set_batch_size(batch_buffer_size);
    properties.set_pre_buffer(false);
    properties.set_use_threads(false);

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file),
        properties, &parquet_reader));

    ASSERT_NOT_OK(parquet_reader->GetSchema(&schema));

    auto num_rowgroups = parquet_reader->num_row_groups();
    
    for (int i = 0; i < num_rowgroups; ++i) {
      row_group_indices.push_back(i);
    }

    auto num_columns = schema->num_fields();
    for (int i = 0; i < num_columns; ++i) {
      column_indices.push_back(i);
    }
  }

  void SetUp(const ::benchmark::State& state) {
  }

  void TearDown(const ::benchmark::State& state) {
  }

 protected:
  long SetCPU(uint32_t cpuindex){
    cpu_set_t cs;
    CPU_ZERO (&cs);
    CPU_SET (cpuindex, &cs);
    return sched_setaffinity (0, sizeof(cs), &cs);
  }

 protected:
  std::string file_name;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::vector<int> row_group_indices;
  std::vector<int> column_indices;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
  parquet::ArrowReaderProperties properties;

};

BENCHMARK_DEFINE_F(GoogleBenchmarkColumnarToRow, CacheScan)(benchmark::State& state){

  SetCPU(state.thread_index());

   arrow::Compression::type compression_type = (arrow::Compression::type) state.range(1);

    std::shared_ptr<arrow::RecordBatch> record_batch;
    int64_t elapse_read = 0;
    int64_t num_batches = 0;
    int64_t num_rows = 0;
    int64_t init_time = 0;
    int64_t write_time = 0;

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;
    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file),
        properties, &parquet_reader));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    ASSERT_NOT_OK(parquet_reader->GetRecordBatchReader(row_group_indices, column_indices,
                                                  &record_batch_reader));
    do{
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      
      if (record_batch) {
        batches.push_back(record_batch);
        num_batches += 1;
        num_rows += record_batch->num_rows();
      }
    } while (record_batch);
    

    
    for(auto _: state)
    {
      for (const auto& batch : batches) {
        std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter = 
          std::make_shared<ColumnarToRowConverter>(batch, arrow::default_memory_pool());
        TIME_NANO_OR_THROW(init_time, columnarToRowConverter->Init());
        TIME_NANO_OR_THROW(write_time, columnarToRowConverter->Write());
      }
    }


    state.counters["rowgroups"] = benchmark::Counter(row_group_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] = benchmark::Counter(column_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] = benchmark::Counter(num_batches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] = benchmark::Counter(num_rows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] = benchmark::Counter(batch_buffer_size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
   
    state.counters["parquet_parse"] = benchmark::Counter(elapse_read, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["init_time"] = benchmark::Counter(init_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] = benchmark::Counter(write_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

}

BENCHMARK_DEFINE_F(GoogleBenchmarkColumnarToRow, IterateScan)(benchmark::State& state) {

  SetCPU(state.thread_index());

    int64_t elapse_read = 0;
    int64_t num_batches = 0;
    int64_t num_rows = 0;
    int64_t init_time = 0;
    int64_t write_time = 0;

    std::shared_ptr<arrow::RecordBatch> record_batch;

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;
    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file),
        properties, &parquet_reader));

    for(auto _: state)
    {
      ASSERT_NOT_OK(parquet_reader->GetRecordBatchReader(row_group_indices, column_indices,
                                                  &record_batch_reader));
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      while (record_batch) {
        num_batches += 1;
        num_rows += record_batch->num_rows();
        std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter = 
          std::make_shared<ColumnarToRowConverter>(record_batch, arrow::default_memory_pool());
        TIME_NANO_OR_THROW(init_time, columnarToRowConverter->Init());
        TIME_NANO_OR_THROW(write_time, columnarToRowConverter->Write());
        TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      }
    }  

    state.counters["rowgroups"] = benchmark::Counter(row_group_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] = benchmark::Counter(column_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] = benchmark::Counter(num_batches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] = benchmark::Counter(num_rows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] = benchmark::Counter(batch_buffer_size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] = benchmark::Counter(elapse_read, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["init_time"] = benchmark::Counter(init_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] = benchmark::Counter(write_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

}

BENCHMARK_REGISTER_F(GoogleBenchmarkColumnarToRow, CacheScan)->Iterations(10)
      ->Args({96*16, arrow::Compression::FASTPFOR})
      ->Threads(1)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

}  // namespace columnartorow
}  // namespace sparkcolumnarplugin

BENCHMARK_MAIN();
