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

#include <random>
#include <utility>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/record_batch.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/projector.h>

#include "shuffle/partition_writer.h"
#include "shuffle/utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

struct SplitOptions {
  int32_t buffer_size = kDefaultSplitterBufferSize;
  int32_t num_sub_dirs = kDefaultNumSubDirs;
  arrow::Compression::type compression_type = arrow::Compression::UNCOMPRESSED;

  std::string data_file;

  static SplitOptions Defaults();
};

class Splitter {
 public:
  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions, const gandiva::ExpressionVector& expr_vector,
      SplitOptions options = SplitOptions::Defaults());

  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions, SplitOptions options = SplitOptions::Defaults());

  virtual const std::shared_ptr<arrow::Schema>& schema() const { return schema_; }

  virtual arrow::Status Split(const arrow::RecordBatch&);

  /***
   * Stop all writers created by this splitter. If the data buffer managed by the writer
   * is not empty, write to output stream as RecordBatch. Then sort the temporary files by
   * partition id.
   * @return
   */
  arrow::Status Stop();

  int64_t TotalBytesWritten() const { return total_bytes_written_; }

  int64_t TotalBytesSpilled() const { return total_bytes_spilled_; }

  int64_t TotalWriteTime() const { return total_write_time_; }

  int64_t TotalSpillTime() const { return total_spill_time_; }

  int64_t TotalComputePidTime() const { return total_compute_pid_time_; }

  const std::vector<int64_t>& PartitionLengths() const { return partition_lengths_; }

  // for testing
  const std::string& DataFile() const { return options_.data_file; }

 protected:
  Splitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
           SplitOptions options)
      : num_partitions_(num_partitions),
        schema_(std::move(schema)),
        options_(std::move(options)) {}

  virtual arrow::Status Init();

  virtual arrow::Result<std::vector<int32_t>> GetNextBatchPartitionWriterIndex(
      const arrow::RecordBatch& rb) = 0;

  arrow::Status DoSplit(const arrow::RecordBatch& rb,
                        const std::vector<int32_t>& writer_idx);

  arrow::Status CreatePartitionWriter(int32_t partition_id);

  int32_t num_partitions_;
  std::shared_ptr<arrow::Schema> schema_;
  SplitOptions options_;

  int64_t total_bytes_written_ = 0;
  int64_t total_bytes_spilled_= 0;
  int64_t total_write_time_ = 0;
  int64_t total_spill_time_ = 0;
  int64_t total_compute_pid_time_ = 0;
  std::vector<int64_t> partition_lengths_;

  // partition writer and parameters
  std::vector<std::shared_ptr<PartitionWriter>> partition_writer_;
  Type::typeId last_type_id_ = Type::SHUFFLE_NOT_IMPLEMENTED;
  std::vector<Type::typeId> column_type_id_;

  // configured local dirs for spilled file
  int32_t dir_selection_ = 0;
  std::vector<int32_t> sub_dir_selection_;
  std::vector<std::string> configured_dirs_;

  std::shared_ptr<arrow::io::FileOutputStream> data_file_os_;
};

class RoundRobinSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<RoundRobinSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      SplitOptions options);

 protected:
  arrow::Result<std::vector<int32_t>> GetNextBatchPartitionWriterIndex(
      const arrow::RecordBatch& rb) override;

 private:
  RoundRobinSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
                     SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  int32_t pid_selection_ = 0;
};

class HashSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<HashSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      const gandiva::ExpressionVector& expr_vector, SplitOptions options);

 private:
  HashSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
               SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  arrow::Status CreateProjector(const gandiva::ExpressionVector& expr_vector);

  arrow::Result<std::vector<int32_t>> GetNextBatchPartitionWriterIndex(
      const arrow::RecordBatch& rb) override;

  std::shared_ptr<gandiva::Projector> projector_;
};

class FallbackRangeSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<FallbackRangeSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      SplitOptions options);

  arrow::Status Split(const arrow::RecordBatch& rb) override;

  const std::shared_ptr<arrow::Schema>& schema() const override { return input_schema_; }

 private:
  FallbackRangeSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
                        SplitOptions options)
      : Splitter(num_partitions, std::move(schema), std::move(options)) {}

  arrow::Status Init() override;

  arrow::Result<std::vector<int32_t>> GetNextBatchPartitionWriterIndex(
      const arrow::RecordBatch& rb) override;

  std::shared_ptr<arrow::Schema> input_schema_;
};

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
