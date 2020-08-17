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
#include "shuffle/partitioning_jni_bridge.h"
#include "shuffle/utils.h"

namespace sparkcolumnarplugin {
namespace shuffle {

class Splitter {
 public:
  ~Splitter();

  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions, const gandiva::ExpressionVector& expr_vector);

  static arrow::Result<std::shared_ptr<Splitter>> Make(
      const std::string& short_name, std::shared_ptr<arrow::Schema> schema,
      int num_partitions);

  virtual const std::shared_ptr<arrow::Schema>& schema() const { return schema_; }

  virtual arrow::Status Split(const arrow::RecordBatch&) = 0;

  /***
   * Stop all writers created by this splitter. If the data buffer managed by the writer
   * is not empty, write to output stream as RecordBatch. Then sort the temporary files by
   * partition id.
   * @return
   */
  virtual arrow::Status Stop() = 0;

  int64_t TotalBytesWritten() const { return total_bytes_written_; }

  int64_t TotalWriteTime() const { return total_write_time_; }

  int64_t TotalComputePidTime() const { return total_compute_pid_time_; }

  virtual const std::vector<std::pair<int32_t, std::string>>& GetPartitionFileInfo()
      const {
    return partition_file_info_;
  }

  void set_compression_type(arrow::Compression::type compression_type) {
    compression_type_ = compression_type;
  }

  void set_buffer_size(int64_t buffer_size) { buffer_size_ = buffer_size; };

 protected:
  Splitter() = default;
  explicit Splitter(std::shared_ptr<arrow::Schema> schema) : schema_(std::move(schema)) {}

  std::shared_ptr<arrow::Schema> schema_;
  arrow::Compression::type compression_type_ = arrow::Compression::UNCOMPRESSED;
  int32_t buffer_size_ = kDefaultSplitterBufferSize;

  std::vector<std::pair<int32_t, std::string>> partition_file_info_;

  int64_t total_bytes_written_ = 0;
  int64_t total_write_time_ = 0;
  int64_t total_compute_pid_time_ = 0;
};

class BasePartitionSplitter : public Splitter {
 public:
  arrow::Status Split(const arrow::RecordBatch& rb) override;

  arrow::Status Stop() override;

 protected:
  BasePartitionSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema)
      : Splitter(std::move(schema)), num_partitions_(num_partitions) {}

  virtual arrow::Status Init();

  virtual arrow::Result<std::vector<int32_t>>
  GetNextBatchPartitionWriterIndex(const arrow::RecordBatch& rb) = 0;

  arrow::Status DoSplit(const arrow::RecordBatch& rb,
                        std::vector<int32_t> writer_idx);

  arrow::Result<std::string> CreateDataFile();

  const int32_t num_partitions_;

  std::vector<std::shared_ptr<PartitionWriter>> partition_writer_;

  // partition writer parameters
  Type::typeId last_type_id_ = Type::SHUFFLE_NOT_IMPLEMENTED;
  std::vector<Type::typeId> column_type_id_;

  // configured local dirs for temporary output file
  int32_t dir_selection_ = 0;
  std::vector<std::string> configured_dirs_;
};

class RoundRobinSplitter : public BasePartitionSplitter {
 public:
  static arrow::Result<std::shared_ptr<RoundRobinSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema);

 protected:
  arrow::Result<std::vector<int32_t>>
  GetNextBatchPartitionWriterIndex(const arrow::RecordBatch& rb) override;

 private:
  RoundRobinSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema)
      : BasePartitionSplitter(num_partitions, std::move(schema)) {}

  int32_t pid_selection_ = 0;
};

class HashSplitter : public BasePartitionSplitter {
 public:
  static arrow::Result<std::shared_ptr<HashSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema,
      const gandiva::ExpressionVector& expr_vector);

 private:
  HashSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema)
      : BasePartitionSplitter(num_partitions, std::move(schema)) {}

  arrow::Status CreateProjector(const gandiva::ExpressionVector& expr_vector);

  arrow::Result<std::vector<int32_t>>
  GetNextBatchPartitionWriterIndex(const arrow::RecordBatch& rb) override;

  std::shared_ptr<gandiva::Projector> projector_;
};

class FallbackRangeSplitter : public BasePartitionSplitter {
 public:
  static arrow::Result<std::shared_ptr<FallbackRangeSplitter>> Create(
      int32_t num_partitions, std::shared_ptr<arrow::Schema> schema);

  arrow::Status Split(const arrow::RecordBatch& rb) override;

  const std::shared_ptr<arrow::Schema>& schema() const override { return input_schema_; }

 private:
  FallbackRangeSplitter(int32_t num_partitions, std::shared_ptr<arrow::Schema> schema)
      : BasePartitionSplitter(num_partitions, std::move(schema)) {}

  arrow::Status Init() override;

  arrow::Result<std::vector<int32_t>>
  GetNextBatchPartitionWriterIndex(const arrow::RecordBatch& rb) override;

  std::shared_ptr<arrow::Schema> input_schema_;
};

}  // namespace shuffle
}  // namespace sparkcolumnarplugin
