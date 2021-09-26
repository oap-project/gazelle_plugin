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

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>
#include <gandiva/projector.h>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstdint>
#include <vector>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/result_iterator.h"
#include "precompile/array.h"
#include "precompile/type_traits.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
static constexpr int32_t kIpcContinuationToken = -1;

// TODO: move this class into common utility
class SpillableCacheStore {
 public:
  SpillableCacheStore() {
    // need to config num_columns_ and result_schema_
    local_spill_dir_ = "sort_spill_" + GenerateUUID();
  }

  ~SpillableCacheStore() {
    for (auto& file_path : spill_file_list_) {
      std::remove(file_path.c_str());
    }
    std::remove(local_spill_dir_.c_str());
  }

  arrow::Status DoSpillAndMakeResultIterator(
      std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter, int64_t* spilled_size,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(DoSpill(iter, spilled_size));
    *out = std::make_shared<SpillableCacheReaderIterator>(spill_file_list_, num_batches_);
    return arrow::Status::OK();
  }

  arrow::Status DoSpill(std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter,
                        int64_t* spilled_size) {
    if (!iter->HasNextUnsafe()) {
      return arrow::Status::OK();
    }

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    std::string spilled_file = local_spill_dir_ + ".arrow";  // TODO(): get tmp dir
    auto spilled_file_os = *fs->OpenOutputStream(spilled_file);

    int64_t size = 0;
    int i = 0;
    bool first_batch = true;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;

    while (iter->HasNextUnsafe()) {
      int64_t single_call_spilled;
      std::shared_ptr<arrow::RecordBatch> batch;
      RETURN_NOT_OK(iter->NextUnsafe(&batch));
      if (first_batch) {
        writer = *arrow::ipc::MakeStreamWriter(spilled_file_os.get(), batch->schema());
        first_batch = false;
      }
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      num_batches_++;
    }
    RETURN_NOT_OK(spilled_file_os->Flush());
    RETURN_NOT_OK(writer->Close());
    spill_file_list_.push_back(spilled_file);
    SetSpilled(true);
    return arrow::Status::OK();
  }

  bool IsSpilled() { return is_spilled_; }

  void SetSpilled(bool state) { is_spilled_ = state; }

  std::string GenerateUUID() {
    boost::uuids::random_generator generator;
    return boost::uuids::to_string(generator());
  }

  std::string random_string(size_t length) {
    auto randchar = []() -> char {
      const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
      const size_t max_index = (sizeof(charset) - 1);
      return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str + ".arrow";
  }

  std::vector<arrow::ArrayVector> GetCache() {
    if (!cached_.empty()) return cached_;
    if (is_spilled_) {
      auto iter =
          std::make_shared<SpillableCacheReaderIterator>(spill_file_list_, num_batches_);
      while (iter->HasNext()) {
        std::shared_ptr<arrow::RecordBatch> out;
        iter->Next(&out);
        if (cached_.empty()) cached_.resize(out->num_columns());
        int i = 0;
        for (int i = 0; i < out->num_columns(); i++) {
          cached_[i].push_back(out->columns()[i]);
        }
      }
    }
    return cached_;
  }

  std::string& GetSpillDir() { return local_spill_dir_; }

  class SpillableCacheReaderIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SpillableCacheReaderIterator(std::vector<std::string> spill_file_list,
                                 const int& num_batches)
        : spill_file_list_(spill_file_list), total_num_batches_(num_batches) {
      if (!spill_file_list_.empty()) {
        auto file = *arrow::io::ReadableFile::Open(spill_file_list_[0]);
        file_reader_ = *arrow::ipc::RecordBatchStreamReader::Open(
            file, arrow::ipc::IpcReadOptions::Defaults());
      }
    }

    bool HasNext() override { return cur_idx_ < total_num_batches_; }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
      RETURN_NOT_OK(file_reader_->ReadNext(out));
      cur_idx_++;
      return arrow::Status::OK();
    }

   private:
    std::vector<std::string> spill_file_list_;
    int cur_idx_ = 0;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> file_reader_;
    const int& total_num_batches_ = 0;
  };

 private:
  std::shared_ptr<arrow::Schema> result_schema_;
  std::vector<std::string> spill_file_list_;
  bool is_spilled_ = false;
  std::string local_spill_dir_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::ipc::IpcWriteOptions options_;
  int num_batches_ = 0;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin