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
    *out = std::make_shared<SpillableCacheReaderIterator>(spill_file_list_);
    return arrow::Status::OK();
  }

  arrow::Status DoSpill(std::shared_ptr<ResultIterator<arrow::RecordBatch>> iter,
                        int64_t* spilled_size) {
    if (!iter->HasNext()) {
      return arrow::Status::OK();
    }

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_ASSIGN_OR_RAISE(auto path_info, fs->GetFileInfo(local_spill_dir_));
    if (path_info.type() == arrow::fs::FileType::NotFound) {
      RETURN_NOT_OK(fs->CreateDir(local_spill_dir_, true));
    }

    int64_t size = 0;
    int i = 0;
    while (iter->HasNext()) {
      int64_t single_call_spilled;
      std::shared_ptr<arrow::RecordBatch> batch;
      RETURN_NOT_OK(iter->Next(&batch));
      RETURN_NOT_OK(SpillData(i++, batch, &single_call_spilled));
      *spilled_size += single_call_spilled;
    }
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

  arrow::Status SpillData(int64_t idx, std::shared_ptr<arrow::RecordBatch> batch,
                          int64_t* spilled_size) {
    arrow::ipc::IpcPayload payload;
    arrow::ipc::IpcWriteOptions options_;
    if (!result_schema_) result_schema_ = batch->schema();

    arrow::ipc::DictionaryFieldMapper dict_file_mapper;  // unused
    std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;
    std::string spilled_file_ =
        local_spill_dir_ + "/" + std::to_string(idx) + ".arrow";  // TODO(): get tmp dir

    ARROW_ASSIGN_OR_RAISE(spilled_file_os_,
                          arrow::io::FileOutputStream::Open(spilled_file_, true));
    int32_t metadata_length = -1;
    auto schema_payload_ = std::make_shared<arrow::ipc::IpcPayload>();
    RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(*result_schema_.get(), options_,
                                               dict_file_mapper, schema_payload_.get()));
    ARROW_RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *schema_payload_.get(), options_, spilled_file_os_.get(), &metadata_length));
    int32_t data_length = -1;
    arrow::ipc::GetRecordBatchPayload(*batch, options_, &payload);
    ARROW_RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        payload, options_, spilled_file_os_.get(), &data_length));
    *spilled_size = data_length;
    spill_file_list_.push_back(spilled_file_);
    return arrow::Status::OK();
  }

  std::vector<arrow::ArrayVector> GetCache() {
    if (!cached_.empty()) return cached_;
    if (is_spilled_) {
      auto iter = std::make_shared<SpillableCacheReaderIterator>(spill_file_list_);
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
    SpillableCacheReaderIterator(std::vector<std::string> spill_file_list)
        : spill_file_list_(spill_file_list) {}

    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>>
    GetRecordBatchStreamReader(const std::string& file_name) {
      ARROW_ASSIGN_OR_RAISE(auto file_, arrow::io::ReadableFile::Open(file_name))
      ARROW_ASSIGN_OR_RAISE(auto file_reader,
                            arrow::ipc::RecordBatchStreamReader::Open(file_));
      return file_reader;
    }

    bool HasNext() override { return cur_file_idx_ < spill_file_list_.size(); }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
#ifdef DEBUG
      std::cout << "read from " << spill_file_list_[cur_file_idx_] << std::endl;
#endif
      ARROW_ASSIGN_OR_RAISE(auto file_reader, GetRecordBatchStreamReader(
                                                  spill_file_list_[cur_file_idx_++]));

      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      RETURN_NOT_OK(file_reader->ReadAll(&batches));
      *out = batches[0];
      return arrow::Status::OK();
    }

   private:
    std::vector<std::string> spill_file_list_;
    int cur_file_idx_ = 0;
  };

 private:
  std::shared_ptr<arrow::Schema> result_schema_;
  std::vector<std::string> spill_file_list_;
  bool is_spilled_ = false;
  std::string local_spill_dir_;
  std::vector<arrow::ArrayVector> cached_;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin