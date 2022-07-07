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
#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bitmap_ops.h>
#include <gandiva/projector.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <thread>
#include <vector>

#include "array_appender.h"
#include "array_comparator.h"
#include "array_list_iterator.h"
#include "array_taker.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/arrow_compute/ext/spillable_cache_store.h"
#include "codegen/arrow_compute/ext/typed_node_visitor.h"
#include "precompile/array.h"
#include "precompile/type.h"
#include "precompile/type_traits.h"
#include "third_party/ska_sort.hpp"
#include "third_party/timsort.hpp"
#include "utils/macros.h"

/**
                 The Overall Implementation of Sort Kernel
 * In general, there are four kenels to use when sorting for different data.
   SortArraysToIndicesKernel::Impl is the base class, and other three kernels,
including SortInplaceKernel, SortOnekeyKernel and SortMultiplekeyKernel, extend
it.
 * Usage:
   SortInplaceKernel is used when sorting for one non-string and non-bool col
without payload. SortOnekeyKernel is used when sorting for single key with
payload, and one string or bool col without payload. SortMultiplekeyKernel is
used when sorting for multiple keys and codegen is disabled.
   SortArraysCodegenKernel is used when sorting for multiple keys and
codegen is enabled.
 * In these kernels, usually ska_sort is used for asc direciton, and std sort is
used for desc direciton. Timsort is used in multiple-key sort.
 * Before sorting, projection and partition can be conducted.
   If projection is required, it is completed before sorting, and the projected
cols are used to do comparison. If partition is required, null, NaN(for double
and float only) and valid value are partitioned before sorting.
   FIXME: 1. Projection in SortInplaceKernel will change the original data,
causing it cannot be used. 2. datatype change after projection is not supported in
SortInplaceKernel.
**/

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using namespace sparkcolumnarplugin::precompile;
using namespace std::chrono_literals;

template <typename T>
using is_number_bool_date = std::integral_constant<
    bool, arrow::is_number_type<T>::value || arrow::is_boolean_type<T>::value ||
              arrow::is_date_type<T>::value || arrow::is_timestamp_type<T>::value>;

///////////////  SortArraysToIndices  ////////////////
class SortArraysToIndicesKernel::Impl {
 public:
  Impl() {
#ifdef DEBUG
    std::cout << "use SortArraysToIndicesKernel::Impl" << std::endl;
#endif
    ctx_ = nullptr;
  }

  Impl(arrow::compute::ExecContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "use SortArraysToIndicesKernel::Impl" << std::endl;
#endif
  }

  virtual ~Impl() {}

  virtual arrow::Status Evaluate(ArrayList& in) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual arrow::Status Spill(int64_t size, int64_t* spilled_size) {
#ifdef DEBUG
    auto before = arrow::default_memory_pool()->bytes_allocated();
#endif
    auto status = SortAndSpill(spilled_size);

#ifdef DEBUG
    auto after = arrow::default_memory_pool()->bytes_allocated();
    auto gap = after - before;
    std::cout << this << " SortKernel Spilled " << *spilled_size
              << " bytes due to manual spill trigger, current memory is " << after
              << " memory released from arrow memory_pool is " << gap << std::endl;
#endif
    return status;
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<SortRelation>>* out) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<SortRelation>>* out) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual std::string GetSignature() { return ""; }

  virtual arrow::Status MaybeSpill() {
    if (GetCurrentMemoryThreshold() > 0 &&
        GetCurrentMemoryUsage() < GetCurrentMemoryThreshold()) {
#ifdef DEBUG
      std::cout << "CurrentMemoryUsage is " << GetCurrentMemoryUsage()
                << ", MemoryThreshold is " << GetCurrentMemoryThreshold()
                << ", won't spill" << std::endl;
#endif
      return arrow::Status::OK();
    } else {
#ifdef DEBUG
      std::cout << "CurrentMemoryUsage is " << GetCurrentMemoryUsage()
                << ", MemoryThreshold is " << GetCurrentMemoryThreshold() << ", do spill"
                << std::endl;
#endif
    }
#ifdef DEBUG
    auto before = arrow::default_memory_pool()->bytes_allocated();
#endif
    int64_t spilled_size;
    auto status = SortAndSpill(&spilled_size);

#ifdef DEBUG
    auto after = arrow::default_memory_pool()->bytes_allocated();
    auto gap = after - before;
    std::cout << this << " SortKernel Spilled " << spilled_size
              << " bytes upon external sort threshold, current memory is " << after
              << ", memory released from arrow memory_pool is " << gap << std::endl;
#endif
    return status;
  }

  virtual arrow::Status SortAndSpill(int64_t* spilled_size) {
    return arrow::Status::NotImplemented(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual uint64_t GetCurrentMemoryUsage() {
    throw std::runtime_error(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  virtual uint64_t GetCurrentMemoryThreshold() {
    throw std::runtime_error(
        "This function is not supported in SortArraysToIndicesKernel::Impl.");
  }

  class SortRelationResultIterator : public ResultIterator<SortRelation> {
   public:
    SortRelationResultIterator(std::shared_ptr<SortRelation> sort_relation)
        : sort_relation_(sort_relation) {}
    arrow::Status Next(std::shared_ptr<SortRelation>* out) {
      *out = sort_relation_;
      return arrow::Status::OK();
    }

   private:
    std::shared_ptr<SortRelation> sort_relation_;
  };

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::ExecContext* ctx,
                         std::shared_ptr<arrow::Schema> schema,
                         std::shared_ptr<FixedSizeBinaryArray> indices_in,
                         std::vector<arrow::ArrayVector> cached)
        : ctx_(ctx),
          schema_(schema),
          indices_in_cache_(indices_in),
          total_length_(indices_in->length()) {
      col_num_ = schema->num_fields();
      if (col_num_ > cached.size()) {
        throw std::runtime_error(
            "SorterResultIterator received cache with " + std::to_string(cached.size()) +
            ", which does not match expected num_cols of " + std::to_string(col_num_));
      }
      indices_begin_ = (ArrayItemIndexS*)indices_in->value_data();
      for (int i = 0; i < col_num_; i++) {
        auto field = schema->field(i);
        std::shared_ptr<TakerBase> taker;
        THROW_NOT_OK(MakeArrayTaker(ctx_, field->type(), &taker));
        taker_list_.push_back(taker);
      }

      for (int i = 0; i < col_num_; i++) {
        int array_num = cached[i].size();
        for (int array_id = 0; array_id < array_num; array_id++) {
          taker_list_[i]->AddArray(cached[i][array_id]);
        }
      }
      batch_size_ = GetBatchSize();
    }

    ~SorterResultIterator() {}

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      thread_lck_.lock();
      auto status = HasNextUnsafe();
      thread_lck_.unlock();
      return status;
    }

    bool HasNextUnsafe() override {
      if (ext_sort_result_iter_) return ext_sort_result_iter_->HasNext();
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
      thread_lck_.lock();
      auto status = NextUnsafe(out);
      thread_lck_.unlock();
      return status;
    }

    arrow::Status NextUnsafe(std::shared_ptr<arrow::RecordBatch>* out) override {
      if (ext_sort_result_iter_) return ext_sort_result_iter_->Next(out);
      auto length = (total_length_ - offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - offset_);
      ArrayList arrays;
      for (int i = 0; i < col_num_; i++) {
        std::shared_ptr<arrow::Array> out_array;
        taker_list_[i]->TakeFromIndices(indices_begin_ + offset_, length, &out_array);
        arrays.push_back(out_array);
      }

      offset_ += length;
      *out = arrow::RecordBatch::Make(schema_, length, arrays);
      return arrow::Status::OK();
    }

    arrow::Status ClearCache() {
      indices_in_cache_.reset();
      indices_begin_ = nullptr;
      taker_list_.clear();
      return arrow::Status::OK();
    }

    arrow::Status StartLock() {
      /*if (thread_lck_.try_lock_for(30s)) {
        return arrow::Status::OK();
      } else {
        return arrow::Status::Cancelled("Unable to get lock in 30s");
      }*/
      thread_lck_.lock();
      return arrow::Status::OK();
    }

    arrow::Status EndLock() {
      thread_lck_.unlock();
      return arrow::Status::OK();
    }

    arrow::Status SwitchToExternalSorterResultIterator(
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter) {
      ext_sort_result_iter_ = ext_sort_result_iter;
      RETURN_NOT_OK(ClearCache());
      return arrow::Status::OK();
    }

    bool IsSpilled() { return ext_sort_result_iter_ ? true : false; }

   private:
    std::timed_mutex
        thread_lck_;  // since there maybe two threads will access this ResultIter,
                      // one is regular task thread, one is spill thread
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    std::shared_ptr<arrow::Schema> schema_;
    arrow::compute::ExecContext* ctx_;
    uint64_t batch_size_;
    int col_num_;
    ArrayItemIndexS* indices_begin_;
    std::vector<std::shared_ptr<arrow::DataType>> type_list_;
    std::vector<std::shared_ptr<TakerBase>> taker_list_;
    std::shared_ptr<FixedSizeBinaryArray> indices_in_cache_;
    bool is_spilled_ = false;
    int64_t spilled_size_ = 0;
    std::shared_ptr<SpillableCacheStore> spillablecachestore_;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter_;
  };

  class ExternalSorter {
   public:
    ExternalSorter(arrow::compute::ExecContext* ctx,
                   std::vector<std::shared_ptr<ComparatorBase>> comparator_v,
                   std::shared_ptr<ComparatorKeyProjector> comparator_key_projector)
        : ctx_(ctx),
          comparator_v_(comparator_v),
          comparator_key_projector_(comparator_key_projector) {}
    arrow::Status Spill(std::shared_ptr<ResultIterator<arrow::RecordBatch>> out) {
      RETURN_NOT_OK(Enqueue(out));
      return arrow::Status::OK();
    }

    arrow::Status Enqueue(std::shared_ptr<ResultIterator<arrow::RecordBatch>> out) {
      auto spillable_sort_iter = std::make_shared<SpillableSorterReaderIterator>(
          ctx_, out, comparator_v_, comparator_key_projector_);
      if (spillable_sort_iter->HasNext()) {
        sorted_iter_heap_.push(spillable_sort_iter);
      }
      return arrow::Status::OK();
    }

    arrow::Status MakeResultIterator(
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
      *out =
          std::make_shared<ExternalSorterResultIterator>(ctx_, schema, sorted_iter_heap_);
      return arrow::Status::OK();
    }

    class SpillableSorterReaderIterator {
     public:
      SpillableSorterReaderIterator(
          arrow::compute::ExecContext* ctx,
          std::shared_ptr<ResultIterator<arrow::RecordBatch>> sorted_iter,
          std::vector<std::shared_ptr<ComparatorBase>> comparator_v,
          std::shared_ptr<ComparatorKeyProjector> comparator_key_projector)
          : ctx_(ctx),
            comparator_v_(comparator_v),
            comparator_key_projector_(comparator_key_projector) {
        spillable_cache_store_ = std::make_shared<SpillableCacheStore>();
        auto status = spillable_cache_store_->DoSpillAndMakeResultIterator(
            sorted_iter, &spilled_size_, &sorted_iter_);
        if (!status.ok()) {
          throw std::runtime_error(
              "SpillableSorterReaderIterator DoSpillAndMakeResultIterator failed on "
              "error, " +
              status.message());
        }
      }
      class SpillableSorterReaderIteratorCompare {
       public:
        bool operator()(std::shared_ptr<SpillableSorterReaderIterator>& lhs,
                        std::shared_ptr<SpillableSorterReaderIterator>& rhs) {
          for (auto comp : lhs->comparator_v_) {
            auto res = comp->Compare(lhs->current_cached_batch_in_comparator_idx_,
                                     rhs->current_cached_batch_in_comparator_idx_,
                                     lhs->cache_offset_, rhs->cache_offset_);
// In comparison, 1 represents for true, 0 for false, and 2 for equal.
#ifdef DEBUG
            std::cout << "compare res left:"
                      << lhs->current_cached_batch_in_comparator_idx_ << "_"
                      << lhs->cache_offset_ << (res > 0 ? " <=" : " >")
                      << " right:" << rhs->current_cached_batch_in_comparator_idx_ << "_"
                      << rhs->cache_offset_ << std::endl;
#endif
            if (res != 2) return res == 0;
          }
          return false;
        }
      };

      bool HasNext() {
        if (!sorted_iter_->HasNext()) {
          if (!current_in_cache_batch_) return false;
          if (cache_offset_ >= current_in_cache_batch_->num_rows()) return false;
          // remaining case are for last batch, still return true
        } else {
          // not last batch
          if (!current_in_cache_batch_ ||
              cache_offset_ >= current_in_cache_batch_->num_rows()) {
            // load next batch
            auto status = sorted_iter_->Next(&current_in_cache_batch_);
            if (!status.ok()) {
              throw std::runtime_error(
                  "SpillableSorterReaderIterator load Next failed, " + status.message());
              return false;
            } else {
              // we load a new batch
              cache_offset_ = 0;
              THROW_NOT_OK(UpdateComparator());
            }
          }
        }
        return true;
      }

      arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
        // This api is used when only one item in Heap
        if (cache_offset_ == 0) {
          *out = current_in_cache_batch_;
          cache_offset_ += current_in_cache_batch_->num_rows();
        } else {
          // copy to a new batch of remaining data
          auto cur_size = current_in_cache_batch_->num_rows() - cache_offset_;
          auto schema = current_in_cache_batch_->schema();
          std::vector<std::shared_ptr<arrow::Array>> out_arrs;
          for (auto arr : current_in_cache_batch_->columns()) {
            std::shared_ptr<arrow::Array> copied;
            auto sliced = arr->Slice(cache_offset_);
            RETURN_NOT_OK(arrow::Concatenate({sliced}, ctx_->memory_pool(), &copied));
            out_arrs.push_back(copied);
          }
          *out = arrow::RecordBatch::Make(schema, cur_size, out_arrs);
          cache_offset_ += cur_size;
        }
        return arrow::Status::OK();
      }

      arrow::Status UpdateComparator() {
        // we need to release original cached batch and loaded new batch to appender
        // cache
        int num_comparator = comparator_key_projector_->num_columns_;
        arrow::ArrayVector key_columns;
        if (comparator_key_projector_->need_projection_) {
          // do projection here, and the projected arrays are used for comparison
          RETURN_NOT_OK(comparator_key_projector_->key_projector_->Evaluate(
              *current_in_cache_batch_, ctx_->memory_pool(), &key_columns));
        } else {
          for (auto key_idx : comparator_key_projector_->key_idx_list_) {
            key_columns.push_back(current_in_cache_batch_->column(key_idx));
          }
        }
        if (comp_first_batch_) {
          comp_first_batch_ = false;
          for (int i = 0; i < num_comparator; i++) {
            RETURN_NOT_OK(comparator_v_[i]->AddArray(key_columns[i]));
          }
        } else {
          for (int i = 0; i < num_comparator; i++) {
            RETURN_NOT_OK(
                comparator_v_[i]->ReleaseArray(current_cached_batch_in_comparator_idx_));
            RETURN_NOT_OK(comparator_v_[i]->AddArray(key_columns[i]));
          }
        }
        current_cached_batch_in_comparator_idx_ = comparator_v_[0]->GetCacheSize() - 1;

        return arrow::Status::OK();
      }

      arrow::Status UpdateAppender(
          std::vector<std::shared_ptr<AppenderBase>> appender_list) {
        if (cache_offset_ > 0) return arrow::Status::OK();
        // we need to release original cached batch and loaded new batch to appender
        // cache
        if (appender_first_batch_) {
          appender_first_batch_ = false;
          for (int i = 0; i < current_in_cache_batch_->num_columns(); i++) {
            RETURN_NOT_OK(
                (appender_list)[i]->AddArray(current_in_cache_batch_->column(i)));
          }
        } else {
          for (int i = 0; i < current_in_cache_batch_->num_columns(); i++) {
            RETURN_NOT_OK(
                (appender_list)[i]->ReleaseArray(current_cached_batch_in_appender_idx_));
            RETURN_NOT_OK(
                (appender_list)[i]->AddArray(current_in_cache_batch_->column(i)));
          }
        }
        current_cached_batch_in_appender_idx_ = appender_list[0]->GetCacheSize() - 1;

        return arrow::Status::OK();
      }

      arrow::Status AppendRow(std::vector<std::shared_ptr<AppenderBase>> appender_list) {
        // we assumed user will call hasNext firstly, so now we have
        // current_in_cache_batch_ on hold
        if (appender_list.size() == 0) return arrow::Status::OK();
        RETURN_NOT_OK(UpdateAppender(appender_list));
#ifdef DEBUG
        std::cout << "SpillableSorterReaderIterator AppendRow for "
                  << appender_list.size() << " columns, append index is "
                  << current_cached_batch_in_comparator_idx_ << "_" << cache_offset_
                  << std::endl;
#endif
        for (auto appender : appender_list) {
          RETURN_NOT_OK(
              appender->Append(current_cached_batch_in_appender_idx_, cache_offset_));
        }
        cache_offset_++;
        return arrow::Status::OK();
      }

     protected:
      arrow::compute::ExecContext* ctx_;
      std::shared_ptr<ResultIterator<arrow::RecordBatch>> sorted_iter_;
      std::shared_ptr<arrow::RecordBatch> current_in_cache_batch_;
      // This is a trick for Appender to appender data without redudant dynamic type
      // convert.
      bool comp_first_batch_ = true;
      bool appender_first_batch_ = true;
      uint16_t current_cached_batch_in_appender_idx_ = 0;
      uint16_t current_cached_batch_in_comparator_idx_ = 0;
      uint16_t cache_offset_ = 0;
      std::vector<std::shared_ptr<ComparatorBase>> comparator_v_;
      std::shared_ptr<SpillableCacheStore> spillable_cache_store_;
      std::shared_ptr<ComparatorKeyProjector> comparator_key_projector_;
      int64_t spilled_size_;
    };

    class ExternalSorterResultIterator : public ResultIterator<arrow::RecordBatch> {
     public:
      ExternalSorterResultIterator(
          arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::Schema> schema,
          std::priority_queue<
              std::shared_ptr<SpillableSorterReaderIterator>,
              std::vector<std::shared_ptr<SpillableSorterReaderIterator>>,
              SpillableSorterReaderIterator::SpillableSorterReaderIteratorCompare>
              sorted_iter_heap)
          : ctx_(ctx), schema_(schema), sorted_iter_heap_(sorted_iter_heap) {
        for (auto field : schema->fields()) {
          std::shared_ptr<AppenderBase> appender;
          THROW_NOT_OK(MakeAppender(ctx_, field->type(), AppenderBase::left, &appender));
          appender_list_.push_back(appender);
        }
        expected_batch_size_ = GetBatchSize();
      }

      bool HasNext() override {
#ifdef DEBUG
        std::cout << "ExternalSorterResultIterator heap size is "
                  << sorted_iter_heap_.size() << std::endl;
#endif
        return !sorted_iter_heap_.empty();
      }

      arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
        // if there is only one item in heap
        if (sorted_iter_heap_.size() == 1) {
          auto min_iter = sorted_iter_heap_.top();
          RETURN_NOT_OK(min_iter->Next(out));
          if (!min_iter->HasNext()) {
            sorted_iter_heap_.pop();
          }
          return arrow::Status::OK();
        }

        int64_t cur_size = 0;
        while (cur_size < expected_batch_size_ && !sorted_iter_heap_.empty()) {
          auto min_iter = sorted_iter_heap_.top();
          sorted_iter_heap_.pop();
          RETURN_NOT_OK(min_iter->AppendRow(appender_list_));
          cur_size += 1;
          if (min_iter->HasNext()) {
            sorted_iter_heap_.push(min_iter);
          }
        }
        ArrayList out_arrs;
        int arr_idx = 0;
        for (auto appender : appender_list_) {
          std::shared_ptr<arrow::Array> out;
          RETURN_NOT_OK(appender->Finish(&out));
          RETURN_NOT_OK(appender->Reset());
          out_arrs.push_back(out);
        }
#ifdef DEBUG
        std::cout << "ExternalSorterResultIterator result length is " << cur_size
                  << std::endl;
#endif
        *out = arrow::RecordBatch::Make(schema_, cur_size, out_arrs);
        return arrow::Status::OK();
      }

     private:
      std::shared_ptr<arrow::Schema> schema_;
      arrow::compute::ExecContext* ctx_;
      std::priority_queue<
          std::shared_ptr<SpillableSorterReaderIterator>,
          std::vector<std::shared_ptr<SpillableSorterReaderIterator>>,
          SpillableSorterReaderIterator::SpillableSorterReaderIteratorCompare>
          sorted_iter_heap_;
      std::vector<std::shared_ptr<AppenderBase>> appender_list_;
      int64_t expected_batch_size_;
    };

   private:
    arrow::compute::ExecContext* ctx_;
    std::shared_ptr<arrow::Schema> schema_;
    std::priority_queue<
        std::shared_ptr<SpillableSorterReaderIterator>,
        std::vector<std::shared_ptr<SpillableSorterReaderIterator>>,
        SpillableSorterReaderIterator::SpillableSorterReaderIteratorCompare>
        sorted_iter_heap_;
    std::vector<std::shared_ptr<ComparatorBase>> comparator_v_;
    std::shared_ptr<ComparatorKeyProjector> comparator_key_projector_;
  };

 protected:
  arrow::compute::ExecContext* ctx_;
};

///////////////  SortArraysInPlace  ////////////////
template <typename DATATYPE, typename CTYPE>
class SortInplaceKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortInplaceKernel(arrow::compute::ExecContext* ctx,
                    std::shared_ptr<arrow::Schema> result_schema,
                    std::shared_ptr<gandiva::Projector> key_projector,
                    std::vector<bool> sort_directions, std::vector<bool> nulls_order,
                    bool NaN_check)
      : ctx_(ctx),
        nulls_first_(nulls_order[0]),
        asc_(sort_directions[0]),
        result_schema_(result_schema),
        key_projector_(key_projector),
        NaN_check_(NaN_check) {
    memory_threshold_ = GetMemoryThreshold();
  }

  arrow::Status Evaluate(ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_0_.push_back(outputs[0]);
      nulls_total_ += outputs[0]->null_count();
      cache_size_total_ += GetArrayVectorSize(outputs);
    } else {
      cached_0_.push_back(in[0]);
      nulls_total_ += in[0]->null_count();
      cache_size_total_ += GetArrayVectorSize(in);
    }

    items_total_ += in[0]->length();
    return arrow::Status::OK();
  }

  arrow::Status SortAndSpill(int64_t* spilled_size) override {
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
    if (final_result_iter_ && external_sorter_) {
      // external sort is enabled and still need extra space, nothing can be done here
      *spilled_size = 0;
      return arrow::Status::OK();
    } else if (final_result_iter_) {
      // sort has reached final phase, we don't need to create result iter
      out = final_result_iter_;
    } else {
      // sort is in evaluate phase, we should create result Iter for current in memory
      // data
      RETURN_NOT_OK(MakeResultIterator(result_schema_, &out));
    }
    *spilled_size = GetCurrentMemoryUsage();
    auto typed_final_result_iter = std::dynamic_pointer_cast<SorterResultIterator>(out);
    auto lock_status = typed_final_result_iter->StartLock();
    if (!lock_status.ok()) {
      *spilled_size = 0;
      return arrow::Status::OK();
    }

    if (!external_sorter_) {
      std::shared_ptr<ComparatorBase> comp;
      RETURN_NOT_OK(MakeComparator<DATATYPE>(asc_, nulls_first_, NaN_check_, &comp));
      std::vector<std::shared_ptr<ComparatorBase>> comp_v = {comp};
      auto key_proj =
          key_projector_
              ? std::make_shared<ComparatorKeyProjector>(1, key_projector_)
              : std::make_shared<ComparatorKeyProjector>(std::vector<int>({0}));
      external_sorter_ = std::make_shared<ExternalSorter>(ctx_, comp_v, key_proj);
    }

    if (typed_final_result_iter->HasNextUnsafe()) {
      RETURN_NOT_OK(external_sorter_->Spill(out));
      if (final_result_iter_) {
        // update final_result_iter_ to use external result iterator
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter;
        RETURN_NOT_OK(
            external_sorter_->MakeResultIterator(result_schema_, &ext_sort_result_iter));
        RETURN_NOT_OK(typed_final_result_iter->SwitchToExternalSorterResultIterator(
            ext_sort_result_iter));
      }
    } else {
      *spilled_size = 0;
    }
    typed_final_result_iter->EndLock();
    RETURN_NOT_OK(ClearCache());
#ifdef DEBUG
    std::cout << "Spilled called, " << *spilled_size << " released" << std::endl;
#endif
    return arrow::Status::OK();
  }

  arrow::Status ClearCache() {
    cache_size_total_ = 0;
    nulls_total_ = 0;
    items_total_ = 0;
    cached_0_.clear();
    return arrow::Status::OK();
  }

  uint64_t GetCurrentMemoryUsage() override { return cache_size_total_; }

  uint64_t GetCurrentMemoryThreshold() override { return memory_threshold_; }

  arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
#ifdef DEBUG
    std::cout << "MergeSpilledAndMakeResultIterator "
              << (external_sorter_ ? "merge with external sorter"
                                   : "no spilled data to merge")
              << std::endl;
#endif
    RETURN_NOT_OK(MakeResultIterator(schema, out));
    final_result_iter_ = *out;
    if (external_sorter_) {
      if (*out) {
        RETURN_NOT_OK(external_sorter_->Enqueue(*out));
      }
      RETURN_NOT_OK(external_sorter_->MakeResultIterator(schema, out));
    }
    return arrow::Status::OK();
  }

  // This function is used for float/double data without null value.
  // If NaN_check_ is true, we need to do partition for NaN before sort.
  template <typename TYPE>
  auto SortNoNull(TYPE* indices_begin, TYPE* indices_end) ->
      typename std::enable_if_t<std::is_floating_point<TYPE>::value> {
    if (asc_) {
      auto sort_end = indices_end;
      if (NaN_check_) {
        sort_end = std::partition(indices_begin, indices_end,
                                  [](TYPE i) { return !std::isnan(i); });
      }
      ska_sort(indices_begin, sort_end);
    } else {
      auto sort_begin = indices_begin;
      if (NaN_check_) {
        sort_begin = std::partition(indices_begin, indices_end,
                                    [](TYPE i) { return std::isnan(i); });
      }
      auto desc_comp = [this](TYPE& x, TYPE& y) { return x > y; };
      std::sort(sort_begin, indices_end, desc_comp);
    }
  }

  // This function is used for non-float and non-double data without null value.
  template <typename TYPE>
  auto SortNoNull(TYPE* indices_begin, TYPE* indices_end) ->
      typename std::enable_if_t<!std::is_floating_point<TYPE>::value &&
                                !std::is_same<TYPE, arrow::Decimal128>::value> {
    if (asc_) {
      ska_sort(indices_begin, indices_end);
    } else {
      auto desc_comp = [this](TYPE& x, TYPE& y) { return x > y; };
      std::sort(indices_begin, indices_end, desc_comp);
    }
  }

  // This function is used for non-float and non-double data without null value.
  template <typename TYPE>
  auto SortNoNull(TYPE* indices_begin, TYPE* indices_end) ->
      typename std::enable_if_t<std::is_same<TYPE, arrow::Decimal128>::value> {
    if (asc_) {
      auto comp = [this](TYPE& x, TYPE& y) { return x < y; };
      std::sort(indices_begin, indices_end, comp);
    } else {
      auto comp = [this](TYPE& x, TYPE& y) { return x > y; };
      std::sort(indices_begin, indices_end, comp);
    }
  }

  // This function is used for float/double data with null value.
  // We should do partition for null and NaN (if (NaN_check_ is true).
  template <typename TYPE, typename ArrayType>
  auto Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) ->
      typename std::enable_if_t<std::is_floating_point<TYPE>::value> {
    std::iota(indices_begin, indices_end, 0);
    auto sort_begin = indices_begin;
    auto sort_end = indices_end;

    if (asc_) {
      if (nulls_first_) {
        sort_begin = std::partition(indices_begin, indices_end, [&values](uint64_t ind) {
          return values.IsNull(ind);
        });
        if (NaN_check_) {
          // values should be sorted to:
          // null, null, ..., valid-1, valid-2, ..., valid-3, NaN, NaN, ...
          sort_end = std::partition(sort_begin, indices_end, [&values](uint64_t ind) {
            return !std::isnan(values.GetView(ind));
          });
        }
        ska_sort(sort_begin, sort_end,
                 [&values](auto& x) -> decltype(auto) { return values.GetView(x); });
      } else {
        sort_end = std::partition(indices_begin, indices_end, [&values](uint64_t ind) {
          return !values.IsNull(ind);
        });
        if (NaN_check_) {
          // values should be sorted to:
          // valid-1, valid-2, ..., valid-3, NaN, NaN, ..., null, null, ...
          sort_end = std::partition(indices_begin, sort_end, [&values](uint64_t ind) {
            return !std::isnan(values.GetView(ind));
          });
        }
        ska_sort(indices_begin, sort_end,
                 [&values](auto& x) -> decltype(auto) { return values.GetView(x); });
      }
    } else {
      auto comp = [&values](uint64_t left, uint64_t right) {
        return values.GetView(left) > values.GetView(right);
      };
      if (nulls_first_) {
        sort_begin = std::partition(indices_begin, indices_end, [&values](uint64_t ind) {
          return values.IsNull(ind);
        });
        if (NaN_check_) {
          // values should be sorted to:
          // null, null, NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ...
          sort_begin = std::partition(sort_begin, indices_end, [&values](uint64_t ind) {
            return std::isnan(values.GetView(ind));
          });
        }
        std::sort(sort_begin, indices_end, comp);
      } else {
        sort_end = std::partition(indices_begin, indices_end, [&values](uint64_t ind) {
          return !values.IsNull(ind);
        });
        if (NaN_check_) {
          // values should be sorted to:
          // NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ..., null, null, ...
          sort_begin = std::partition(indices_begin, sort_end, [&values](uint64_t ind) {
            return std::isnan(values.GetView(ind));
          });
        }
        std::sort(sort_begin, sort_end, comp);
      }
    }
  }

  // This function is used for non-float and non-double data with null value.
  // We should do partition for null.
  template <typename TYPE, typename ArrayType>
  auto Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) ->
      typename std::enable_if_t<!std::is_floating_point<TYPE>::value &&
                                !std::is_same<TYPE, arrow::Decimal128>::value> {
    std::iota(indices_begin, indices_end, 0);
    if (asc_) {
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        ska_sort(nulls_end, indices_end,
                 [&values](auto& x) -> decltype(auto) { return values.GetView(x); });
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        ska_sort(indices_begin, nulls_begin,
                 [&values](auto& x) -> decltype(auto) { return values.GetView(x); });
      }
    } else {
      auto comp = [&values](uint64_t left, uint64_t right) {
        return values.GetView(left) > values.GetView(right);
      };
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        std::sort(nulls_end, indices_end, comp);
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        std::sort(indices_begin, nulls_begin, comp);
      }
    }
  }

  template <typename TYPE, typename ArrayType>
  auto Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) ->
      typename std::enable_if_t<std::is_same<TYPE, arrow::Decimal128>::value> {
    std::iota(indices_begin, indices_end, 0);
    if (asc_) {
      auto comp = [&values](uint64_t left, uint64_t right) {
        return values.GetView(left) < values.GetView(right);
      };
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        std::sort(nulls_end, indices_end, comp);
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        std::sort(indices_begin, nulls_begin, comp);
      }
    } else {
      auto comp = [&values](uint64_t left, uint64_t right) {
        return values.GetView(left) > values.GetView(right);
      };
      if (nulls_first_) {
        auto nulls_end =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return values.IsNull(ind); });
        std::sort(nulls_end, indices_end, comp);
      } else {
        auto nulls_begin =
            std::partition(indices_begin, indices_end,
                           [&values](uint64_t ind) { return !values.IsNull(ind); });
        std::sort(indices_begin, nulls_begin, comp);
      }
    }
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    if (cached_0_.empty()) return arrow::Status::OK();
    RETURN_NOT_OK(
        arrow::Concatenate(cached_0_, ctx_->memory_pool(), &concatenated_array_));
    if (nulls_total_ == 0) {
      // Function SortNoNull is used.
      CTYPE* indices_begin = concatenated_array_->data()->GetMutableValues<CTYPE>(1);
      CTYPE* indices_end = indices_begin + concatenated_array_->length();

      SortNoNull<CTYPE>(indices_begin, indices_end);
      *out = std::make_shared<SorterResultIterator>(ctx_, schema, concatenated_array_,
                                                    nulls_first_, asc_);
    } else {
      // Function Sort is used.
      auto typed_array = std::make_shared<ArrayType_0>(concatenated_array_);
      std::shared_ptr<arrow::Array> indices_out;

      int64_t buf_size = typed_array->length() * sizeof(uint64_t);
      ARROW_ASSIGN_OR_RAISE(auto indices_buf,
                            AllocateBuffer(buf_size, ctx_->memory_pool()));
      int64_t* indices_begin = reinterpret_cast<int64_t*>(indices_buf->mutable_data());
      int64_t* indices_end = indices_begin + typed_array->length();

      Sort<CTYPE, ArrayType_0>(indices_begin, indices_end, *typed_array.get());
      indices_out = std::make_shared<arrow::UInt64Array>(typed_array->length(),
                                                         std::move(indices_buf));
      std::shared_ptr<arrow::Array> sort_out;
      arrow::compute::TakeOptions options;
      auto maybe_sort_out = arrow::compute::Take(*concatenated_array_.get(),
                                                 *indices_out.get(), options, ctx_);
      sort_out = *std::move(maybe_sort_out);
      *out = std::make_shared<SorterResultIterator>(ctx_, schema, sort_out, nulls_first_,
                                                    asc_);
    }
    return arrow::Status::OK();
  }

 private:
  using ArrayType_0 = typename TypeTraits<DATATYPE>::ArrayType;
  arrow::ArrayVector cached_0_;
  std::shared_ptr<arrow::Array> concatenated_array_;
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  bool NaN_check_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;
  uint64_t memory_threshold_ = -1;
  uint64_t cache_size_total_ = 0;
  std::shared_ptr<ExternalSorter> external_sorter_;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> final_result_iter_;

  class SorterResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SorterResultIterator(arrow::compute::ExecContext* ctx,
                         std::shared_ptr<arrow::Schema> result_schema,
                         std::shared_ptr<arrow::Array> result_arr, bool nulls_first,
                         bool asc)
        : ctx_(ctx),
          result_schema_(result_schema),
          total_length_(result_arr->length()),
          nulls_total_(result_arr->null_count()),
          nulls_first_(nulls_first),
          asc_(asc),
          result_arr_(result_arr) {
      batch_size_ = GetBatchSize();
    }

    std::string ToString() override { return "SortArraysToIndicesResultIterator"; }

    bool HasNext() override {
      thread_lck_.lock();
      auto status = HasNextUnsafe();
      thread_lck_.unlock();
      return status;
    }

    bool HasNextUnsafe() {
      if (ext_sort_result_iter_) return ext_sort_result_iter_->HasNext();
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    // This class is used to copy a piece of memory from the sorted ArrayData
    // to a result array.
    // It can be used only in sorted data because of null count calculation.
    template <typename KeyType>
    class SliceImpl {
     public:
      SliceImpl(const arrow::ArrayData& in, arrow::MemoryPool* pool, int64_t length,
                int64_t offset, int64_t null_total, bool null_first, int64_t total_length)
          : in_(in),
            pool_(pool),
            length_(length),
            offset_(offset),
            null_total_(null_total) {
        out_data_.type = in.type;
        out_data_.length = length;
        out_data_.buffers.resize(in.buffers.size());
        out_data_.child_data.resize(in.child_data.size());
        for (auto& data : out_data_.child_data) {
          data = std::make_shared<arrow::ArrayData>();
        }
        // decide null_count of this sliced array
        if (null_total == 0) {
          out_data_.null_count = 0;
        } else {
          if (null_first) {
            if ((offset + length) > null_total_) {
              out_data_.null_count =
                  (null_total_ - offset > 0) ? (null_total_ - offset) : 0;
            } else {
              out_data_.null_count = length;
            }
          } else {
            auto valid_total = total_length - null_total_;
            if ((offset + length) < valid_total) {
              out_data_.null_count = 0;
            } else {
              out_data_.null_count =
                  (offset - valid_total) > 0 ? length : (offset + length - valid_total);
            }
          }
        }
      }

      arrow::Status Slice(arrow::ArrayData* out) {
        const auto& buffer_0 = in_.buffers[0];
        const auto& buffer_1 = in_.buffers[1];
        if (out_data_.null_count) {
          SliceBitmap(buffer_0, &out_data_.buffers[0]);
        }
        SliceBuffer(buffer_1, &out_data_.buffers[1]);
        *out = std::move(out_data_);
        return arrow::Status::OK();
      }

     private:
      /// offset, length pair for representing a Range of a buffer or array
      struct Range {
        int64_t offset, length;

        Range() : offset(-1), length(0) {}
        Range(int64_t o, int64_t l) : offset(o), length(l) {}
      };

      /// non-owning view into a range of bits
      struct Bitmap {
        Bitmap() = default;
        Bitmap(const uint8_t* d, Range r) : data(d), range(r) {}
        explicit Bitmap(const std::shared_ptr<arrow::Buffer>& buffer, Range r)
            : Bitmap(buffer ? buffer->data() : nullptr, r) {}

        const uint8_t* data;
        Range range;

        bool AllSet() const { return data == nullptr; }
      };

      arrow::Status SliceBuffer(const std::shared_ptr<arrow::Buffer>& buffer,
                                std::shared_ptr<arrow::Buffer>* out) {
        ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer(size * length_, pool_));
        auto out_data = (*out)->mutable_data();
        auto data_begin = buffer->data();
        std::memcpy(out_data, data_begin + size * offset_, size * length_);
        return arrow::Status::OK();
      }

      arrow::Status SliceBitmap(const std::shared_ptr<arrow::Buffer>& buffer,
                                std::shared_ptr<arrow::Buffer>* out) {
        Range range(offset_, length_);
        Bitmap bitmap = Bitmap(buffer, range);

        auto length = bitmap.range.length;
        auto offset = bitmap.range.offset;
        ARROW_ASSIGN_OR_RAISE(*out, AllocateBitmap(length, pool_));
        uint8_t* dst = (*out)->mutable_data();

        if (bitmap.AllSet()) {
          arrow::BitUtil::SetBitsTo(dst, offset, length, true);
        } else {
          arrow::internal::CopyBitmap(bitmap.data, offset, length, dst, 0);
        }
        return arrow::Status::OK();
      }

      arrow::ArrayData in_;
      arrow::ArrayData out_data_;
      arrow::MemoryPool* pool_;
      int64_t length_;
      int64_t offset_;
      int64_t null_total_;
      int size = sizeof(KeyType);
    };

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override {
      thread_lck_.lock();
      auto status = NextUnsafe(out);
      thread_lck_.unlock();
      return status;
    }

    arrow::Status NextUnsafe(std::shared_ptr<arrow::RecordBatch>* out) {
      if (ext_sort_result_iter_) return ext_sort_result_iter_->Next(out);
      auto length = (total_length_ - offset_) > batch_size_ ? batch_size_
                                                            : (total_length_ - offset_);
      arrow::ArrayData result_data = *result_arr_->data();
      arrow::ArrayData out_data;
      SliceImpl<CTYPE>(result_data, ctx_->memory_pool(), length, offset_, nulls_total_,
                       nulls_first_, total_length_)
          .Slice(&out_data);
      std::shared_ptr<arrow::Array> out_0 =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(out_data)));
      offset_ += length;
      *out = arrow::RecordBatch::Make(result_schema_, length, {out_0});
      return arrow::Status::OK();
    }

    arrow::Status ClearCache() {
      result_arr_.reset();
      return arrow::Status::OK();
    }

    arrow::Status StartLock() {
      /*if (thread_lck_.try_lock_for(30s)) {
        return arrow::Status::OK();
      } else {
        return arrow::Status::Cancelled("Unable to get lock in 30s");
      }*/
      thread_lck_.lock();
      return arrow::Status::OK();
    }

    arrow::Status EndLock() {
      thread_lck_.unlock();
      return arrow::Status::OK();
    }

    arrow::Status SwitchToExternalSorterResultIterator(
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter) {
      ext_sort_result_iter_ = ext_sort_result_iter;
      RETURN_NOT_OK(ClearCache());
      return arrow::Status::OK();
    }

    bool IsSpilled() { return ext_sort_result_iter_ ? true : false; }

   private:
    std::timed_mutex
        thread_lck_;  // since there maybe two threads will access this ResultIter,
                      // one is regular task thread, one is spill thread
    using ArrayType_0 = typename arrow::TypeTraits<DATATYPE>::ArrayType;
    using BuilderType_0 = typename arrow::TypeTraits<DATATYPE>::BuilderType;
    std::shared_ptr<arrow::Array> result_arr_;
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    const uint64_t nulls_total_;
    std::shared_ptr<arrow::Schema> result_schema_;
    arrow::compute::ExecContext* ctx_;
    uint64_t batch_size_;
    bool nulls_first_;
    bool asc_;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter_;
  };
};

///////////////  SortArraysOneKey  ////////////////
template <typename DATATYPE>
class SortOnekeyKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortOnekeyKernel(arrow::compute::ExecContext* ctx,
                   std::shared_ptr<arrow::Schema> result_schema,
                   std::shared_ptr<gandiva::Projector> key_projector,
                   std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                   std::vector<bool> sort_directions, std::vector<bool> nulls_order,
                   bool NaN_check)
      : ctx_(ctx),
        nulls_first_(nulls_order[0]),
        asc_(sort_directions[0]),
        result_schema_(result_schema),
        key_projector_(key_projector),
        NaN_check_(NaN_check) {
#ifdef DEBUG
    std::cout << "UseSortOnekeyKernel" << std::endl;
#endif
    auto indices =
        GetIndicesFromSchemaCaseInsensitive(result_schema, key_field_list[0]->name());
    if (indices.size() < 1) {
      std::cout << "[ERROR] SortOnekeyKernel for arithmetic can't find key "
                << key_field_list[0]->ToString() << " from " << result_schema->ToString()
                << std::endl;
      throw JniPendingException("SortOnekeyKernel for arithmetic can't find key");
    }
    key_id_ = indices[0];
    col_num_ = result_schema->num_fields();
    memory_threshold_ = GetMemoryThreshold();
  }
  ~SortOnekeyKernel() {}

  arrow::Status Evaluate(ArrayList& in) override {
    num_batches_++;
    // do projection here
    arrow::ArrayVector outputs;
    if (key_projector_) {
      auto length = in.size() > 0 ? in[key_id_]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &outputs));
      cached_key_.push_back(std::make_shared<ArrayType_key>(outputs[0]));
      nulls_total_ += outputs[0]->null_count();
      cache_size_total_ += GetArrayVectorSize(outputs);
    } else {
      nulls_total_ += in[key_id_]->null_count();
      cached_key_.push_back(std::make_shared<ArrayType_key>(in[key_id_]));
    }

    items_total_ += in[key_id_]->length();
    length_list_.push_back(in[key_id_]->length());
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }

    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    cache_size_total_ += GetArrayVectorSize(in);

    return arrow::Status::OK();
  }

  arrow::Status ClearCache() {
    cache_size_total_ = 0;
    items_total_ = 0;
    num_batches_ = 0;
    nulls_total_ = 0;
    length_list_.clear();
    cached_.clear();
    cached_key_.clear();
    return arrow::Status::OK();
  }

  arrow::Status SortAndSpill(int64_t* spilled_size) override {
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
    if (final_result_iter_ && external_sorter_) {
      // external sort is enabled and still need extra space, nothing can be done here
      *spilled_size = 0;
      return arrow::Status::OK();
    } else if (final_result_iter_) {
      // sort has reached final phase, we don't need to create result iter
      out = final_result_iter_;
    } else {
      // sort is in evaluate phase, we should create result Iter for current in memory
      // data
      RETURN_NOT_OK(MakeResultIterator(result_schema_, &out));
    }
    *spilled_size = GetCurrentMemoryUsage();
    auto typed_final_result_iter = std::dynamic_pointer_cast<SorterResultIterator>(out);
    auto lock_status = typed_final_result_iter->StartLock();
    if (!lock_status.ok()) {
      *spilled_size = 0;
      return arrow::Status::OK();
    }

    if (!external_sorter_) {
      std::shared_ptr<ComparatorBase> comp;
      auto key_proj =
          key_projector_
              ? std::make_shared<ComparatorKeyProjector>(1, key_projector_)
              : std::make_shared<ComparatorKeyProjector>(std::vector<int>({key_id_}));
      RETURN_NOT_OK(MakeComparator<DATATYPE>(asc_, nulls_first_, NaN_check_, &comp));
      std::vector<std::shared_ptr<ComparatorBase>> comp_v = {comp};
      external_sorter_ = std::make_shared<ExternalSorter>(ctx_, comp_v, key_proj);
    }
    if (typed_final_result_iter->HasNextUnsafe()) {
      RETURN_NOT_OK(external_sorter_->Spill(out));
      if (final_result_iter_) {
        // update final_result_iter_ to use external result iterator
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter;
        RETURN_NOT_OK(
            external_sorter_->MakeResultIterator(result_schema_, &ext_sort_result_iter));
        RETURN_NOT_OK(typed_final_result_iter->SwitchToExternalSorterResultIterator(
            ext_sort_result_iter));
      }
    } else {
      *spilled_size = 0;
    }
    typed_final_result_iter->EndLock();
    RETURN_NOT_OK(ClearCache());
#ifdef DEBUG
    std::cout << "Spilled called, " << *spilled_size << " released" << std::endl;
#endif
    return arrow::Status::OK();
  }

  uint64_t GetCurrentMemoryUsage() override { return cache_size_total_; }

  uint64_t GetCurrentMemoryThreshold() override { return memory_threshold_; }

  arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
#ifdef DEBUG
    std::cout << "MergeSpilledAndMakeResultIterator "
              << (external_sorter_ ? "merge with external sorter"
                                   : "no spilled data to merge")
              << std::endl;
#endif
    RETURN_NOT_OK(MakeResultIterator(schema, out));
    final_result_iter_ = *out;
    if (external_sorter_) {
      if (*out) {
        RETURN_NOT_OK(external_sorter_->Enqueue(
            std::dynamic_pointer_cast<SorterResultIterator>(*out)));
      }
      RETURN_NOT_OK(external_sorter_->MakeResultIterator(schema, out));
    }
    return arrow::Status::OK();
  }

  void PartitionNulls(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end) {
    int64_t indices_i = 0;
    int64_t indices_null = 0;

    if (nulls_total_ == 0) {
      // if all batches have no null value,
      // we do not need to check whether the value is null
      ARROW_CHECK_LE(num_batches_, 64 * 1024);
      for (int array_id = 0; array_id < num_batches_; array_id++) {
        ARROW_CHECK_LE(length_list_[array_id], 64 * 1024);
        for (int64_t i = 0; i < length_list_[array_id]; i++) {
          (indices_begin + indices_i)->array_id = array_id;
          (indices_begin + indices_i)->id = i;
          indices_i++;
        }
      }
    } else {
      // we should support nulls first and nulls last here
      ARROW_CHECK_LE(num_batches_, 64 * 1024);
      for (int array_id = 0; array_id < num_batches_; array_id++) {
        ARROW_CHECK_LE(length_list_[array_id], 64 * 1024);
        if (cached_key_[array_id]->null_count() == 0) {
          // if this array has no null value,
          // we do need to check if the value is null
          for (int64_t i = 0; i < length_list_[array_id]; i++) {
            if (nulls_first_) {
              (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_begin + indices_i)->array_id = array_id;
              (indices_begin + indices_i)->id = i;
              indices_i++;
            }
          }
        } else {
          for (int64_t i = 0; i < length_list_[array_id]; i++) {
            if (nulls_first_) {
              if (!cached_key_[array_id]->IsNull(i)) {
                (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
                (indices_begin + nulls_total_ + indices_i)->id = i;
                indices_i++;
              } else {
                (indices_begin + indices_null)->array_id = array_id;
                (indices_begin + indices_null)->id = i;
                indices_null++;
              }
            } else {
              if (!cached_key_[array_id]->IsNull(i)) {
                (indices_begin + indices_i)->array_id = array_id;
                (indices_begin + indices_i)->id = i;
                indices_i++;
              } else {
                (indices_end - nulls_total_ + indices_null)->array_id = array_id;
                (indices_end - nulls_total_ + indices_null)->id = i;
                indices_null++;
              }
            }
          }
        }
      }
    }
  }

  int64_t PartitionNaNs(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end) {
    int64_t indices_i = 0;
    int64_t indices_nan = 0;

    ARROW_CHECK_LE(num_batches_, 64 * 1024);
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      ARROW_CHECK_LE(length_list_[array_id], 64 * 1024);
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        if (cached_key_[array_id]->IsNull(i)) {
          continue;
        }
        if (nulls_first_) {
          if (asc_) {
            // values should be partitioned to:
            // null, null, ..., valid-1, valid-2, ..., valid-3, NaN, NaN, ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_begin + nulls_total_ + indices_i)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_end - indices_nan - 1)->array_id = array_id;
              (indices_end - indices_nan - 1)->id = i;
              indices_nan++;
            }
          } else {
            // values should be partitioned to:
            // null, null, ..., NaN, NaN, ..., valid-1, valid-2, ..., valid-3,
            // ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_end - indices_i - 1)->array_id = array_id;
              (indices_end - indices_i - 1)->id = i;
              indices_i++;
            } else {
              (indices_begin + nulls_total_ + indices_nan)->array_id = array_id;
              (indices_begin + nulls_total_ + indices_nan)->id = i;
              indices_nan++;
            }
          }
        } else {
          if (asc_) {
            // values should be partitioned to:
            // valid-1, valid-2, ..., valid-3, ..., NaN, NaN, ..., null, null,
            // ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_begin + indices_i)->array_id = array_id;
              (indices_begin + indices_i)->id = i;
              indices_i++;
            } else {
              (indices_end - nulls_total_ - indices_nan - 1)->array_id = array_id;
              (indices_end - nulls_total_ - indices_nan - 1)->id = i;
              indices_nan++;
            }
          } else {
            // values should be partitioned to:
            // NaN, NaN, ..., valid-1, valid-2, ..., valid-3, ..., null, null,
            // ...
            if (!std::isnan(cached_key_[array_id]->GetView(i))) {
              (indices_end - nulls_total_ - indices_i - 1)->array_id = array_id;
              (indices_end - nulls_total_ - indices_i - 1)->id = i;
              indices_i++;
            } else {
              (indices_begin + indices_nan)->array_id = array_id;
              (indices_begin + indices_nan)->id = i;
              indices_nan++;
            }
          }
        }
      }
    }
    return indices_nan;
  }

  template <typename T>
  auto Partition(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end,
                 int64_t& num_nan) ->
      typename std::enable_if_t<arrow::is_floating_type<T>::value> {
    PartitionNulls(indices_begin, indices_end);
    if (NaN_check_) {
      num_nan = PartitionNaNs(indices_begin, indices_end);
    }
  }

  template <typename T>
  auto Partition(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end,
                 int64_t& num_nan) ->
      typename std::enable_if_t<!arrow::is_floating_type<T>::value> {
    PartitionNulls(indices_begin, indices_end);
  }

  template <typename T>
  auto Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t num_nan)
      -> typename std::enable_if_t<is_number_bool_date<T>::value> {
    if (asc_) {
      if (nulls_first_) {
        ska_sort(indices_begin + nulls_total_, indices_begin + items_total_ - num_nan,
                 [this](auto& x) -> decltype(auto) {
                   return cached_key_[x.array_id]->GetView(x.id);
                 });
      } else {
        ska_sort(indices_begin, indices_begin + items_total_ - nulls_total_ - num_nan,
                 [this](auto& x) -> decltype(auto) {
                   return cached_key_[x.array_id]->GetView(x.id);
                 });
      }
    } else {
      auto comp = [this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
        return cached_key_[x.array_id]->GetView(x.id) >
               cached_key_[y.array_id]->GetView(y.id);
      };
      if (nulls_first_) {
        gfx::timsort(indices_begin + nulls_total_ + num_nan, indices_begin + items_total_,
                  comp);
      } else {
        gfx::timsort(indices_begin + num_nan, indices_begin + items_total_ - nulls_total_,
                  comp);
      }
    }
  }

  template <typename T>
  auto Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t num_nan)
      -> typename std::enable_if_t<std::is_same<T, arrow::StringType>::value> {
    if (asc_) {
      auto comp = [this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
        return cached_key_[x.array_id]->GetString(x.id) <
               cached_key_[y.array_id]->GetString(y.id);
      };
      if (nulls_first_) {
        gfx::timsort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        gfx::timsort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    } else {
      auto comp = [this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
        return cached_key_[x.array_id]->GetString(x.id) >
               cached_key_[y.array_id]->GetString(y.id);
      };
      if (nulls_first_) {
        gfx::timsort(indices_begin + nulls_total_, indices_begin + items_total_, comp);
      } else {
        gfx::timsort(indices_begin, indices_begin + items_total_ - nulls_total_, comp);
      }
    }
  }

  template <typename T>
  auto Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end, int64_t num_nan)
      -> typename std::enable_if_t<arrow::is_decimal_type<T>::value> {
    if (asc_) {
      auto comp = [this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
        return cached_key_[x.array_id]->GetView(x.id) <
               cached_key_[y.array_id]->GetView(y.id);
      };
      if (nulls_first_) {
        gfx::timsort(indices_begin + nulls_total_, indices_begin + items_total_ - num_nan,
                  comp);
      } else {
        gfx::timsort(indices_begin, indices_begin + items_total_ - nulls_total_ - num_nan,
                  comp);
      }
    } else {
      auto comp = [this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
        return cached_key_[x.array_id]->GetView(x.id) >
               cached_key_[y.array_id]->GetView(y.id);
      };
      if (nulls_first_) {
        gfx::timsort(indices_begin + nulls_total_ + num_nan, indices_begin + items_total_,
                  comp);
      } else {
        gfx::timsort(indices_begin + num_nan, indices_begin + items_total_ - nulls_total_,
                  comp);
      }
    }
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf = *std::move(maybe_buffer);
    ArrayItemIndexS* indices_begin =
        reinterpret_cast<ArrayItemIndexS*>(indices_buf->mutable_data());
    ArrayItemIndexS* indices_end = indices_begin + items_total_;
    // do partition and sort here
    int64_t num_nan = 0;
    Partition<DATATYPE>(indices_begin, indices_end, num_nan);
    Sort<DATATYPE>(indices_begin, indices_end, num_nan);
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(
        MakeFixedSizeBinaryType(sizeof(ArrayItemIndexS) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));

    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    if (cached_.empty()) return arrow::Status::OK();
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

 private:
  using ArrayType_key = typename TypeTraits<DATATYPE>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_key>> cached_key_;
  std::vector<arrow::ArrayVector> cached_;
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  bool nulls_first_;
  bool asc_;
  bool NaN_check_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  uint64_t nulls_total_ = 0;
  int col_num_;
  int key_id_;
  uint64_t memory_threshold_ = -1;
  uint64_t cache_size_total_ = 0;
  std::shared_ptr<ExternalSorter> external_sorter_;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> final_result_iter_;
};

///////////////  SortArraysCodegen  ////////////////
class SortArraysCodegenKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortArraysCodegenKernel(arrow::compute::ExecContext* ctx,
                          std::shared_ptr<arrow::Schema> result_schema,
                          std::shared_ptr<gandiva::Projector> key_projector,
                          std::vector<std::shared_ptr<arrow::DataType>> projected_types,
                          std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                          std::vector<bool> sort_directions,
                          std::vector<bool> nulls_order, bool NaN_check)
      : ctx_(ctx),
        result_schema_(result_schema),
        key_projector_(key_projector),
        key_field_list_(key_field_list),
        sort_directions_(sort_directions),
        nulls_order_(nulls_order),
        projected_types_(projected_types),
        NaN_check_(NaN_check) {
#ifdef DEBUG
    std::cout << "use SortArraysCodegenKernel" << std::endl;
#endif
    for (auto field : key_field_list) {
      auto indices = GetIndicesFromSchemaCaseInsensitive(result_schema, field->name());
      if (indices.size() < 1) {
        std::cout << "[ERROR] SortArraysCodegenKernel can't find key "
                  << field->ToString() << " from " << result_schema->ToString()
                  << std::endl;
        throw JniPendingException("SortArraysCodegenKernel can't find key");
      }
      key_index_list_.push_back(indices[0]);
    }
    col_num_ = result_schema->num_fields();
    if (!key_projector) {
      auto status = LoadJITFunction(key_field_list);
      if (!status.ok()) {
        std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
        throw JniPendingException("Sort LoadJITFunction failed");
        ;
      }
    } else {
      int i = 0;
      for (auto type : projected_types) {
        auto field = arrow::field(std::to_string(i), type);
        projected_field_list_.push_back(field);
        i++;
      }
      auto status = LoadJITFunction(projected_field_list_);
      if (!status.ok()) {
        std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
        throw JniPendingException("Sort LoadJITFunction failed");
        ;
      }
    }
    memory_threshold_ = GetMemoryThreshold();
  }

  arrow::Status LoadJITFunction(
      std::vector<std::shared_ptr<arrow::Field>> key_field_list) {
    // generate ddl signature
    std::stringstream func_args_ss;
    for (int i = 0; i < sort_directions_.size(); i++) {
      func_args_ss << "[Sorter]" << (nulls_order_[i] ? "nulls_first" : "nulls_last")
                   << "|" << (sort_directions_[i] ? "asc" : "desc");
    }
    for (int i = 0; i < key_field_list.size(); i++) {
      auto field = key_field_list[i];
      func_args_ss << "[Type]" << field->type()->ToString();
    }
#ifdef DEBUG
    std::cout << "func_args_ss is " << func_args_ss.str() << std::endl;
#endif
    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    signature_ = signature_ss.str();

    auto file_lock = FileSpinLock();
    auto status = LoadLibrary(signature_, ctx_, &sorter_);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes();
      // compile codes
      const arrow::Status& status1 = CompileCodes(codes, signature_);
      if (!status1.ok()) {
        FileSpinUnLock(file_lock);
        return status1;
      }
      const arrow::Status& status2 = LoadLibrary(signature_, ctx_, &sorter_);
      if (!status2.ok()) {
        FileSpinUnLock(file_lock);
        return status2;
      }
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  std::string GetSignature() { return signature_; }

  arrow::Status Evaluate(ArrayList& in) override {
    num_batches_++;
    if (cached_.size() < col_num_) {
      cached_.resize(col_num_);
    }
    if (!key_projector_) {
      ArrayList key_cols;
      for (auto idx : key_index_list_) {
        key_cols.push_back(in[idx]);
      }
      sorter_->Evaluate(key_cols);
    } else {
      std::vector<std::shared_ptr<arrow::Array>> projected_batch;
      // do projection here, and the projected arrays are used for comparison
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &projected_batch));
      sorter_->Evaluate(projected_batch);
      cache_size_total_ += GetArrayVectorSize(projected_batch);
    }
    items_total_ += in[0]->length();

    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    cache_size_total_ += GetArrayVectorSize(in);

    return arrow::Status::OK();
  }

  arrow::Status ClearCache() {
    num_batches_ = 0;
    cache_size_total_ = 0;
    cached_.clear();
    items_total_ = 0;
    return sorter_->ClearCache();
  }

  arrow::Status SortAndSpill(int64_t* spilled_size) override {
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
    if (final_result_iter_ && external_sorter_) {
      // external sort is enabled and still need extra space, nothing can be done here
      *spilled_size = 0;
      return arrow::Status::OK();
    } else if (final_result_iter_) {
      // sort has reached final phase, we don't need to create result iter
      out = final_result_iter_;
    } else {
      // sort is in evaluate phase, we should create result Iter for current in memory
      // data
      RETURN_NOT_OK(MakeResultIterator(result_schema_, &out));
    }
    *spilled_size = GetCurrentMemoryUsage();
    auto typed_final_result_iter = std::dynamic_pointer_cast<SorterResultIterator>(out);
    auto lock_status = typed_final_result_iter->StartLock();
    if (!lock_status.ok()) {
      *spilled_size = 0;
      return arrow::Status::OK();
    }

    if (!external_sorter_) {
      std::vector<std::shared_ptr<ComparatorBase>> comp_v;
      gandiva::FieldVector field_list =
          key_projector_ ? projected_field_list_ : key_field_list_;
      auto key_proj = key_projector_
                          ? std::make_shared<ComparatorKeyProjector>(
                                projected_field_list_.size(), key_projector_)
                          : std::make_shared<ComparatorKeyProjector>(key_index_list_);
      auto idx = 0;
      for (auto field : field_list) {
        std::shared_ptr<ComparatorBase> comp;
        RETURN_NOT_OK(MakeComparator(field->type(), sort_directions_[idx],
                                     nulls_order_[idx], NaN_check_, &comp));
        idx++;
        comp_v.push_back(comp);
      }
      external_sorter_ = std::make_shared<ExternalSorter>(ctx_, comp_v, key_proj);
    }
    if (typed_final_result_iter->HasNextUnsafe()) {
      RETURN_NOT_OK(external_sorter_->Spill(out));
      if (final_result_iter_) {
        // update final_result_iter_ to use external result iterator
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter;
        RETURN_NOT_OK(
            external_sorter_->MakeResultIterator(result_schema_, &ext_sort_result_iter));
        RETURN_NOT_OK(typed_final_result_iter->SwitchToExternalSorterResultIterator(
            ext_sort_result_iter));
      }
    } else {
      *spilled_size = 0;
    }
    typed_final_result_iter->EndLock();
    RETURN_NOT_OK(ClearCache());
#ifdef DEBUG
    std::cout << "Spilled called, " << *spilled_size << " released" << std::endl;
#endif
    return arrow::Status::OK();
  }

  uint64_t GetCurrentMemoryUsage() override { return cache_size_total_; }

  uint64_t GetCurrentMemoryThreshold() override { return memory_threshold_; }

  arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
#ifdef DEBUG
    std::cout << "MergeSpilledAndMakeResultIterator "
              << (external_sorter_ ? "merge with external sorter"
                                   : "no spilled data to merge")
              << std::endl;
#endif
    RETURN_NOT_OK(MakeResultIterator(schema, out));
    final_result_iter_ = *out;
    if (external_sorter_) {
      if (*out) {
        RETURN_NOT_OK(external_sorter_->Enqueue(
            std::dynamic_pointer_cast<SorterResultIterator>(*out)));
      }
      RETURN_NOT_OK(external_sorter_->MakeResultIterator(schema, out));
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<SortRelation>>* out) override {
    return arrow::Status::NotImplemented(
        "MergeSpilledAndMakeResultIterator for ResultIterator<SortRelation> is not "
        "supported yet");
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    if (cached_.empty()) return arrow::Status::OK();
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(sorter_->FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<SortRelation>>* out) override {
    RETURN_NOT_OK(sorter_->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    return arrow::Status::OK();
  }

  class TypedSorterCodeGenImpl {
   public:
    TypedSorterCodeGenImpl(std::string indice, std::shared_ptr<arrow::DataType> data_type)
        : indice_(indice), data_type_(data_type) {}
    std::string GetCachedVariablesDefine() {
      std::stringstream ss;
      ss << "using ArrayType_" << indice_ << " = " << GetTypeString(data_type_, "Array")
         << ";" << std::endl;
      ss << "std::vector<std::shared_ptr<ArrayType_" << indice_ << ">> cached_" << indice_
         << "_;" << std::endl;
      return ss.str();
    }
    std::string GetCachedVariablesClear() {
      std::stringstream ss;
      ss << "cached_" << indice_ << "_.clear();" << std::endl;
      return ss.str();
    }

   private:
    std::string indice_;
    std::shared_ptr<arrow::DataType> data_type_;
  };

  std::string ProduceCodes() {
    int indice = 0;
    std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> key_typed_codegen_list;
    if (key_projector_) {
      for (auto field : projected_field_list_) {
        auto codegen = std::make_shared<TypedSorterCodeGenImpl>(std::to_string(indice),
                                                                field->type());
        key_typed_codegen_list.push_back(codegen);
        indice++;
      }
    } else {
      for (auto field : key_field_list_) {
        auto codegen = std::make_shared<TypedSorterCodeGenImpl>(std::to_string(indice),
                                                                field->type());
        key_typed_codegen_list.push_back(codegen);
        indice++;
      }
    }

    std::string cached_insert_str = GetCachedInsert();

    std::string comp_func_str = GetCompFunction(true);

    std::string comp_func_str_without_null = GetCompFunction(false);

    std::string sort_func_str = GetSortFunction();

    std::string cached_variables_define_str =
        GetCachedVariablesDefine(key_typed_codegen_list);

    std::string cached_variables_clear_str =
        GetCachedVariablesClear(key_typed_codegen_list);

    return BaseCodes() + R"(

#include "arrow/util/logging.h"
#include "precompile/wscgapi.hpp"


class TypedSorterImpl : public CodeGenBase {
 public:
  TypedSorterImpl(arrow::compute::ExecContext* ctx) : ctx_(ctx) {}

  arrow::Status Evaluate(ArrayList& in) override {
    num_batches_++;
    )" + cached_insert_str +
           R"(
    auto cur = cached_0_[cached_0_.size() - 1];         
    items_total_ += cur->length();
    length_list_.push_back(cur->length());
    
    return arrow::Status::OK();
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {
    // we should support nulls first and nulls last here
    // we should also support desc and asc here
    )" + comp_func_str +
           comp_func_str_without_null +
           R"(
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf = *std::move(maybe_buffer);

    ArrayItemIndexS* indices_begin =
        reinterpret_cast<ArrayItemIndexS*>(indices_buf->mutable_data());
    ArrayItemIndexS* indices_end = indices_begin + items_total_;

    int64_t indices_i = 0;
    ARROW_CHECK_LE(num_batches_, 64 * 1024);
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      ARROW_CHECK_LE(length_list_[array_id], 64*1024);
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        (indices_begin + indices_i)->array_id = array_id;
        (indices_begin + indices_i)->id = i;
        indices_i++;
      }
    }

    )" + sort_func_str +
           R"(
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(MakeFixedSizeBinaryType(sizeof(ArrayItemIndexS) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status ClearCache() {
    num_batches_ = 0;
    items_total_ = 0;
    length_list_.clear();
  )" + cached_variables_clear_str +
           R"(
    return arrow::Status::OK();
  }
  
 private:
  )" + cached_variables_define_str +
           R"(
  std::vector<int64_t> length_list_;
  arrow::compute::ExecContext* ctx_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  // If all batches has no null value, has_null_ will remain false
  bool has_null_ = false;
};

extern "C" void MakeCodeGen(arrow::compute::ExecContext* ctx,
                            std::shared_ptr<CodeGenBase>* out) {
  *out = std::make_shared<TypedSorterImpl>(ctx);
}

    )";
  }

  std::string GetCachedInsert() {
    std::stringstream ss;
    for (int i = 0; i < key_index_list_.size(); i++) {
      ss << "cached_" << i << "_.push_back(std::make_shared<ArrayType_" << i << ">(in["
         << i
         << "]));\n"
         // update has_null_
         << "if (!has_null_ && cached_" << i << "_[cached_" << i
         << "_.size() - 1]->null_count() > 0) {"
         << "has_null_ = true;}" << std::endl;
    }
    return ss.str();
  }

  std::string GetSortFunction() {
    std::stringstream ss;
    ss << "if (has_null_) {\n"
       << "gfx::timsort(indices_begin, indices_begin + items_total_, comp);} "
          "else {\n"
       << "gfx::timsort(indices_begin, indices_begin + items_total_, "
          "comp_without_null);}"
       << std::endl;
    return ss.str();
  }

  std::string GetCompFunction(bool has_null) {
    std::stringstream ss;
    bool projected;
    if (key_projector_) {
      projected = true;
    } else {
      projected = false;
    }
    if (has_null) {
      ss << "auto comp = [this](const ArrayItemIndexS& x, const "
            "ArrayItemIndexS& y) {"
         << GetCompFunction_(0, projected, key_field_list_, projected_types_,
                             sort_directions_, nulls_order_);
    } else {
      ss << "auto comp_without_null = "
         << "[this](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {"
         << GetCompFunction_Without_Null_(0, projected, key_field_list_, projected_types_,
                                          sort_directions_);
    }
    ss << "};\n";
    return ss.str();
  }

  std::string GetCompFunction_(
      int cur_key_idx, bool projected,
      const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
      const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
      const std::vector<bool>& sort_directions, const std::vector<bool>& nulls_order) {
    std::string comp_str;
    bool asc = sort_directions[cur_key_idx];
    bool nulls_first = nulls_order[cur_key_idx];
    std::shared_ptr<arrow::DataType> data_type;
    std::string array;
    // if projected, use projected batch to compare, and use projected type
    array = "cached_";
    if (projected) {
      data_type = projected_types[cur_key_idx];
    } else {
      data_type = key_field_list[cur_key_idx]->type();
    }

    auto x_num_value =
        array + std::to_string(cur_key_idx) + "_[x.array_id]->GetView(x.id)";
    auto x_str_value =
        array + std::to_string(cur_key_idx) + "_[x.array_id]->GetString(x.id)";
    auto y_num_value =
        array + std::to_string(cur_key_idx) + "_[y.array_id]->GetView(y.id)";
    auto y_str_value =
        array + std::to_string(cur_key_idx) + "_[y.array_id]->GetString(y.id)";
    auto is_x_null = array + std::to_string(cur_key_idx) + "_[x.array_id]->IsNull(x.id)";
    auto is_y_null = array + std::to_string(cur_key_idx) + "_[y.array_id]->IsNull(y.id)";
    auto x_null_count =
        array + std::to_string(cur_key_idx) + "_[x.array_id]->null_count() > 0";
    auto y_null_count =
        array + std::to_string(cur_key_idx) + "_[y.array_id]->null_count() > 0";
    auto x_null = "(" + x_null_count + " && " + is_x_null + " )";
    auto y_null = "(" + y_null_count + " && " + is_y_null + " )";
    auto is_x_nan = "std::isnan(" + x_num_value + ")";
    auto is_y_nan = "std::isnan(" + y_num_value + ")";

    // Multiple keys sorting w/ nulls first/last is supported.
    std::stringstream ss;
    // We need to determine the position of nulls.
    ss << "if (" << x_null << ") {\n";
    // If value accessed from x is null, return true to make nulls first.
    if (nulls_first) {
      ss << "return true;\n}";
    } else {
      ss << "return false;\n}";
    }
    // If value accessed from y is null, return false to make nulls first.
    ss << " else if (" << y_null << ") {\n";
    if (nulls_first) {
      ss << "return false;\n}";
    } else {
      ss << "return true;\n}";
    }
    // If datatype is floating, we need to do partition for NaN if NaN check is
    // enabled
    if (data_type->id() == arrow::Type::DOUBLE || data_type->id() == arrow::Type::FLOAT) {
      if (NaN_check_) {
        ss << "else if (" << is_x_nan << ") {\n";
        if (asc) {
          ss << "return false;\n}";
        } else {
          ss << "return true;\n}";
        }
        ss << "else if (" << is_y_nan << ") {\n";
        if (asc) {
          ss << "return true;\n}";
        } else {
          ss << "return false;\n}";
        }
      }
    }

    // If values accessed from x and y are both not null
    ss << " else {\n";

    // Multiple keys sorting w/ different ordering is supported.
    // For string type of data, GetString should be used instead of GetView.
    if (asc) {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " < " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " < " << y_num_value << ";\n}\n";
      }
    } else {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " > " << y_str_value << ";\n}\n";
      } else {
        ss << "return " << x_num_value << " > " << y_num_value << ";\n}\n";
      }
    }
    comp_str = ss.str();
    if ((cur_key_idx + 1) == sort_directions.size()) {
      return comp_str;
    }
    // clear the contents of stringstream
    ss.str(std::string());
    if (data_type->id() == arrow::Type::STRING) {
      ss << "if ((" << x_null << " && " << y_null << ") || (" << x_str_value
         << " == " << y_str_value << ")) {";
    } else {
      if (NaN_check_ && (data_type->id() == arrow::Type::DOUBLE ||
                         data_type->id() == arrow::Type::FLOAT)) {
        // need to check NaN
        ss << "if ((" << x_null << " && " << y_null << ") || (" << is_x_nan << " && "
           << is_y_nan << ") || (" << x_num_value << " == " << y_num_value << ")) {";
      } else {
        ss << "if ((" << x_null << " && " << y_null << ") || (" << x_num_value
           << " == " << y_num_value << ")) {";
      }
    }
    ss << GetCompFunction_(cur_key_idx + 1, projected, key_field_list, projected_types,
                           sort_directions, nulls_order)
       << "} else { " << comp_str << "}";
    return ss.str();
  }

  std::string GetCompFunction_Without_Null_(
      int cur_key_idx, bool projected,
      const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
      const std::vector<std::shared_ptr<arrow::DataType>>& projected_types,
      const std::vector<bool>& sort_directions) {
    std::string comp_str;
    bool asc = sort_directions[cur_key_idx];
    std::shared_ptr<arrow::DataType> data_type;
    std::string array;
    // if projected, use projected batch to compare, and use projected type
    array = "cached_";
    if (projected) {
      data_type = projected_types[cur_key_idx];
    } else {
      data_type = key_field_list[cur_key_idx]->type();
    }

    auto x_num_value =
        array + std::to_string(cur_key_idx) + "_[x.array_id]->GetView(x.id)";
    auto x_str_value =
        array + std::to_string(cur_key_idx) + "_[x.array_id]->GetString(x.id)";
    auto y_num_value =
        array + std::to_string(cur_key_idx) + "_[y.array_id]->GetView(y.id)";
    auto y_str_value =
        array + std::to_string(cur_key_idx) + "_[y.array_id]->GetString(y.id)";
    auto is_x_nan = "std::isnan(" + x_num_value + ")";
    auto is_y_nan = "std::isnan(" + y_num_value + ")";

    // Multiple keys sorting w/ nulls first/last is supported.
    std::stringstream ss;
    // If datatype is floating, we need to do partition for NaN if NaN check is
    // enabled
    if (NaN_check_ && (data_type->id() == arrow::Type::DOUBLE ||
                       data_type->id() == arrow::Type::FLOAT)) {
      ss << "if (" << is_x_nan << ") {\n";
      if (asc) {
        ss << "return false;\n}";
      } else {
        ss << "return true;\n}";
      }
      ss << "else if (" << is_y_nan << ") {\n";
      if (asc) {
        ss << "return true;\n}";
      } else {
        ss << "return false;\n}";
      }
      // If values accessed from x and y are both not nan
      ss << " else {\n";
    }

    // Multiple keys sorting w/ different ordering is supported.
    // For string type of data, GetString should be used instead of GetView.
    if (asc) {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " < " << y_str_value << ";\n";
      } else {
        ss << "return " << x_num_value << " < " << y_num_value << ";\n";
      }
    } else {
      if (data_type->id() == arrow::Type::STRING) {
        ss << "return " << x_str_value << " > " << y_str_value << ";\n";
      } else {
        ss << "return " << x_num_value << " > " << y_num_value << ";\n";
      }
    }
    if (NaN_check_ && (data_type->id() == arrow::Type::DOUBLE ||
                       data_type->id() == arrow::Type::FLOAT)) {
      ss << "}" << std::endl;
    }
    comp_str = ss.str();
    if ((cur_key_idx + 1) == sort_directions.size()) {
      return comp_str;
    }
    // clear the contents of stringstream
    ss.str(std::string());
    if (data_type->id() == arrow::Type::STRING) {
      ss << "if (" << x_str_value << " == " << y_str_value << ") {";
    } else {
      if (NaN_check_ && (data_type->id() == arrow::Type::DOUBLE ||
                         data_type->id() == arrow::Type::FLOAT)) {
        // need to check NaN
        ss << "if ((" << is_x_nan << " && " << is_y_nan << ") || (" << x_num_value
           << " == " << y_num_value << ")) {";
      } else {
        ss << "if (" << x_num_value << " == " << y_num_value << ") {";
      }
    }
    ss << GetCompFunction_Without_Null_(cur_key_idx + 1, projected, key_field_list,
                                        projected_types, sort_directions)
       << "} else { " << comp_str << "}";
    return ss.str();
  }

  std::string GetCachedVariablesDefine(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetCachedVariablesDefine() << std::endl;
    }
    return ss.str();
  }

  std::string GetCachedVariablesClear(
      std::vector<std::shared_ptr<TypedSorterCodeGenImpl>> shuffle_typed_codegen_list) {
    std::stringstream ss;
    for (auto codegen : shuffle_typed_codegen_list) {
      ss << codegen->GetCachedVariablesClear() << std::endl;
    }
    return ss.str();
  }

 private:
  arrow::compute::ExecContext* ctx_;
  std::vector<int> key_index_list_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::vector<arrow::ArrayVector> cached_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types_;
  std::vector<std::shared_ptr<arrow::Field>> projected_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> key_field_list_;
  std::shared_ptr<CodeGenBase> sorter_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  // true for asc, false for desc
  std::vector<bool> sort_directions_;
  // true for nulls_first, false for nulls_last
  std::vector<bool> nulls_order_;
  bool NaN_check_;
  int col_num_ = 0;
  std::string signature_;
  uint64_t memory_threshold_ = -1;
  uint64_t cache_size_total_ = 0;
  std::shared_ptr<ExternalSorter> external_sorter_;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> final_result_iter_;
};

///////////////  SortArraysMultipleKeys  ////////////////
class SortMultiplekeyKernel : public SortArraysToIndicesKernel::Impl {
 public:
  SortMultiplekeyKernel(arrow::compute::ExecContext* ctx,
                        std::shared_ptr<arrow::Schema> result_schema,
                        std::shared_ptr<gandiva::Projector> key_projector,
                        std::vector<std::shared_ptr<arrow::DataType>> projected_types,
                        std::vector<std::shared_ptr<arrow::Field>> key_field_list,
                        std::vector<bool> sort_directions, std::vector<bool> nulls_order,
                        bool NaN_check)
      : ctx_(ctx),
        nulls_order_(nulls_order),
        sort_directions_(sort_directions),
        result_schema_(result_schema),
        key_projector_(key_projector),
        key_field_list_(key_field_list),
        NaN_check_(NaN_check) {
#ifdef DEBUG
    std::cout << "UseSortMultiplekeyKernel" << std::endl;
#endif
    for (auto field : key_field_list) {
      auto indices = GetIndicesFromSchemaCaseInsensitive(result_schema, field->name());
      if (indices.size() < 1) {
        std::cout << "[ERROR] SortArraysToIndicesKernel::Impl can't find key "
                  << field->ToString() << " from " << result_schema->ToString()
                  << std::endl;
        throw JniPendingException("SortArraysToIndicesKernel::Impl can't find key");
      }
      key_index_list_.push_back(indices[0]);
    }
    col_num_ = result_schema->num_fields();
    int i = 0;
    for (auto type : projected_types) {
      auto field = arrow::field(std::to_string(i), type);
      projected_field_list_.push_back(field);
      i++;
    }
    memory_threshold_ = GetMemoryThreshold();
  }
  ~SortMultiplekeyKernel() {}

  arrow::Status Evaluate(ArrayList& in) override {
    num_batches_++;
    if (cached_.size() <= col_num_) {
      cached_.resize(col_num_ + 1);
    }
    for (int i = 0; i < col_num_; i++) {
      cached_[i].push_back(in[i]);
    }
    cache_size_total_ += GetArrayVectorSize(in);
    if (key_projector_) {
      int projected_col_num = projected_field_list_.size();
      if (projected_.size() <= projected_col_num) {
        projected_.resize(projected_col_num + 1);
      }
      std::vector<std::shared_ptr<arrow::Array>> projected_batch;
      // do projection here, and the projected arrays are used for comparison
      auto length = in.size() > 0 ? in[0]->length() : 0;
      auto in_batch = arrow::RecordBatch::Make(result_schema_, length, in);
      RETURN_NOT_OK(
          key_projector_->Evaluate(*in_batch, ctx_->memory_pool(), &projected_batch));
      for (int i = 0; i < projected_col_num; i++) {
        std::shared_ptr<arrow::Array> col = projected_batch[i];
        projected_[i].push_back(col);
        cache_size_total_ += GetArrayVectorSize(projected_[i]);
      }
    }
    items_total_ += in[0]->length();
    length_list_.push_back(in[0]->length());
    return arrow::Status::OK();
  }

  int compareInternal(int left_array_id, int64_t left_id, int right_array_id,
                      int64_t right_id, int keys_num) {
    int key_idx = 0;
    while (key_idx < keys_num) {
      // In comparison, 1 represents for true, 0 for false, and 2 for equal.
      int cmp_res = 2;
      cmp_functions_[key_idx](left_array_id, right_array_id, left_id, right_id, cmp_res);
      if (cmp_res != 2) {
        return cmp_res;
      }
      key_idx += 1;
    }
    return 2;
  }

  bool compareRow(int left_array_id, int64_t left_id, int right_array_id,
                  int64_t right_id, int keys_num) {
    if (compareInternal(left_array_id, left_id, right_array_id, right_id, keys_num) ==
        1) {
#ifdef DEBUG
      std::cout << "compare res left:" << left_array_id << "_" << left_id
                << " < right:" << right_array_id << "_" << right_id << std::endl;
#endif
      return true;
    }
#ifdef DEBUG
    std::cout << "compare res left:" << left_array_id << "_" << left_id
              << " >= right:" << right_array_id << "_" << right_id << std::endl;
#endif
    return false;
  }

  arrow::Status ClearCache() {
    num_batches_ = 0;
    items_total_ = 0;
    cache_size_total_ = 0;
    projected_.clear();
    cached_.clear();
    length_list_.clear();
    comparators_.clear();
    cmp_functions_.clear();
    return arrow::Status::OK();
  }

  arrow::Status SortAndSpill(int64_t* spilled_size) override {
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
    if (final_result_iter_ && external_sorter_) {
      // external sort is enabled and still need extra space, nothing can be done here
      *spilled_size = 0;
      return arrow::Status::OK();
    } else if (final_result_iter_) {
      // sort has reached final phase, we don't need to create result iter
      out = final_result_iter_;
    } else {
      // sort is in evaluate phase, we should create result Iter for current in memory
      // data
      RETURN_NOT_OK(MakeResultIterator(result_schema_, &out));
    }
    *spilled_size = GetCurrentMemoryUsage();
    auto typed_final_result_iter = std::dynamic_pointer_cast<SorterResultIterator>(out);
    auto lock_status = typed_final_result_iter->StartLock();
    if (!lock_status.ok()) {
      *spilled_size = 0;
      return arrow::Status::OK();
    }

    if (!external_sorter_) {
      std::vector<std::shared_ptr<ComparatorBase>> comp_v;
      gandiva::FieldVector field_list =
          key_projector_ ? projected_field_list_ : key_field_list_;
      auto key_proj = key_projector_
                          ? std::make_shared<ComparatorKeyProjector>(
                                projected_field_list_.size(), key_projector_)
                          : std::make_shared<ComparatorKeyProjector>(key_index_list_);
      auto idx = 0;
      for (auto field : field_list) {
        std::shared_ptr<ComparatorBase> comp;
        RETURN_NOT_OK(MakeComparator(field->type(), sort_directions_[idx],
                                     nulls_order_[idx], NaN_check_, &comp));
        idx++;
        comp_v.push_back(comp);
      }
      external_sorter_ = std::make_shared<ExternalSorter>(ctx_, comp_v, key_proj);
    }
    if (typed_final_result_iter->HasNextUnsafe()) {
      RETURN_NOT_OK(external_sorter_->Spill(out));
      if (final_result_iter_) {
        // update final_result_iter_ to use external result iterator
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> ext_sort_result_iter;
        RETURN_NOT_OK(
            external_sorter_->MakeResultIterator(result_schema_, &ext_sort_result_iter));
        RETURN_NOT_OK(typed_final_result_iter->SwitchToExternalSorterResultIterator(
            ext_sort_result_iter));
      }
    } else {
      *spilled_size = 0;
    }
    typed_final_result_iter->EndLock();
    RETURN_NOT_OK(ClearCache());
#ifdef DEBUG
    std::cout << "Spilled called, " << *spilled_size << " released" << std::endl;
#endif
    return arrow::Status::OK();
  }

  uint64_t GetCurrentMemoryUsage() override { return cache_size_total_; }

  uint64_t GetCurrentMemoryThreshold() override { return memory_threshold_; }

  arrow::Status MergeSpilledAndMakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
#ifdef DEBUG
    std::cout << "MergeSpilledAndMakeResultIterator "
              << (external_sorter_ ? "merge with external sorter"
                                   : "no spilled data to merge")
              << std::endl;
#endif
    RETURN_NOT_OK(MakeResultIterator(schema, out));
    final_result_iter_ = *out;
    if (external_sorter_) {
      if (*out) {
        RETURN_NOT_OK(external_sorter_->Enqueue(
            std::dynamic_pointer_cast<SorterResultIterator>(*out)));
      }
      RETURN_NOT_OK(external_sorter_->MakeResultIterator(schema, out));
    }
    return arrow::Status::OK();
  }

  void Sort(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end) {
    int keys_num = sort_directions_.size();
    auto comp = [this, &keys_num](const ArrayItemIndexS& x, const ArrayItemIndexS& y) {
      return compareRow(x.array_id, x.id, y.array_id, y.id, keys_num);
    };
    gfx::timsort(indices_begin, indices_begin + items_total_, comp);
  }

  void Partition(ArrayItemIndexS* indices_begin, ArrayItemIndexS* indices_end) {
    int64_t indices_i = 0;
    int64_t indices_null = 0;
    ARROW_CHECK_LE(num_batches_, 64 * 1024);
    for (int array_id = 0; array_id < num_batches_; array_id++) {
      ARROW_CHECK_LE(length_list_[array_id], 64 * 1024);
      for (int64_t i = 0; i < length_list_[array_id]; i++) {
        (indices_begin + indices_i)->array_id = array_id;
        (indices_begin + indices_i)->id = i;
        indices_i++;
      }
    }
  }

  arrow::Status FinishInternal(std::shared_ptr<FixedSizeBinaryArray>* out) {
    // initiate buffer for all arrays
    std::shared_ptr<arrow::Buffer> indices_buf;
    int64_t buf_size = items_total_ * sizeof(ArrayItemIndexS);
    auto maybe_buffer = arrow::AllocateBuffer(buf_size, ctx_->memory_pool());
    indices_buf = *std::move(maybe_buffer);
    ArrayItemIndexS* indices_begin =
        reinterpret_cast<ArrayItemIndexS*>(indices_buf->mutable_data());
    ArrayItemIndexS* indices_end = indices_begin + items_total_;
    // do partition and sort here
    Partition(indices_begin, indices_end);
    if (key_projector_) {
      std::vector<int> projected_key_idx_list;
      for (int i = 0; i < projected_field_list_.size(); i++) {
        projected_key_idx_list.push_back(i);
      }
      MakeCmpFunction(projected_, projected_field_list_, projected_key_idx_list,
                      sort_directions_, nulls_order_, NaN_check_, comparators_,
                      cmp_functions_);
    } else {
      MakeCmpFunction(cached_, key_field_list_, key_index_list_, sort_directions_,
                      nulls_order_, NaN_check_, comparators_, cmp_functions_);
    }
    Sort(indices_begin, indices_end);
    std::shared_ptr<arrow::FixedSizeBinaryType> out_type;
    RETURN_NOT_OK(
        MakeFixedSizeBinaryType(sizeof(ArrayItemIndexS) / sizeof(int32_t), &out_type));
    RETURN_NOT_OK(MakeFixedSizeBinaryArray(out_type, items_total_, indices_buf, out));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    if (cached_.empty()) return arrow::Status::OK();
    std::shared_ptr<FixedSizeBinaryArray> indices_out;
    RETURN_NOT_OK(FinishInternal(&indices_out));
    *out = std::make_shared<SorterResultIterator>(ctx_, schema, indices_out, cached_);
    return arrow::Status::OK();
  }

 private:
  std::vector<arrow::ArrayVector> cached_;
  std::vector<arrow::ArrayVector> projected_;
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<arrow::Schema> result_schema_;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::vector<std::shared_ptr<arrow::Field>> key_field_list_;
  std::vector<std::shared_ptr<arrow::Field>> projected_field_list_;
  std::vector<bool> nulls_order_;
  std::vector<bool> sort_directions_;
  std::vector<int> key_index_list_;
  bool NaN_check_;
  std::vector<int64_t> length_list_;
  uint64_t num_batches_ = 0;
  uint64_t items_total_ = 0;
  int col_num_;
  uint64_t memory_threshold_ = -1;
  uint64_t cache_size_total_ = 0;
  std::shared_ptr<ExternalSorter> external_sorter_;
  std::vector<std::shared_ptr<ComparatorBase>> comparators_;
  std::vector<std::function<void(int, int, int64_t, int64_t, int&)>> cmp_functions_;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> final_result_iter_;
};

arrow::Status SortArraysToIndicesKernel::Make(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, std::vector<bool> nulls_order, bool NaN_check,
    bool do_codegen, int result_type, std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SortArraysToIndicesKernel>(
      ctx, result_schema, sort_key_node, key_field_list, sort_directions, nulls_order,
      NaN_check, do_codegen, result_type);
  return arrow::Status::OK();
}
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::TimestampType)

SortArraysToIndicesKernel::SortArraysToIndicesKernel(
    arrow::compute::ExecContext* ctx, std::shared_ptr<arrow::Schema> result_schema,
    gandiva::NodeVector sort_key_node,
    std::vector<std::shared_ptr<arrow::Field>> key_field_list,
    std::vector<bool> sort_directions, std::vector<bool> nulls_order, bool NaN_check,
    bool do_codegen, int result_type) {
  // represents whether need to projection for sort keys
  bool pre_processed_key_ = false;
  gandiva::NodePtr key_project;
  gandiva::ExpressionVector key_project_exprs;

  for (auto node : sort_key_node) {
    std::shared_ptr<TypedNodeVisitor> node_visitor;
    THROW_NOT_OK(MakeTypedNodeVisitor(node, &node_visitor));
    if (node_visitor->GetResultType() != TypedNodeVisitor::FieldNode) {
      // if projection is needed
      pre_processed_key_ = true;
      break;
    }
  }
  // If not all key_node are FieldNode, we need to do projection
  std::shared_ptr<gandiva::Projector> key_projector;
  std::vector<std::shared_ptr<arrow::DataType>> projected_types;
  if (pre_processed_key_) {
    key_project_exprs = GetGandivaKernel(sort_key_node);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    THROW_NOT_OK(gandiva::Projector::Make(result_schema, key_project_exprs, configuration,
                                          &key_projector));
    for (const auto& expr : key_project_exprs) {
      auto key_type = expr->root()->return_type();
      projected_types.push_back(key_type);
    }
  }

  if (key_field_list.size() == 1 && result_schema->num_fields() == 1 &&
      key_field_list[0]->type()->id() != arrow::Type::STRING &&
      key_field_list[0]->type()->id() != arrow::Type::BOOL && !pre_processed_key_) {
    // Will use SortInplace when sorting for one non-string and non-boolean col
#ifdef DEBUG
    std::cout << "UseSortInplace" << std::endl;
#endif
    if (key_field_list[0]->type()->id() == arrow::Type::DECIMAL128) {
      impl_.reset(new SortInplaceKernel<arrow::Decimal128Type, arrow::Decimal128>(
          ctx, result_schema, key_projector, sort_directions, nulls_order, NaN_check));
    } else {
      switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                               \
  case InType::type_id: {                                                             \
    using CType = typename arrow::TypeTraits<InType>::CType;                          \
    impl_.reset(new SortInplaceKernel<InType, CType>(                                 \
        ctx, result_schema, key_projector, sort_directions, nulls_order, NaN_check)); \
  } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
        default: {
          std::cout << "SortInplaceKernel type not supported, type is "
                    << key_field_list[0]->type() << std::endl;
        } break;
      }
    }
  } else if (key_field_list.size() == 1 && result_schema->num_fields() >= 1) {
    // Will use SortOnekey when:
    // 1. sorting for one col with payload 2. sorting for one string col or one
    // bool col
#ifdef DEBUG
    std::cout << "UseSortOneKey" << std::endl;
#endif
    if (pre_processed_key_) {
      // if needs projection, will use projected type for key col
      if (projected_types[0]->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType>(
            ctx, result_schema, key_projector, key_field_list, sort_directions,
            nulls_order, NaN_check));
      } else if (projected_types[0]->id() == arrow::Type::DECIMAL128) {
        impl_.reset(new SortOnekeyKernel<arrow::Decimal128Type>(
            ctx, result_schema, key_projector, key_field_list, sort_directions,
            nulls_order, NaN_check));
      } else {
        switch (projected_types[0]->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    using CType = typename arrow::TypeTraits<InType>::CType;                    \
    impl_.reset(new SortOnekeyKernel<InType>(ctx, result_schema, key_projector, \
                                             key_field_list, sort_directions,   \
                                             nulls_order, NaN_check));          \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    } else {
      // if no projection, will use the original type for key col
      if (key_field_list[0]->type()->id() == arrow::Type::STRING) {
        impl_.reset(new SortOnekeyKernel<arrow::StringType>(
            ctx, result_schema, key_projector, key_field_list, sort_directions,
            nulls_order, NaN_check));
      } else if (key_field_list[0]->type()->id() == arrow::Type::DECIMAL128) {
        impl_.reset(new SortOnekeyKernel<arrow::Decimal128Type>(
            ctx, result_schema, key_projector, key_field_list, sort_directions,
            nulls_order, NaN_check));
      } else {
        switch (key_field_list[0]->type()->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    using CType = typename arrow::TypeTraits<InType>::CType;                    \
    impl_.reset(new SortOnekeyKernel<InType>(ctx, result_schema, key_projector, \
                                             key_field_list, sort_directions,   \
                                             nulls_order, NaN_check));          \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "SortOnekeyKernel type not supported, type is "
                      << key_field_list[0]->type() << std::endl;
          } break;
        }
      }
    }
  } else {
    if (do_codegen) {
      // Will use Sort with Codegen for multiple-key sort
      impl_.reset(new SortArraysCodegenKernel(ctx, result_schema, key_projector,
                                              projected_types, key_field_list,
                                              sort_directions, nulls_order, NaN_check));
    } else {
      // Will use Sort without Codegen for multiple-key sort
      impl_.reset(new SortMultiplekeyKernel(ctx, result_schema, key_projector,
                                            projected_types, key_field_list,
                                            sort_directions, nulls_order, NaN_check));
    }
  }
  kernel_name_ = "SortArraysToIndicesKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status SortArraysToIndicesKernel::Evaluate(ArrayList& in) {
  const std::lock_guard<std::mutex> lock(spill_lck_);
  RETURN_NOT_OK(impl_->Evaluate(in));
  RETURN_NOT_OK(impl_->MaybeSpill());
  return arrow::Status::OK();
}

arrow::Status SortArraysToIndicesKernel::Spill(int64_t size, int64_t* spilled_size) {
  if (in_spilling_) {
    *spilled_size = 0;
    return arrow::Status::OK();
  }
  const std::lock_guard<std::mutex> lock(spill_lck_);
  in_spilling_ = true;
  auto status = impl_->Spill(size, spilled_size);
  in_spilling_ = false;
  return status;
}

arrow::Status SortArraysToIndicesKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  const std::lock_guard<std::mutex> lock(spill_lck_);
  return impl_->MergeSpilledAndMakeResultIterator(schema, out);
}

arrow::Status SortArraysToIndicesKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<SortRelation>>* out) {
  const std::lock_guard<std::mutex> lock(spill_lck_);
  return impl_->MergeSpilledAndMakeResultIterator(schema, out);
}

std::string SortArraysToIndicesKernel::GetSignature() { return impl_->GetSignature(); }

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
