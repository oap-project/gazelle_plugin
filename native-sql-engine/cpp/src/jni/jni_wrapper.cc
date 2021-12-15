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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/jniutil/jni_util.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <arrow/util/iterator.h>
#include <jni.h>
#include <malloc.h>

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "codegen/code_generator_factory.h"
#include "codegen/common/hash_relation.h"
#include "codegen/common/result_iterator.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
#include "operators/columnar_to_row_converter.h"
#include "operators/row_to_columnar_converter.h"
#include "proto/protobuf_utils.h"
#include "shuffle/splitter.h"

namespace {

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
    env->ThrowNew(runtime_exception_class, e.what()); \
    return fallback_expr;                             \
  }
// macro ended

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    ThrowPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result, const std::string& message) {
  if (!result.status().ok()) {
    ThrowPendingException(message + " - " + result.status().message());
  }
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

void JniAssertOkOrThrow(arrow::Status status, const std::string& message) {
  if (!status.ok()) {
    ThrowPendingException(message + " - " + status.message());
  }
}

void JniThrow(const std::string& message) { ThrowPendingException(message); }

}  // namespace

namespace types {
class ExpressionList;
}  // namespace types

static jclass serializable_obj_builder_class;
static jmethodID serializable_obj_builder_constructor;

static jclass byte_array_class;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass serialized_record_batch_iterator_class;
static jclass metrics_builder_class;
static jmethodID metrics_builder_constructor;

static jmethodID serialized_record_batch_iterator_hasNext;
static jmethodID serialized_record_batch_iterator_next;

static jclass arrow_columnar_to_row_info_class;
static jmethodID arrow_columnar_to_row_info_constructor;

using arrow::jni::ConcurrentMap;

static jint JNI_VERSION = JNI_VERSION_1_8;

using CodeGenerator = sparkcolumnarplugin::codegen::CodeGenerator;
using ColumnarToRowConverter = sparkcolumnarplugin::columnartorow::ColumnarToRowConverter;
using RowToColumnarConverter = sparkcolumnarplugin::rowtocolumnar::RowToColumnarConverter;
static arrow::jni::ConcurrentMap<std::shared_ptr<CodeGenerator>> handler_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIteratorBase>>
    batch_iterator_holder_;

using sparkcolumnarplugin::shuffle::SplitOptions;
using sparkcolumnarplugin::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<arrow::Schema>>
    decompression_schema_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>>
    columnar_to_row_converter_holder_;

std::shared_ptr<CodeGenerator> GetCodeGenerator(JNIEnv* env, jlong id) {
  auto handler = handler_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

std::shared_ptr<ResultIteratorBase> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

template <typename T>
std::shared_ptr<ResultIterator<T>> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = GetBatchIterator(env, id);
  return std::dynamic_pointer_cast<ResultIterator<T>>(handler);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FromBytes(
    JNIEnv* env, std::shared_ptr<arrow::Schema> schema, jbyteArray bytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                        arrow::jniutil::DeserializeUnsafeFromJava(env, schema, bytes))
  return batch;
}

arrow::Result<jbyteArray> ToBytes(JNIEnv* env,
                                  std::shared_ptr<arrow::RecordBatch> batch) {
  ARROW_ASSIGN_OR_RAISE(jbyteArray bytes,
                        arrow::jniutil::SerializeUnsafeFromNative(env, batch))
  return bytes;
}

class JavaRecordBatchIterator {
 public:
  explicit JavaRecordBatchIterator(JavaVM* vm,
                                   jobject java_serialized_record_batch_iterator,
                                   std::shared_ptr<arrow::Schema> schema)
      : vm_(vm),
        java_serialized_record_batch_iterator_(java_serialized_record_batch_iterator),
        schema_(std::move(schema)) {}

  // singleton, avoid stack instantiation
  JavaRecordBatchIterator(const JavaRecordBatchIterator& itr) = delete;
  JavaRecordBatchIterator(JavaRecordBatchIterator&& itr) = delete;

  virtual ~JavaRecordBatchIterator() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) == JNI_OK) {
#ifdef DEBUG
      std::cout << "DELETING GLOBAL ITERATOR REF "
                << reinterpret_cast<long>(java_serialized_record_batch_iterator_) << "..."
                << std::endl;
#endif
      env->DeleteGlobalRef(java_serialized_record_batch_iterator_);
    }
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
#ifdef DEBUG
    std::cout << "PICKING ITERATOR REF "
              << reinterpret_cast<long>(java_serialized_record_batch_iterator_) << "..."
              << std::endl;
#endif
    if (!env->CallBooleanMethod(java_serialized_record_batch_iterator_,
                                serialized_record_batch_iterator_hasNext)) {
      return nullptr;  // stream ended
    }
    auto bytes = (jbyteArray)env->CallObjectMethod(java_serialized_record_batch_iterator_,
                                                   serialized_record_batch_iterator_next);
    RETURN_NOT_OK(arrow::jniutil::CheckException(env));
    ARROW_ASSIGN_OR_RAISE(auto batch, FromBytes(env, schema_, bytes));
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject java_serialized_record_batch_iterator_;
  std::shared_ptr<arrow::Schema> schema_;
};

class JavaRecordBatchIteratorWrapper {
 public:
  explicit JavaRecordBatchIteratorWrapper(
      std::shared_ptr<JavaRecordBatchIterator> delegated)
      : delegated_(std::move(delegated)) {}

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() { return delegated_->Next(); }

 private:
  std::shared_ptr<JavaRecordBatchIterator> delegated_;
};

// See Java class
// org/apache/arrow/dataset/jni/NativeSerializedRecordBatchIterator
//
arrow::Result<arrow::RecordBatchIterator> MakeJavaRecordBatchIterator(
    JavaVM* vm, jobject java_serialized_record_batch_iterator,
    std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<arrow::Schema> schema_moved = std::move(schema);
  arrow::RecordBatchIterator itr = arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>(
      JavaRecordBatchIteratorWrapper(std::make_shared<JavaRecordBatchIterator>(
          vm, java_serialized_record_batch_iterator, schema_moved)));
  return itr;
}

using FileSystem = arrow::fs::FileSystem;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");
  unsupportedoperation_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  serializable_obj_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/NativeSerializableObject;");
  serializable_obj_builder_constructor =
      GetMethodID(env, serializable_obj_builder_class, "<init>", "([J[I)V");

  byte_array_class = CreateGlobalClassReference(env, "[B");
  split_result_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/SplitResult;");
  split_result_constructor =
      GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J)V");

  metrics_builder_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/MetricsObject;");
  metrics_builder_constructor =
      GetMethodID(env, metrics_builder_class, "<init>", "([J[J)V");

  serialized_record_batch_iterator_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/execution/ColumnarNativeIterator;");
  serialized_record_batch_iterator_hasNext =
      GetMethodID(env, serialized_record_batch_iterator_class, "hasNext", "()Z");
  serialized_record_batch_iterator_next =
      GetMethodID(env, serialized_record_batch_iterator_class, "next", "()[B");

  arrow_columnar_to_row_info_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/ArrowColumnarToRowInfo;");
  arrow_columnar_to_row_info_constructor =
      GetMethodID(env, arrow_columnar_to_row_info_class, "<init>", "(J[J[JJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(unsupportedoperation_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(serializable_obj_builder_class);
  env->DeleteGlobalRef(split_result_class);
  env->DeleteGlobalRef(serialized_record_batch_iterator_class);
  env->DeleteGlobalRef(arrow_columnar_to_row_info_class);

  env->DeleteGlobalRef(byte_array_class);

  handler_holder_.Clear();
  batch_iterator_holder_.Clear();
  shuffle_splitter_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
  decompression_schema_holder_.Clear();
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(
    JNIEnv* env, jobject obj, jstring pathObj) {
  JNI_METHOD_START
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env, jobject obj, jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetSortSpillThreshold(
    JNIEnv* env, jobject obj, jlong spill_size) {
  JNI_METHOD_START
  setenv("NATIVESQL_MAX_MEMORY_SIZE", std::to_string(spill_size).c_str(), 1);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(
    JNIEnv* env, jobject obj, jboolean is_enable) {
  setenv("NATIVESQL_METRICS_TIME", (is_enable ? "true" : "false"), 1);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeBuild(
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray schema_arr,
    jbyteArray exprs_arr, jbyteArray res_schema_arr,
    jboolean return_when_finish = false) {
  JNI_METHOD_START

  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  JniAssertOkOrThrow(MakeExprVector(env, exprs_arr, &expr_vector, &ret_types),
                     "failed to parse expressions protobuf");

  if (res_schema_arr != nullptr) {
    std::shared_ptr<arrow::Schema> resSchema;
    JniAssertOkOrThrow(MakeSchema(env, res_schema_arr, &resSchema),
                       "failed to readSchema");
    ret_types = resSchema->fields();
  }

#ifdef DEBUG
  for (auto expr : expr_vector) {
    std::cout << expr->ToString() << std::endl;
  }
#endif

  std::shared_ptr<CodeGenerator> handler;
  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    env->ThrowNew(illegal_argument_exception_class,
                  "Memory pool does not exist or has been closed");
    return -1;
  }
  JniAssertOkOrThrow(
      sparkcolumnarplugin::codegen::CreateCodeGenerator(
          pool, schema, expr_vector, ret_types, &handler, return_when_finish),
      "nativeBuild: failed to create CodeGenerator");

#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  std::cout << "build native Evaluator " << handler->ToString()
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << batch_holder_size << "]" << std::endl;
#endif

  return handler_holder_.Insert(std::move(handler));
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeBuildWithFinish(
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray schema_arr,
    jbyteArray exprs_arr, jbyteArray finish_exprs_arr) {
  JNI_METHOD_START

  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  JniAssertOkOrThrow(MakeExprVector(env, exprs_arr, &expr_vector, &ret_types),
                     "failed to parse expressions protobuf");

  gandiva::ExpressionVector finish_expr_vector;
  gandiva::FieldVector finish_ret_types;
  JniAssertOkOrThrow(
      MakeExprVector(env, finish_exprs_arr, &finish_expr_vector, &finish_ret_types),
      "failed to parse expressions protobuf");

  std::shared_ptr<CodeGenerator> handler;
  arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }
  JniAssertOkOrThrow(
      sparkcolumnarplugin::codegen::CreateCodeGenerator(
          pool, schema, expr_vector, ret_types, &handler, true, finish_expr_vector),
      "nativeBuild: failed to create CodeGenerator");
  return handler_holder_.Insert(std::move(handler));
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetReturnFields(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  JniAssertOkOrThrow(handler->SetResSchema(schema), "failed to set result schema");
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeClose(JNIEnv* env,
                                                                        jobject obj,
                                                                        jlong id) {
  JNI_METHOD_START
  auto handler = GetCodeGenerator(env, id);
  if (handler.use_count() > 2) {
    std::cout << "evaluator ptr use count is " << handler.use_count() - 1 << std::endl;
  }
#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  std::cout << "close native Evaluator " << handler->ToString()
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << batch_holder_size << "]" << std::endl;
#endif
  handler_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSpill(
    JNIEnv* env, jobject obj, jlong id, jlong size, jboolean call_by_self) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  jlong spilled_size;
  JniAssertOkOrThrow(handler->Spill(size, call_by_self, &spilled_size),
                     "nativeSpill: spill failed");
  return spilled_size;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getSchema(&schema));

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in));

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  JniAssertOkOrThrow(handler->evaluate(in, &out), "nativeEvaluate: evaluate failed");

  std::shared_ptr<arrow::Schema> res_schema;
  JniAssertOkOrThrow(handler->getResSchema(&res_schema));
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    jbyteArray serialized_record_batch =
        JniGetOrThrow(ToBytes(env, record_batch), "Error deserializing message");
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return serialized_record_batch_array;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithIterator(
    JNIEnv* env, jobject obj, jlong id, jobject itr) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getSchema(&schema));

  // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
  // TODO Release this in JNI Unload or dependent object's destructor
  jobject itr2 = env->NewGlobalRef(itr);
  JniAssertOkOrThrow(handler->evaluate(
      std::move(JniGetOrThrow(MakeJavaRecordBatchIterator(vm, itr2, schema),
                              "nativeEvaluate: error making java iterator"))));
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeGetSignature(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  return env->NewStringUTF((handler->GetSignature()).c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithSelection(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jint selection_vector_count, jlong selection_vector_buf_addr,
    jlong selection_vector_buf_size) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getSchema(&schema));

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in));

  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  JniAssertOkOrThrow(handler->evaluate(selection_array, in, &out),
                     "nativeEvaluate: evaluate failed");

  std::shared_ptr<arrow::Schema> res_schema;
  JniAssertOkOrThrow(handler->getResSchema(&res_schema));
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    jbyteArray serialized_record_batch =
        JniGetOrThrow(ToBytes(env, record_batch), "Error deserializing message");
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return serialized_record_batch_array;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMember(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getSchema(&schema));

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in));

  JniAssertOkOrThrow(handler->SetMember(in), "nativeEvaluate: evaluate failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  JNI_METHOD_END()
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinish(JNIEnv* env,
                                                                         jobject obj,
                                                                         jlong id) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  JniAssertOkOrThrow(handler->finish(&out), "nativeFinish: finish failed");

  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getResSchema(&schema));

  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    jbyteArray serialized_record_batch =
        JniGetOrThrow(ToBytes(env, record_batch), "Error deserializing message");
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  return serialized_record_batch_array;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinishByIterator(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<ResultIteratorBase> out;
  JniAssertOkOrThrow(handler->finish(&out), "nativeFinishForIterator: finish failed");

  return batch_iterator_holder_.Insert(std::move(out));
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetDependency(
    JNIEnv* env, jobject obj, jlong id, jlong iter_id, int index) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, iter_id);
  JniAssertOkOrThrow(handler->SetDependency(iter, index),
                     "nativeSetDependency: finish failed");
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeHasNext(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    JniThrow(error_message);
  }
  return iter->HasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeFetchMetrics(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator(env, id);
  std::shared_ptr<Metrics> metrics;
  JniAssertOkOrThrow(iter->GetMetrics(&metrics));
  auto output_length_list = env->NewLongArray(metrics->num_metrics);
  auto process_time_list = env->NewLongArray(metrics->num_metrics);
  env->SetLongArrayRegion(output_length_list, 0, metrics->num_metrics,
                          metrics->output_length);
  env->SetLongArrayRegion(process_time_list, 0, metrics->num_metrics,
                          metrics->process_time);
  return env->NewObject(metrics_builder_class, metrics_builder_constructor,
                        output_length_list, process_time_list);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeNext(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  if (!iter->HasNext()) return nullptr;
  JniAssertOkOrThrow(iter->Next(&out), "nativeNext: get Next() failed");
  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, out), "Error deserializing message");
  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeNextHashRelation(JNIEnv* env,
                                                                   jobject obj,
                                                                   jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator<HashRelation>(env, id);
  std::shared_ptr<HashRelation> out;
  JniAssertOkOrThrow(iter->Next(&out), "nativeNext: get Next() failed");

  int src_sizes[4];
  long src_addrs[4];
  arrow::Status status = out->UnsafeGetHashTableObject(src_addrs, src_sizes);
  if (!status.ok()) {
    auto memory_addrs = env->NewLongArray(0);
    auto sizes = env->NewIntArray(0);
    return env->NewObject(serializable_obj_builder_class,
                          serializable_obj_builder_constructor, memory_addrs, sizes);
  }
  auto memory_addrs = env->NewLongArray(4);
  auto sizes = env->NewIntArray(4);
  env->SetLongArrayRegion(memory_addrs, 0, 4, src_addrs);
  env->SetIntArrayRegion(sizes, 0, 4, src_sizes);
  return env->NewObject(serializable_obj_builder_class,
                        serializable_obj_builder_constructor, memory_addrs, sizes);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeSetHashRelation(
    JNIEnv* env, jobject obj, jlong id, jlongArray memory_addrs, jintArray sizes) {
  JNI_METHOD_START
  auto iter = GetBatchIterator<HashRelation>(env, id);
  std::shared_ptr<HashRelation> out;
  JniAssertOkOrThrow(iter->Next(&out));

  int in_len = env->GetArrayLength(memory_addrs);
  jlong* in_addrs = env->GetLongArrayElements(memory_addrs, 0);
  jint* in_sizes = env->GetIntArrayElements(sizes, 0);
  JniAssertOkOrThrow(out->UnsafeSetHashTableObject(in_len, in_addrs, in_sizes));
  env->ReleaseLongArrayElements(memory_addrs, in_addrs, JNI_ABORT);
  env->ReleaseIntArrayElements(sizes, in_sizes, JNI_ABORT);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeProcess(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    JniThrow(error_message);
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch),
      "failed to readSchema");
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  JniAssertOkOrThrow(iter->Process(in, &out),
                     "nativeProcess: ResultIterator process next failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, out), "Error deserializing message");

  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    JniThrow(error_message);
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch));
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::shared_ptr<arrow::RecordBatch> out;
  JniAssertOkOrThrow(iter->Process(in, &out, selection_array),
                     "nativeProcess: ResultIterator process next failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, out), "Error deserializing message");

  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOne(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    JniThrow(error_message);
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch));
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  if (iter) {
    JniAssertOkOrThrow(iter->ProcessAndCacheOne(in),
                       "nativeProcessAndCache: ResultIterator process next failed");
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOneWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema), "failed to readSchema");
  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    JniThrow(error_message);
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch));
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);
  JniAssertOkOrThrow(iter->ProcessAndCacheOne(in, selection_array),
                     "nativeProcessAndCache: ResultIterator process next failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeSetDependencies(
    JNIEnv* env, jobject this_obj, jlong id, jlongArray ids) {
  JNI_METHOD_START
  int ids_size = env->GetArrayLength(ids);
  long* ids_data = env->GetLongArrayElements(ids, 0);
  std::vector<std::shared_ptr<ResultIteratorBase>> dependent_batch_list;
  for (int i = 0; i < ids_size; i++) {
    auto handler = GetBatchIterator(env, ids_data[i]);
    dependent_batch_list.push_back(handler);
  }
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  JniAssertOkOrThrow(iter->SetDependencies(dependent_batch_list),
                     "nativeSetDependencies");

  env->ReleaseLongArrayElements(ids, ids_data, JNI_ABORT);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
  JNI_METHOD_START
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeSpill(
    JNIEnv* env, jobject obj, jlong splitter_id, jlong size, jboolean call_by_self) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }

  jlong spilled_size;
  JniAssertOkOrThrow(splitter->SpillFixedSize(size, &spilled_size),
                     "(shuffle) nativeSpill: spill failed");
  return spilled_size;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate2(
    JNIEnv* env, jobject obj, jlong id, jbyteArray bytes) {
  JNI_METHOD_START
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  JniAssertOkOrThrow(handler->getSchema(&schema));

  auto maybe_batch = FromBytes(env, schema, bytes);
  auto in = std::move(*maybe_batch);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  JniAssertOkOrThrow(handler->evaluate(in, &out), "nativeEvaluate: evaluate failed");

  std::shared_ptr<arrow::Schema> res_schema;
  JniAssertOkOrThrow(handler->getResSchema(&res_schema));
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    jbyteArray serialized_record_batch =
        JniGetOrThrow(ToBytes(env, record_batch), "Error deserializing message");
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  return serialized_record_batch_array;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jbyteArray schema_arr, jbyteArray expr_arr, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs,
    jstring local_dirs_jstr, jboolean prefer_spill, jlong memory_pool_id) {
  JNI_METHOD_START
  if (partitioning_name_jstr == NULL) {
    JniThrow(std::string("Short partitioning name can't be null"));
    return 0;
  }
  if (schema_arr == NULL) {
    JniThrow(std::string("Make splitter schema can't be null"));
  }
  if (data_file_jstr == NULL) {
    JniThrow(std::string("Shuffle DataFile can't be null"));
  }
  if (local_dirs_jstr == NULL) {
    JniThrow(std::string("Shuffle DataFile can't be null"));
  }

  auto partitioning_name_c = env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
  auto partitioning_name = std::string(partitioning_name_c);
  env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

  auto splitOptions = SplitOptions::Defaults();
  splitOptions.prefer_spill = prefer_spill;
  if (buffer_size > 0) {
    splitOptions.buffer_size = buffer_size;
  }
  if (num_sub_dirs > 0) {
    splitOptions.num_sub_dirs = num_sub_dirs;
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitOptions.compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
  splitOptions.data_file = std::string(data_file_c);
  env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }
  splitOptions.memory_pool = pool;

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  gandiva::ExpressionVector expr_vector = {};
  if (expr_arr != NULL) {
    gandiva::FieldVector ret_types;
    JniAssertOkOrThrow(MakeExprVector(env, expr_arr, &expr_vector, &ret_types),
                       "Failed to parse expressions protobuf");
  }

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cout << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = env->GetMethodID(cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, mid_getid);
    splitOptions.thread_id = (int64_t)sid;
  }

  jclass tc_cls = env->FindClass("org/apache/spark/TaskContext");
  jmethodID get_tc_mid =
      env->GetStaticMethodID(tc_cls, "get", "()Lorg/apache/spark/TaskContext;");
  jobject tc_obj = env->CallStaticObjectMethod(tc_cls, get_tc_mid);
  if (tc_obj == NULL) {
    std::cout << "TaskContext.get() return NULL" << std::endl;
  } else {
    jmethodID get_tsk_attmpt_mid = env->GetMethodID(tc_cls, "taskAttemptId", "()J");
    jlong attmpt_id = env->CallLongMethod(tc_obj, get_tsk_attmpt_mid);
    splitOptions.task_attempt_id = (int64_t)attmpt_id;
  }

  auto splitter =
      JniGetOrThrow(Splitter::Make(partitioning_name, std::move(schema), num_partitions,
                                   expr_vector, std::move(splitOptions)),
                    "Failed create native shuffle splitter");

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));

  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_setCompressType(
    JNIEnv* env, jobject, jlong splitter_id, jstring compression_type_jstr) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      JniAssertOkOrThrow(
          splitter->SetCompressType(compression_type_result.MoveValueUnsafe()));
    }
  }
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jboolean first_record_batch) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    JniThrow("Native split: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native split: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("Native split: length of buf_addrs and buf_sizes mismatch");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  JniAssertOkOrThrow(
      MakeRecordBatch(splitter->input_schema(), num_rows, (int64_t*)in_buf_addrs,
                      (int64_t*)in_buf_sizes, in_bufs_len, &in),
      "Native split: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (first_record_batch) {
    return splitter->CompressedSize(*in);
  }
  JniAssertOkOrThrow(splitter->Split(*in), "Native split: splitter split failed");
  return -1L;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }

  JniAssertOkOrThrow(splitter->Stop(), "Native split: splitter stop failed");

  const auto& partition_length = splitter->PartitionLengths();
  auto partition_length_arr = env->NewLongArray(partition_length.size());
  auto src = reinterpret_cast<const jlong*>(partition_length.data());
  env->SetLongArrayRegion(partition_length_arr, 0, partition_length.size(), src);
  jobject split_result = env->NewObject(
      split_result_class, split_result_constructor, splitter->TotalComputePidTime(),
      splitter->TotalWriteTime(), splitter->TotalSpillTime(),
      splitter->TotalCompressTime(), splitter->TotalBytesWritten(),
      splitter->TotalBytesSpilled(), partition_length_arr);

  return split_result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  shuffle_splitter_holder_.Erase(splitter_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema));

  return decompression_schema_holder_.Insert(schema);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring compression_type_jstr,
    jint num_rows, jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask) {
  JNI_METHOD_START
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);
  if (!schema) {
    std::string error_message =
        "Invalid schema holder id " + std::to_string(schema_holder_id);
    JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    JniThrow("Native decompress: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native decompress: buf_sizes can't be null");
  }
  if (buf_mask == NULL) {
    JniThrow("Native decompress: buf_mask can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("Native decompress: length of buf_addrs and buf_sizes mismatch");
  }

  auto compression_type = arrow::Compression::UNCOMPRESSED;
  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  // make buffers from raws
  auto in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  auto in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);
  auto in_buf_mask = env->GetLongArrayElements(buf_mask, JNI_FALSE);

  std::vector<std::shared_ptr<arrow::Buffer>> input_buffers;
  input_buffers.reserve(in_bufs_len);
  for (auto buffer_idx = 0; buffer_idx < in_bufs_len; buffer_idx++) {
    input_buffers.push_back(std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(in_buf_addrs[buffer_idx]),
        in_buf_sizes[buffer_idx]));
  }

  // decompress buffers
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.use_threads = false;
  JniAssertOkOrThrow(
      DecompressBuffers(compression_type, options, (uint8_t*)in_buf_mask, input_buffers,
                        schema->fields()),
      "ShuffleDecompressionJniWrapper_decompress, failed to decompress buffers");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  // make arrays from buffers
  std::shared_ptr<arrow::RecordBatch> rb;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, input_buffers, input_buffers.size(), &rb),
      "ShuffleDecompressionJniWrapper_decompress, failed to MakeRecordBatch upon "
      "buffers");
  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, rb), "Error deserializing message");

  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_pool_id) {
  JNI_METHOD_START
  if (schema_arr == NULL) {
    JniThrow("Native convert columnar to row schema can't be null");
  }
  if (buf_addrs == NULL) {
    JniThrow("Native convert columnar to row: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native convert columnar to row: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow(
        "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch");
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  JniAssertOkOrThrow(MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                     (int64_t*)in_buf_sizes, in_bufs_len, &rb),
                     "Native convert columnar to row: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  // convert the record batch to spark unsafe row.
  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }

  std::shared_ptr<ColumnarToRowConverter> columnar_to_row_converter =
      std::make_shared<ColumnarToRowConverter>(rb, pool);
  JniAssertOkOrThrow(columnar_to_row_converter->Init(),
                     "Native convert columnar to row: Init "
                     "ColumnarToRowConverter failed");
  JniAssertOkOrThrow(
      columnar_to_row_converter->Write(),
      "Native convert columnar to row: ColumnarToRowConverter write failed");

  const auto& offsets = columnar_to_row_converter->GetOffsets();
  const auto& lengths = columnar_to_row_converter->GetLengths();
  int64_t instanceID =
      columnar_to_row_converter_holder_.Insert(columnar_to_row_converter);

  auto offsets_arr = env->NewLongArray(num_rows);
  auto offsets_src = reinterpret_cast<const jlong*>(offsets.data());
  env->SetLongArrayRegion(offsets_arr, 0, num_rows, offsets_src);
  auto lengths_arr = env->NewLongArray(num_rows);
  auto lengths_src = reinterpret_cast<const jlong*>(lengths.data());
  env->SetLongArrayRegion(lengths_arr, 0, num_rows, lengths_src);
  long address = reinterpret_cast<long>(columnar_to_row_converter->GetBufferAddress());

  jobject arrow_columnar_to_row_info = env->NewObject(
      arrow_columnar_to_row_info_class, arrow_columnar_to_row_info_constructor,
      instanceID, offsets_arr, lengths_arr, address);
  return arrow_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env, jobject, jlong instance_id) {
  JNI_METHOD_START
  columnar_to_row_converter_holder_.Erase(instance_id);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ArrowRowToColumnarJniWrapper_nativeConvertRowToColumnar(
    JNIEnv* env, jobject, jbyteArray schema_arr,  jlongArray row_length, jlong memory_address, 
      jlong memory_pool_id) {
      std::cout << "Calling the nativeConvertRowToColumnar method" << "\n";

  if (schema_arr == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert row to columnar schema can't be null").c_str());
    return NULL;
  }
  if (row_length == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert row to columnar: buf_addrs can't be null").c_str());
    return NULL;
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);
  jlong* in_row_length = env->GetLongArrayElements(row_length, JNI_FALSE);
  uint8_t* address = reinterpret_cast<uint8_t*>(memory_address);
  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  int num_rows = env->GetArrayLength(row_length);
  int num_columnars = schema->num_fields();
  std::shared_ptr<RowToColumnarConverter> row_to_columnar_converter =
      std::make_shared<RowToColumnarConverter>(schema, num_columnars, num_rows, 
                in_row_length, address, pool);
  JniAssertOkOrThrow(row_to_columnar_converter->Init(), "Native convert Row to Columnar Init "
                     "RowToColumnarConverter failed");

  return NULL;
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocTrim(JNIEnv* env,
                                                                     jobject obj) {
  JNI_METHOD_START
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocStats(JNIEnv* env,
                                                                      jobject obj) {
  JNI_METHOD_START
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
