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
#include "proto/protobuf_utils.h"
#include "shuffle/splitter.h"

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
      GetMethodID(env, arrow_columnar_to_row_info_class, "<init>", "(J[J[J)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
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
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env, jobject obj, jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
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
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (res_schema_arr != nullptr) {
    std::shared_ptr<arrow::Schema> resSchema;
    msg = MakeSchema(env, res_schema_arr, &resSchema);
    if (!msg.ok()) {
      std::string error_message = "failed to readSchema, err msg is " + msg.message();
      env->ThrowNew(io_exception_class, error_message.c_str());
    }
    ret_types = resSchema->fields();
  }

#ifdef DEBUG
  for (auto expr : expr_vector) {
    std::cout << expr->ToString() << std::endl;
  }
#endif

  std::shared_ptr<CodeGenerator> handler;
  try {
    arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
    if (pool == nullptr) {
      env->ThrowNew(illegal_argument_exception_class,
                    "Memory pool does not exist or has been closed");
      return -1;
    }
    msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
        pool, schema, expr_vector, ret_types, &handler, return_when_finish);
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

#ifdef DEBUG
  auto handler_holder_size = handler_holder_.Size();
  auto batch_holder_size = batch_iterator_holder_.Size();
  std::cout << "build native Evaluator " << handler->ToString()
            << "\nremain refCnt [buffer|Evaluator|batchIterator] is ["
            << batch_holder_size << "]" << std::endl;
#endif

  return handler_holder_.Insert(std::move(handler));
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeBuildWithFinish(
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray schema_arr,
    jbyteArray exprs_arr, jbyteArray finish_exprs_arr) {
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector finish_expr_vector;
  gandiva::FieldVector finish_ret_types;
  msg = MakeExprVector(env, finish_exprs_arr, &finish_expr_vector, &finish_ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<CodeGenerator> handler;
  try {
    arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
    if (pool == nullptr) {
      env->ThrowNew(illegal_argument_exception_class,
                    "Memory pool does not exist or has been closed");
      return -1;
    }
    msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
        pool, schema, expr_vector, ret_types, &handler, true, finish_expr_vector);
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return handler_holder_.Insert(std::move(handler));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetReturnFields(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  msg = handler->SetResSchema(schema);
  if (!msg.ok()) {
    std::string error_message =
        "failed to set result schema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeClose(JNIEnv* env,
                                                                        jobject obj,
                                                                        jlong id) {
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
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSpill(
    JNIEnv* env, jobject obj, jlong id, jlong size, jboolean call_by_self) {
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  jlong spilled_size;
  arrow::Status status = handler->Spill(size, call_by_self, &spilled_size);
  if (!status.ok()) {
    std::string error_message =
        "nativeSpill: spill failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return -1L;
  }
  return spilled_size;
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    const arrow::Result<jbyteArray>& r = ToBytes(env, record_batch);
    if (!r.ok()) {
      std::string error_message = "Error deserializing message" + r.status().message();
      env->ThrowNew(io_exception_class, error_message.c_str());
      return nullptr;
    }
    jbyteArray serialized_record_batch = r.ValueOrDie();
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return serialized_record_batch_array;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithIterator(
    JNIEnv* env, jobject obj, jlong id, jobject itr) {
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    std::string error_message = "Unable to get JavaVM instance";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
  // TODO Release this in JNI Unload or dependent object's destructor
  jobject itr2 = env->NewGlobalRef(itr);
  arrow::Result<arrow::RecordBatchIterator> rb_itr_status =
      MakeJavaRecordBatchIterator(vm, itr2, schema);

  if (!rb_itr_status.ok()) {
    std::string error_message =
        "nativeEvaluate: error making java iterator" + rb_itr_status.status().ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  status = handler->evaluate(std::move(rb_itr_status.ValueOrDie()));

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT jstring JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeGetSignature(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  return env->NewStringUTF((handler->GetSignature()).c_str());
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithSelection(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jint selection_vector_count, jlong selection_vector_buf_addr,
    jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(selection_array, in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    const arrow::Result<jbyteArray>& r = ToBytes(env, record_batch);
    if (!r.ok()) {
      std::string error_message = "Error deserializing message" + r.status().message();
      env->ThrowNew(io_exception_class, error_message.c_str());
      return nullptr;
    }
    jbyteArray serialized_record_batch = r.ValueOrDie();
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return serialized_record_batch_array;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMember(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  status = handler->SetMember(in);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinish(JNIEnv* env,
                                                                         jobject obj,
                                                                         jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->finish(&out);

  if (!status.ok()) {
    std::string error_message =
        "nativeFinish: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> schema;
  status = handler->getResSchema(&schema);

  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    const arrow::Result<jbyteArray>& r = ToBytes(env, record_batch);
    if (!r.ok()) {
      std::string error_message = "Error deserializing message" + r.status().message();
      env->ThrowNew(io_exception_class, error_message.c_str());
      return nullptr;
    }
    jbyteArray serialized_record_batch = r.ValueOrDie();
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  return serialized_record_batch_array;
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeFinishByIterator(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<ResultIteratorBase> out;
  status = handler->finish(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeFinishForIterator: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return batch_iterator_holder_.Insert(std::move(out));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetDependency(
    JNIEnv* env, jobject obj, jlong id, jlong iter_id, int index) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, iter_id);
  status = handler->SetDependency(iter, index);
  if (!status.ok()) {
    std::string error_message =
        "nativeSetDependency: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT jboolean JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeHasNext(
    JNIEnv* env, jobject obj, jlong id) {
  auto iter = GetBatchIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return iter->HasNext();
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeFetchMetrics(
    JNIEnv* env, jobject obj, jlong id) {
  auto iter = GetBatchIterator(env, id);
  std::shared_ptr<Metrics> metrics;
  iter->GetMetrics(&metrics);
  auto output_length_list = env->NewLongArray(metrics->num_metrics);
  auto process_time_list = env->NewLongArray(metrics->num_metrics);
  env->SetLongArrayRegion(output_length_list, 0, metrics->num_metrics,
                          metrics->output_length);
  env->SetLongArrayRegion(process_time_list, 0, metrics->num_metrics,
                          metrics->process_time);
  return env->NewObject(metrics_builder_class, metrics_builder_constructor,
                        output_length_list, process_time_list);
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeNext(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  if (!iter->HasNext()) return nullptr;
  status = iter->Next(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeNext: get Next() failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  const arrow::Result<jbyteArray>& r = ToBytes(env, out);
  if (!r.ok()) {
    std::string error_message = "Error deserializing message" + r.status().message();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return nullptr;
  }
  jbyteArray serialized_record_batch = r.ValueOrDie();

  return serialized_record_batch;
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeNextHashRelation(JNIEnv* env,
                                                                   jobject obj,
                                                                   jlong id) {
  arrow::Status status;
  auto iter = GetBatchIterator<HashRelation>(env, id);
  std::shared_ptr<HashRelation> out;
  status = iter->Next(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeNext: get Next() failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int src_sizes[4];
  long src_addrs[4];
  status = out->UnsafeGetHashTableObject(src_addrs, src_sizes);
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
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeSetHashRelation(
    JNIEnv* env, jobject obj, jlong id, jlongArray memory_addrs, jintArray sizes) {
  arrow::Status status;
  auto iter = GetBatchIterator<HashRelation>(env, id);
  std::shared_ptr<HashRelation> out;
  status = iter->Next(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeSetHashRelation: get Next() failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_len = env->GetArrayLength(memory_addrs);
  jlong* in_addrs = env->GetLongArrayElements(memory_addrs, 0);
  jint* in_sizes = env->GetIntArrayElements(sizes, 0);
  out->UnsafeSetHashTableObject(in_len, in_addrs, in_sizes);
  env->ReleaseLongArrayElements(memory_addrs, in_addrs, JNI_ABORT);
  env->ReleaseIntArrayElements(sizes, in_sizes, JNI_ABORT);
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeProcess(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  status = iter->Process(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  const arrow::Result<jbyteArray>& r = ToBytes(env, out);
  if (!r.ok()) {
    std::string error_message = "Error deserializing message" + r.status().message();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return nullptr;
  }
  jbyteArray serialized_record_batch = r.ValueOrDie();

  return serialized_record_batch;
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
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
  status = iter->Process(in, &out, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  const arrow::Result<jbyteArray>& r = ToBytes(env, out);
  if (!r.ok()) {
    std::string error_message = "Error deserializing message" + r.status().message();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return nullptr;
  }
  jbyteArray serialized_record_batch = r.ValueOrDie();

  return serialized_record_batch;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOne(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  if (iter) {
    status = iter->ProcessAndCacheOne(in);
  }

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error "
        "msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_BatchIterator_nativeProcessAndCacheOneWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
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
  status = iter->ProcessAndCacheOne(in, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error "
        "msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeSetDependencies(
    JNIEnv* env, jobject this_obj, jlong id, jlongArray ids) {
  int ids_size = env->GetArrayLength(ids);
  long* ids_data = env->GetLongArrayElements(ids, 0);
  std::vector<std::shared_ptr<ResultIteratorBase>> dependent_batch_list;
  for (int i = 0; i < ids_size; i++) {
    auto handler = GetBatchIterator(env, ids_data[i]);
    dependent_batch_list.push_back(handler);
  }
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  arrow::Status status = iter->SetDependencies(dependent_batch_list);

  if (!status.ok()) {
    std::string error_message =
        "nativeSetDependencies: Error "
        "msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(ids, ids_data, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeSpill(
    JNIEnv* env, jobject obj, jlong splitter_id, jlong size, jboolean call_by_self) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return -1L;
  }

  jlong spilled_size;
  arrow::Status status = splitter->SpillFixedSize(size, &spilled_size);
  if (!status.ok()) {
    std::string error_message =
        "(shuffle) nativeSpill: spill failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return -1L;
  }
  return spilled_size;
}

JNIEXPORT jobjectArray JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate2(
    JNIEnv* env, jobject obj, jlong id, jbyteArray bytes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  auto maybe_batch = FromBytes(env, schema, bytes);
  auto in = std::move(*maybe_batch);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray serialized_record_batch_array =
      env->NewObjectArray(out.size(), byte_array_class, nullptr);
  int i = 0;
  for (const auto& record_batch : out) {
    const arrow::Result<jbyteArray>& r = ToBytes(env, record_batch);
    if (!r.ok()) {
      std::string error_message = "Error deserializing message" + r.status().message();
      env->ThrowNew(io_exception_class, error_message.c_str());
      return nullptr;
    }
    jbyteArray serialized_record_batch = r.ValueOrDie();
    env->SetObjectArrayElement(serialized_record_batch_array, i++,
                               serialized_record_batch);
  }

  return serialized_record_batch_array;
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jbyteArray schema_arr, jbyteArray expr_arr, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs,
    jstring local_dirs_jstr, jboolean prefer_spill, jlong memory_pool_id) {
  if (partitioning_name_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Short partitioning name can't be null").c_str());
    return 0;
  }
  if (schema_arr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Make splitter schema can't be null").c_str());
    return 0;
  }
  if (data_file_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Shuffle DataFile can't be null").c_str());
    return 0;
  }
  if (local_dirs_jstr == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Shuffle DataFile can't be null").c_str());
    return 0;
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

  try {
    auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
    if (pool == nullptr) {
      env->ThrowNew(illegal_argument_exception_class,
                    "Memory pool does not exist or has been closed");
      return -1;
    }
    splitOptions.memory_pool = pool;
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  gandiva::ExpressionVector expr_vector = {};
  if (expr_arr != NULL) {
    gandiva::FieldVector ret_types;
    auto status = MakeExprVector(env, expr_arr, &expr_vector, &ret_types);
    if (!status.ok()) {
      env->ThrowNew(
          illegal_argument_exception_class,
          std::string("Failed to parse expressions protobuf, error message is " +
                      status.message())
              .c_str());
      return 0;
    }
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

  auto make_result = Splitter::Make(partitioning_name, std::move(schema), num_partitions,
                                    expr_vector, std::move(splitOptions));
  if (!make_result.ok()) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Failed create native shuffle splitter, error message is " +
                              make_result.status().message())
                      .c_str());
    return 0;
  }
  auto splitter = make_result.MoveValueUnsafe();

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_setCompressType(
    JNIEnv* env, jobject, jlong splitter_id, jstring compression_type_jstr) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return;
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitter->SetCompressType(compression_type_result.MoveValueUnsafe());
    }
  }
  return;
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jboolean first_record_batch) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return -1;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native split: buf_addrs can't be null").c_str());
    return -1;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native split: buf_sizes can't be null").c_str());
    return -1;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native split: length of buf_addrs and buf_sizes mismatch").c_str());
    return -1;
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  auto status =
      MakeRecordBatch(splitter->input_schema(), num_rows, (int64_t*)in_buf_addrs,
                      (int64_t*)in_buf_sizes, in_bufs_len, &in);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native split: make record batch failed, error message is " +
                    status.message())
            .c_str());
    return -1;
  }

  if (first_record_batch) {
    return splitter->CompressedSize(*in);
  } else {
    status = splitter->Split(*in);
    if (!status.ok()) {
      // Throw IOException
      env->ThrowNew(io_exception_class,
                    std::string("Native split: splitter split failed, error message is " +
                                status.message())
                        .c_str());
    }
    return -1;
  }
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return nullptr;
  }

  auto status = splitter->Stop();

  if (!status.ok()) {
    // Throw IOException
    env->ThrowNew(io_exception_class,
                  std::string("Native split: splitter stop failed, error message is " +
                              status.message())
                      .c_str());
    return nullptr;
  }

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
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  shuffle_splitter_holder_.Erase(splitter_id);
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  return decompression_schema_holder_.Insert(schema);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring compression_type_jstr,
    jint num_rows, jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask) {
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);
  if (!schema) {
    std::string error_message =
        "Invalid schema holder id " + std::to_string(schema_holder_id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
    return nullptr;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_addrs can't be null").c_str());
    return nullptr;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_sizes can't be null").c_str());
    return nullptr;
  }
  if (buf_mask == NULL) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native decompress: buf_mask can't be null").c_str());
    return nullptr;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native decompress: length of buf_addrs and buf_sizes mismatch")
            .c_str());
    return nullptr;
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
  auto status = DecompressBuffers(compression_type, options, (uint8_t*)in_buf_mask,
                                  input_buffers, schema->fields());
  if (!status.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("ShuffleDecompressionJniWrapper_decompress, failed to "
                              "decompress buffers, error message is " +
                              status.message())
                      .c_str());
    return nullptr;
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  // make arrays from buffers
  std::shared_ptr<arrow::RecordBatch> rb;
  status = MakeRecordBatch(schema, num_rows, input_buffers, input_buffers.size(), &rb);
  if (!status.ok()) {
    env->ThrowNew(io_exception_class,
                  std::string("ShuffleDecompressionJniWrapper_decompress, failed to "
                              "MakeRecordBatch upon buffers, error message is " +
                              status.message())
                      .c_str());
    return nullptr;
  }
  const arrow::Result<jbyteArray>& r =
      ToBytes(env, arrow::RecordBatch::Make(schema, num_rows, std::move(arrays)));
  if (!r.ok()) {
    std::string error_message = "Error deserializing message" + r.status().message();
    env->ThrowNew(io_exception_class, error_message.c_str());
    return nullptr;
  }
  jbyteArray serialized_record_batch = r.ValueOrDie();

  return serialized_record_batch;
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_address, jlong memory_size,
    jlong fixed_size_per_row) {
  if (schema_arr == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row schema can't be null").c_str());
    return NULL;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row: buf_addrs can't be null").c_str());
    return NULL;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row: buf_sizes can't be null").c_str());
    return NULL;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string(
            "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch")
            .c_str());
    return NULL;
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  auto status = MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                (int64_t*)in_buf_sizes, in_bufs_len, &rb);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native convert columnar to row: make record batch failed, "
                              "error message is " +
                              status.message())
                      .c_str());
    return NULL;
  }

  uint8_t* address = reinterpret_cast<uint8_t*>(memory_address);

  // convert the record batch to spark unsafe row.
  try {
    std::shared_ptr<ColumnarToRowConverter> columnar_to_row_converter =
        std::make_shared<ColumnarToRowConverter>(rb, address, memory_size,
                                                 fixed_size_per_row);
    auto status = columnar_to_row_converter->Init();
    if (!status.ok()) {
      env->ThrowNew(illegal_argument_exception_class,
                    std::string("Native convert columnar to row: Init "
                                "ColumnarToRowConverter failed, error message is " +
                                status.message())
                        .c_str());
      return NULL;
    }

    status = columnar_to_row_converter->Write();

    if (!status.ok()) {
      env->ThrowNew(
          illegal_argument_exception_class,
          std::string("Native convert columnar to row: ColumnarToRowConverter write "
                      "failed, error message is " +
                      status.message())
              .c_str());
      return NULL;
    }

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

    jobject arrow_columnar_to_row_info = env->NewObject(
        arrow_columnar_to_row_info_class, arrow_columnar_to_row_info_constructor,
        instanceID, offsets_arr, lengths_arr);
    return arrow_columnar_to_row_info;

  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  return NULL;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env, jobject, jlong instance_id) {
  columnar_to_row_converter_holder_.Erase(instance_id);
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocTrim(JNIEnv* env,
                                                                     jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocStats(JNIEnv* env,
                                                                      jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
