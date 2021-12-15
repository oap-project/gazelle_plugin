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

package com.intel.oap.execution

import scala.collection.mutable.ListBuffer

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.ArrowRowToColumnarJniWrapper

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkSchemaUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.Platform

class ArrowRowToColumnarExecSuite extends SharedSparkSession {

  test("ArrowRowToColumnar") {
    val row = InternalRow(3)
    val converter = UnsafeProjection.create(Array[DataType](IntegerType))
    val unsafeRow = converter.apply(row)
    val sizeInBytes = unsafeRow.getSizeInBytes

    val bufferSize = 1024  // 128M can estimator the buffer size based on the data type
    val allocator = SparkMemoryUtils.contextAllocator()
    val arrowBuf = allocator.buffer(bufferSize)

    Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
      null, arrowBuf.memoryAddress(), sizeInBytes)

    val rowLength = new ListBuffer[Long]()

    rowLength += sizeInBytes
    val timeZoneId = SparkSchemaUtils.getLocalTimezoneID()
    val schema1 = StructType(Seq(StructField("i", IntegerType)))
    val arrowSchema = ArrowUtils.toArrowSchema(schema1, timeZoneId)
    val schema = ConverterUtils.getSchemaBytesBuf(arrowSchema)
    val jniWrapper = new ArrowRowToColumnarJniWrapper()

    jniWrapper.nativeConvertRowToColumnar(schema, rowLength.toArray, arrowBuf.memoryAddress(),
      SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
  }
}
