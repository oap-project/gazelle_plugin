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

import java.util.concurrent.TimeUnit._

import scala.collection.mutable.ListBuffer

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.sql.execution.RowToColumnConverter
import com.intel.oap.vectorized.{ArrowRowToColumnarJniWrapper, ArrowWritableColumnVector, CloseableColumnBatchIterator}
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkSchemaUtils}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils.UnsafeItr
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform


class ArrowRowToColumnarExec(child: SparkPlan) extends RowToColumnarExec(child = child) {
  override def nodeName: String = "ArrowRowToColumnarExec"

  buildCheck()

  def buildCheck(): Unit = {
    val schema = child.schema
    for (field <- schema.fields) {
      field.dataType match {
        case d: BooleanType =>
        case d: ByteType =>
        case d: ShortType =>
        case d: IntegerType =>
        case d: LongType =>
        case d: FloatType =>
        case d: DoubleType =>
        case d: StringType =>
        case d: DateType =>
        case d: DecimalType =>
        case d: TimestampType =>
        case d: BinaryType =>
        case _ =>
          throw new UnsupportedOperationException(s"${field.dataType} " +
            s"is not supported in ArrowColumnarToRowExec.")
      }
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val processTime = longMetric("processTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    child.execute().mapPartitions { rowIterator =>

      val jniWrapper = new ArrowRowToColumnarJniWrapper()
      if (rowIterator.hasNext) {
        val res = new Iterator[ColumnarBatch] {
          private val converters = new RowToColumnConverter(localSchema)
          private var last_cb: ColumnarBatch = null
          private var elapse: Long = 0

          override def hasNext: Boolean = {
            rowIterator.hasNext
          }

          override def next(): ColumnarBatch = {
            // Allocate large buffer to store the numRows rows
            val bufferSize = 134217728  // 128M can estimator the buffer size based on the data type
            val allocator = SparkMemoryUtils.contextAllocator()
            val arrowBuf: ArrowBuf = allocator.buffer(bufferSize)

            if (arrowBuf == null) {
              logInfo("the buffer allocated failed and will fall back to non arrow optimization")
              val vectors: Seq[WritableColumnVector] =
                ArrowWritableColumnVector.allocateColumns(numRows, schema)
              var rowCount = 0
              while (rowCount < numRows && rowIterator.hasNext) {
                val row = rowIterator.next()
                val start = System.nanoTime()
                converters.convert(row, vectors.toArray)
                elapse += System.nanoTime() - start
                rowCount += 1
              }
              vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].setValueCount(rowCount))
              processTime.set(NANOSECONDS.toMillis(elapse))
              numInputRows += rowCount
              numOutputBatches += 1
              last_cb = new ColumnarBatch(vectors.toArray, rowCount)
              last_cb
            } else {

              val rowLength = new ListBuffer[Long]()
              var rowCount = 0
              var offset = 0
              while (rowCount < numRows && rowIterator.hasNext) {
                val row = rowIterator.next() // UnsafeRow
                assert(row.isInstanceOf[UnsafeRow])
                val unsafeRow = row.asInstanceOf[UnsafeRow]
                val sizeInBytes = unsafeRow.getSizeInBytes
                Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
                  null, arrowBuf.memoryAddress() + offset, sizeInBytes)
                offset += sizeInBytes
                rowLength += sizeInBytes.toLong
                rowCount += 1
              }
              val timeZoneId = SparkSchemaUtils.getLocalTimezoneID()
              val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
              val schemaBytes: Array[Byte] = ConverterUtils.getSchemaBytesBuf(arrowSchema)
              val serializedRecordBatch = jniWrapper.nativeConvertRowToColumnar(schemaBytes, rowLength.toArray,
                arrowBuf.memoryAddress(), SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
              arrowBuf.close()
              processTime.set(NANOSECONDS.toMillis(elapse))
              numInputRows += rowCount
              numOutputBatches += 1
              val rb = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator, serializedRecordBatch)
              val output = ConverterUtils.fromArrowRecordBatch(arrowSchema, rb)
              val outputNumRows = rb.getLength
              ConverterUtils.releaseArrowRecordBatch(rb)
              last_cb = new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
              last_cb
            }
          }
        }
        new CloseableColumnBatchIterator(res)
      } else {
        Iterator.empty
      }
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ArrowRowToColumnarExec]

  override def equals(other: Any): Boolean = other match {
    case that: ArrowRowToColumnarExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

}
