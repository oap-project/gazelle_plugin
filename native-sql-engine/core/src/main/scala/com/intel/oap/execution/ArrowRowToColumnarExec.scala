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
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkSchemaUtils}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils.UnsafeItr
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.{TaskContext, broadcast}
import org.apache.spark.unsafe.Platform


case class ArrowRowToColumnarExec(child: SparkPlan) extends UnaryExecNode {
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
            s"is not supported in ArrowRowToColumnarExec.")
      }
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
  )

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = true

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): ArrowRowToColumnarExec =
    copy(child = newChild)

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

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
      val timeZoneId = SparkSchemaUtils.getLocalTimezoneID()
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
      var schemaBytes: Array[Byte] = null

      if (rowIterator.hasNext) {
        val res = new Iterator[ColumnarBatch] {
          private val converters = new RowToColumnConverter(localSchema)
          private var last_cb: ColumnarBatch = null
          private var elapse: Long = 0
          val allocator = SparkMemoryUtils.contextAllocator()
          var arrowBuf: ArrowBuf = null
          override def hasNext: Boolean = {
            rowIterator.hasNext
          }
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            if (arrowBuf != null && arrowBuf.isOpen()) {
              arrowBuf.close()
            }
          }
          override def next(): ColumnarBatch = {
            var isUnsafeRow = true
            var firstRow = InternalRow.apply()
            var hasNextRow = false
            if (rowIterator.hasNext) {
              firstRow = rowIterator.next()
              hasNextRow = true
            }
            if (!firstRow.isInstanceOf[UnsafeRow]) {
              isUnsafeRow = false
            }

            if (isUnsafeRow) {
              val rowLength = new ListBuffer[Long]()
              var rowCount = 0
              var offset = 0

              assert(firstRow.isInstanceOf[UnsafeRow])
              val unsafeRow = firstRow.asInstanceOf[UnsafeRow]
              val sizeInBytes = unsafeRow.getSizeInBytes
              // allocate buffer based on 1st row
              val estimatedBufSize = sizeInBytes.toDouble * numRows * 1.2
              arrowBuf = allocator.buffer(estimatedBufSize.toLong)
              Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
                null, arrowBuf.memoryAddress() + offset, sizeInBytes)
              offset += sizeInBytes
              rowLength += sizeInBytes.toLong
              rowCount += 1

              while (rowCount < numRows && rowIterator.hasNext) {
                val row = rowIterator.next() // UnsafeRow
                assert(row.isInstanceOf[UnsafeRow])
                val unsafeRow = row.asInstanceOf[UnsafeRow]
                val sizeInBytes = unsafeRow.getSizeInBytes
                if ((offset + sizeInBytes) > arrowBuf.capacity()) {
                  val tmpBuf = allocator.buffer(((offset + sizeInBytes) * 1.5).toLong)
                  tmpBuf.setBytes(0, arrowBuf, 0, offset)
                  arrowBuf.close()
                  arrowBuf = tmpBuf
                }
                Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
                  null, arrowBuf.memoryAddress() + offset, sizeInBytes)
                offset += sizeInBytes
                rowLength += sizeInBytes.toLong
                rowCount += 1
              }
              if (schemaBytes == null) {
                schemaBytes = ConverterUtils.getSchemaBytesBuf(arrowSchema)
              }
              val start = System.nanoTime()
              val serializedRecordBatch = jniWrapper.nativeConvertRowToColumnar(schemaBytes, rowLength.toArray,
                arrowBuf.memoryAddress(), SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
              elapse = System.nanoTime() - start
              numInputRows += rowCount
              numOutputBatches += 1
              val rb = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator, serializedRecordBatch)
              val output = ConverterUtils.fromArrowRecordBatch(arrowSchema, rb)
              val outputNumRows = rb.getLength
              ConverterUtils.releaseArrowRecordBatch(rb)
              arrowBuf.close()
              last_cb = new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
              processTime += NANOSECONDS.toMillis(elapse)
              last_cb
            } else {
              logInfo("not unsaferow, fallback to java based r2c")
              val vectors: Seq[WritableColumnVector] =
                ArrowWritableColumnVector.allocateColumns(numRows, schema)
              var rowCount = 0

              val start = System.nanoTime()
              converters.convert(firstRow, vectors.toArray)
              elapse += System.nanoTime() - start
              rowCount += 1

              while (rowCount < numRows && rowIterator.hasNext) {
                val row = rowIterator.next()
                val start = System.nanoTime()
                converters.convert(row, vectors.toArray)
                elapse += System.nanoTime() - start
                rowCount += 1
              }
              vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].setValueCount(rowCount))
              processTime += NANOSECONDS.toMillis(elapse)
              numInputRows += rowCount
              numOutputBatches += 1
              last_cb = new ColumnarBatch(vectors.toArray, rowCount)
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

}
