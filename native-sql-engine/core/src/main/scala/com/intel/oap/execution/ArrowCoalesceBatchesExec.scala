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

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.{ArrowCoalesceBatchesJniWrapper, ArrowWritableColumnVector, CloseableColumnBatchIterator}
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.VectorBatchAppender
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class ArrowCoalesceBatchesExec(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "ArrowCoalesceBatches"

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_collectbatch"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_coalescebatch"),
    "avgCoalescedNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg coalesced batch num rows"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    import ArrowCoalesceBatchesExec._

    val recordsPerBatch = conf.arrowMaxRecordsPerBatch
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val avgCoalescedNumRows = longMetric("avgCoalescedNumRows")

    child.executeColumnar().mapPartitions { iter =>
      val jniWrapper = new ArrowCoalesceBatchesJniWrapper()
      val beforeInput = System.nanoTime
      val hasInput = iter.hasNext
      collectTime += System.nanoTime - beforeInput
      val res = if (hasInput) {
        new Iterator[ColumnarBatch] {
          var numBatchesTotal: Long = _
          var numRowsTotal: Long = _
          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ =>
            if (numBatchesTotal > 0) {
              avgCoalescedNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
            }
          }

          override def hasNext: Boolean = {
            val beforeNext = System.nanoTime
            val hasNext = iter.hasNext
            collectTime += System.nanoTime - beforeNext
            hasNext
          }

          override def next(): ColumnarBatch = {

            if (!hasNext) {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }

            var rowCount = 0
            val batchesToAppend = ListBuffer[ColumnarBatch]()

            val arrBufAddrs = new ArrayBuffer[Array[Long]]()
            val arrBufSizes = new ArrayBuffer[Array[Long]]()
            val numrows = ListBuffer[Int]()

            val beforeConcat = System.nanoTime
            while (hasNext && rowCount < recordsPerBatch) {
              val delta: ColumnarBatch = iter.next()
              delta.retain()
              rowCount += delta.numRows
              batchesToAppend += delta

              val bufAddrs = new ListBuffer[Long]()
              val bufSizes = new ListBuffer[Long]()
              val recordBatch = ConverterUtils.createArrowRecordBatch(delta)
              recordBatch.getBuffers().asScala.foreach { buffer => bufAddrs += buffer.memoryAddress() }
              recordBatch.getBuffersLayout().asScala.foreach { bufLayout =>
                bufSizes += bufLayout.getSize()
              }
              ConverterUtils.releaseArrowRecordBatch(recordBatch)
              arrBufAddrs.append(bufAddrs.toArray)
              arrBufSizes.append(bufSizes.toArray)
              numrows.append(delta.numRows)
            }

            // chendi: We need make sure target FieldTypes are exactly the same as src
            val expected_output_arrow_fields = if (batchesToAppend.size > 0) {
              (0 until batchesToAppend(0).numCols).map(i => {
                batchesToAppend(0).column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector.getField
              })
            } else {
              Nil
            }

            val schema = new Schema(expected_output_arrow_fields.asJava)
            val arrowSchema = ConverterUtils.getSchemaBytesBuf(schema)

            val serializedRecordBatch = jniWrapper.nativeCoalesceBatches(
              arrowSchema, rowCount, numrows.toArray, arrBufAddrs.toArray, arrBufSizes.toArray,
              SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
            val rb = UnsafeRecordBatchSerializer.deserializeUnsafe(SparkMemoryUtils.contextAllocator(), serializedRecordBatch)
            val ColVecArr = ConverterUtils.fromArrowRecordBatch(schema, rb)
            val outputNumRows = rb.getLength
            ConverterUtils.releaseArrowRecordBatch(rb)
            val bigColBatch = new ColumnarBatch(ColVecArr.map(v => v.asInstanceOf[ColumnVector]).toArray, rowCount)

            concatTime += System.nanoTime - beforeConcat
            numOutputRows += rowCount
            numInputBatches += batchesToAppend.length
            numOutputBatches += 1

            // used for calculating avgCoalescedNumRows
            numRowsTotal += rowCount
            numBatchesTotal += 1

            batchesToAppend.foreach(cb => cb.close())

            bigColBatch
          }
        }
      } else {
        Iterator.empty
      }
      new CloseableColumnBatchIterator(res)
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): ArrowCoalesceBatchesExec =
    copy(child = newChild)
}

object ArrowCoalesceBatchesExec {
  implicit class ArrowColumnarBatchRetainer(val cb: ColumnarBatch) {
    def retain(): Unit = {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    }
  }
}
