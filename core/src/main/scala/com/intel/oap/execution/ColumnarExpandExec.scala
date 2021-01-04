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

import com.intel.oap.expression._
import com.intel.oap.vectorized._
import org.apache.arrow.vector.ValueVector
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils

case class ColumnarExpandExec(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)
    extends UnaryExecNode {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_condproject"))
  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  val originalInputAttributes = child.output

  override def supportsColumnar = true

  // build check for projection
  projections.map(proj => ColumnarProjection.buildCheck(originalInputAttributes, proj))

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("doExecute is not supported in ColumnarExpandExec.")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputBatches = longMetric("numInputBatches")
    val procTime = longMetric("processTime")
    child.executeColumnar().mapPartitions { iter =>
      val columnarGroups =
        projections.map(proj => ColumnarProjection.create(originalInputAttributes, proj))
      var eval_elapse: Long = 0
      def close = procTime += (eval_elapse / 1000000)
      val res = new Iterator[ColumnarBatch] {
        private[this] var result: ColumnarBatch = _
        private[this] var idx = -1 // -1 means the initial state
        private[this] var input: List[ValueVector] = _
        private[this] var numRows: Int = 0
        private[this] val numGroups = columnarGroups.length
        private[this] val resultStructType =
          StructType(output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

        override def hasNext: Boolean = (-1 < idx && idx < numGroups) || iter.hasNext

        override def next(): ColumnarBatch = {
          if (idx <= 0) {
            // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
            val input_cb = iter.next()
            input = (0 until input_cb.numCols).toList
              .map(input_cb.column(_).asInstanceOf[ArrowWritableColumnVector].getValueVector)
            numRows = input_cb.numRows
            numInputBatches += 1
            idx = 0
          }

          if (numRows == 0) {
            idx = -1
            numOutputBatches += 1
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val beforeEval = System.nanoTime()
          val resultColumnVectorList = columnarGroups(idx).evaluate(numRows, input)
          result = new ColumnarBatch(
            resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]).toArray,
            numRows)
          idx += 1

          if (idx == numGroups) {
            idx = -1
          }

          numOutputRows += numRows
          numOutputBatches += 1
          eval_elapse += System.nanoTime() - beforeEval
          result
        }
      }
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((tc: TaskContext) => {
          close
        })
      new CloseableColumnBatchIterator(res)
    }
  }
}
