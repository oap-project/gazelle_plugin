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

import com.intel.oap.GazellePluginConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.{Scan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch


class ColumnarBatchScanExec(output: Seq[AttributeReference], @transient scan: Scan,
                            runtimeFilters: Seq[Expression])
  extends ColumnarBatchScanExecBase(output, scan, runtimeFilters) {
   val tmpDir: String = GazellePluginConfig.getConf.tmpFile
  override def supportsColumnar(): Boolean = true
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_batchscan"),
    "inputSize" -> SQLMetrics.createSizeMetric(sparkContext, "input size in bytes"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val scanTime = longMetric("scanTime")
    val inputSize = longMetric("inputSize")
    val inputColumnarRDD =
      new ColumnarDataSourceRDD(sparkContext, partitions, readerFactory,
        true, scanTime, numInputBatches, inputSize, tmpDir)
    inputColumnarRDD.map { r =>
      numOutputRows += r.numRows()
      numOutputBatches += 1
      r
    }
  }

  override def doCanonicalize(): ColumnarBatchScanExec = {
    if (runtimeFilters == null) {
      // For spark3.1.
      new ColumnarBatchScanExec(output.map(QueryPlan.normalizeExpressions(_, output)), scan, null)
    } else {
      // For spark3.2.
      new ColumnarBatchScanExec(
        output.map(QueryPlan.normalizeExpressions(_, output)), scan,
        QueryPlan.normalizePredicates(
          runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
          output))
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarBatchScanExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarBatchScanExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }
}
