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

//import com.intel.oap.GazellePluginConfig
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
  * The runtimeFilters is not actually used in ColumnarBatchScanExec currently.
  * This class lacks the implementation for doExecuteColumnar.
  */
abstract class ColumnarBatchScanExec(output: Seq[AttributeReference], @transient scan: Scan,
                                     runtimeFilters: Seq[Expression])
    extends BatchScanExec(output, scan, runtimeFilters) {
  // tmpDir is used by ParquetReader, which looks useless (may be removed in the future).
  // Here, "/tmp" is directly used, no need to get it set through configuration.
  // val tmpDir: String = GazellePluginConfig.getConf.tmpFile
  val tmpDir: String = "/tmp"
  override def supportsColumnar(): Boolean = true
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_batchscan"),
    "inputSize" -> SQLMetrics.createSizeMetric(sparkContext, "input size in bytes"))

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarBatchScanExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarBatchScanExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }
}
