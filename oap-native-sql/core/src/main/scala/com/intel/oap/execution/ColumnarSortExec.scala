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

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.expression._
import com.intel.oap.vectorized._
import java.util.concurrent.TimeUnit._

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.util.{UserAddedJarUtils, Utils, ExecutorManager}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Columnar Based SortExec.
 */
class ColumnarSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
    extends SortExec(sortOrder, global, child, testSpillFrequency) {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = ColumnarPluginConfig.getConf(sparkContext.getConf).numaBindingInfo
  override def supportsColumnar = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override lazy val metrics = Map(
    "totalSortTime" -> SQLMetrics
      .createTimingMetric(sparkContext, "totaltime_sort"),
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in sort process"),
    "shuffleTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in shuffle process"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"))

  val elapse = longMetric("totalSortTime")
  val sortTime = longMetric("sortTime")
  val shuffleTime = longMetric("shuffleTime")
  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")

  def getCodeGenSignature =
    if (!sortOrder
      .filter(
        expr => bindReference(expr.child, child.output, true).isInstanceOf[BoundReference])
      .isEmpty) {
      ColumnarSorter.prebuild(
        sortOrder,
        true,
        child.output,
        sortTime,
        numOutputBatches,
        numOutputRows,
        shuffleTime,
        elapse,
        sparkConf)
    } else {
      ""
    }

  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = ColumnarPluginConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      List()
    }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val signature = getCodeGenSignature
    val listJars = uploadAndListJars(signature)
    listJars.foreach(jar => logInfo(s"Uploaded ${jar}"))
    child.executeColumnar().mapPartitions { iter =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      val hasInput = iter.hasNext
      val res = if (!hasInput) {
        Iterator.empty
      } else {
        ColumnarPluginConfig.getConf(sparkConf)
        val execTempDir = ColumnarPluginConfig.getTempFile
        val jarList = listJars
          .map(jarUrl => {
            logWarning(s"Get Codegened library Jar ${jarUrl}")
            UserAddedJarUtils.fetchJarFromSpark(
              jarUrl,
              execTempDir,
              s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
              sparkConf)
            s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
          })
        val sorter = ColumnarSorter.create(
          sortOrder,
          true,
          child.output,
          jarList,
          sortTime,
          numOutputBatches,
          numOutputRows,
          shuffleTime,
          elapse,
          sparkConf)
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            sorter.close()
          })
        new CloseableColumnBatchIterator(sorter.createColumnarIterator(iter))
      }
      res
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarSortExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarSortExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }
}
