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
import com.intel.oap.expression._
import com.intel.oap.vectorized._
import com.google.common.collect.Lists
import java.util.concurrent.TimeUnit._

import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.util.{ExecutorManager, UserAddedJarUtils, Utils}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.DecimalType

import scala.util.control.Breaks.{break, breakable}


/**
 * Columnar Based SortExec.
 */
case class ColumnarSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
    extends UnaryExecNode
    with ColumnarCodegenSupport {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GazellePluginConfig.getConf.numaBindingInfo
  override def supportsColumnar = true
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarSortExec doesn't support doExecute")
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override lazy val metrics = Map(
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_sort"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in cache all data"),
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in sort process"),
    "shuffleTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in shuffle process"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"))

  val elapse = longMetric("processTime")
  val sortTime = longMetric("sortTime")
  val shuffleTime = longMetric("shuffleTime")
  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")

  buildCheck()

  def buildCheck(): Unit = {
    // check types
    for (attr <- output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarSorter.")
      }
    }
    // check expr
    sortOrder.toList.map(expr => {
      ColumnarExpressionConverter.replaceWithColumnarExpression(expr.child)
    })
  }

  var allLiteral = true
  breakable {
    for (expr <- sortOrder) {
      if (!expr.child.isInstanceOf[Literal]) {
        allLiteral = false
        break
      }
    }
  }

  /*****************  WSCG related function ******************/
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def supportColumnarCodegen: Boolean = true

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def getChild: SparkPlan = child

  override def dependentPlanCtx: ColumnarCodegenContext = {
    // Originally, Sort dependent kernel is SortKernel
    // While since we noticed that
    val inputSchema = ConverterUtils.toArrowSchema(child.output)
    val outSchema = ConverterUtils.toArrowSchema(output)
    ColumnarCodegenContext(
      inputSchema,
      outSchema,
      ColumnarSorter.prepareRelationFunction(sortOrder, child.output))
  }

  override def doCodeGen: ColumnarCodegenContext = null

  /***********************************************************/
  def getCodeGenSignature =
    if (sortOrder.exists(expr =>
        !expr.child.isInstanceOf[Literal] &&
        bindReference(ConverterUtils.getAttrFromExpr(expr.child), child.output, true)
        .isInstanceOf[BoundReference])) {
      ColumnarSorter.prebuild(
        sortOrder,
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
        val tempDir = GazellePluginConfig.getRandomTempDir
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
      } else if (allLiteral) {
        // If sortOrder are all Literal, no need to do sorting.
        new CloseableColumnBatchIterator(iter)
      } else {
        GazellePluginConfig.getConf
        val execTempDir = GazellePluginConfig.getTempFile
        val jarList = listJars.map(jarUrl => {
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

}
