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

import com.intel.oap.vectorized._
import com.intel.oap.ColumnarPluginConfig
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{UserAddedJarUtils, Utils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists
import com.intel.oap.expression._
import com.intel.oap.vectorized.ExpressionEvaluator
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ColumnarSortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null)
    extends BinaryExecNode
    with ColumnarCodegenSupport {

  val sparkConf = sparkContext.getConf
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "prepareTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to prepare left list"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to process"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to merge join"),
    "totaltime_sortmergejoin" -> SQLMetrics
      .createTimingMetric(sparkContext, "totaltime_sortmergejoin"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val joinTime = longMetric("joinTime")
  val prepareTime = longMetric("prepareTime")
  val totaltime_sortmegejoin = longMetric("totaltime_sortmergejoin")
  val resultSchema = this.schema

  override def supportsColumnar = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"ColumnarSortMergeJoinExec doesn't support doExecute")
  }
  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ($opId)".trim
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"
    s"""
      |(${ExplainUtils.getOpId(this)}) $nodeName
      |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
      |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
      |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
    """.stripMargin
  }

  override def output: Seq[Attribute] = {
    if (projectList != null && !projectList.isEmpty)
      return projectList.map(_.toAttribute)
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }
  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For inner join, orders of both sides keys should be kept.
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map {
        case (lKey, rKey) =>
          // Also add the right key and its `sameOrderExpressions`
          SortOrder(
            lKey.child,
            Ascending,
            lKey.sameOrderExpressions + rKey.child ++ rKey.sameOrderExpressions)
      }
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    // There are null rows in both streams, so there is no order.
    case FullOuter => Nil
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private def getKeyOrdering(
      keys: Seq[Expression],
      childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map {
        case (key, childOrder) =>
          SortOrder(key, Ascending, childOrder.sameOrderExpressions + childOrder.child - key)
      }
    } else {
      requiredOrdering
    }
  }

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  val (buildKeys, streamedKeys, buildPlan, streamedPlan) = joinType match {
    case LeftSemi =>
      (rightKeys, leftKeys, right, left)
    case LeftOuter =>
      (rightKeys, leftKeys, right, left)
    case LeftAnti =>
      (rightKeys, leftKeys, right, left)
    case j: ExistenceJoin =>
      (rightKeys, leftKeys, right, left)
    case LeftExistence(_) =>
      (rightKeys, leftKeys, right, left)
    case _ =>
      left match {
        case p: ColumnarSortMergeJoinExec =>
          (rightKeys, leftKeys, right, left)
        case ColumnarConditionProjectExec(_, _, child: ColumnarSortMergeJoinExec) =>
          (rightKeys, leftKeys, right, left)
        case other =>
          (leftKeys, rightKeys, left, right)
      }
  }

  /*****************  WSCG related function ******************/
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      Seq(streamedPlan.executeColumnar())
  }

  override def supportColumnarCodegen: Boolean = true

  val output_skip_alias =
    if (projectList == null || projectList.isEmpty) output
    else projectList.map(expr => ConverterUtils.getAttrFromExpr(expr, true))

  def getKernelFunction: TreeNode = {
    ColumnarSortMergeJoin.prepareKernelFunction(
      buildKeys,
      streamedKeys,
      buildPlan.output,
      streamedPlan.output,
      output_skip_alias,
      joinType,
      condition)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {

    val curBuildPlan: Seq[(SparkPlan, SparkPlan)] = buildPlan match {
      case s: ColumnarSortExec =>
        Seq((s, this))
      case c: ColumnarCodegenSupport
          if !c.isInstanceOf[ColumnarSortExec] && c.supportColumnarCodegen == true =>
        c.getBuildPlans
      case other =>
        /* should be ColumnarInputAdapter or others */
        Seq((other, this))
    }
    streamedPlan match {
      case c: ColumnarCodegenSupport if c.isInstanceOf[ColumnarSortExec] =>
        curBuildPlan ++ Seq((c, this))
      case c: ColumnarCodegenSupport if !c.isInstanceOf[ColumnarSortExec] =>
        c.getBuildPlans ++ curBuildPlan
      case _ =>
        curBuildPlan
    }
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
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

  override def getChild: SparkPlan = streamedPlan

  override def doCodeGen: ColumnarCodegenContext = {
    val childCtx = streamedPlan match {
      case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
        c.doCodeGen
      case _ =>
        null
    }
    val outputSchema = ConverterUtils.toArrowSchema(output_skip_alias)
    val (codeGenNode, inputSchema) = if (childCtx != null) {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction, childCtx.root),
          new ArrowType.Int(32, true)),
        childCtx.inputSchema)
    } else {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction),
          new ArrowType.Int(32, true)),
        new Schema(Lists.newArrayList()))
    }
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }
  //do not call prebuild so we could skip the c++ codegen
  //val triggerBuildSignature = getCodeGenSignature

  /*try {
    ColumnarSortMergeJoin.precheck(
      leftKeys,
      rightKeys,
      resultSchema,
      joinType,
      condition,
      left,
      right,
      joinTime,
      prepareTime,
      totaltime_sortmegejoin,
      numOutputRows,
      sparkConf)
  } catch {
    case e: Throwable =>
      throw e
  }*/

  /***********************************************************/
  def getCodeGenSignature: String =
    if (resultSchema.size > 0) {
      try {
        ColumnarSortMergeJoin.prebuild(
          leftKeys,
          rightKeys,
          resultSchema,
          joinType,
          condition,
          left,
          right,
          joinTime,
          prepareTime,
          totaltime_sortmegejoin,
          numOutputRows,
          sparkConf)
      } catch {
        case e: Throwable =>
          throw e
      }
    } else {
      ""
    }

  def uploadAndListJars(signature: String) =
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
    right.executeColumnar().zipPartitions(left.executeColumnar()) { (streamIter, buildIter) =>
      ColumnarPluginConfig.getConf(sparkConf)
      val execTempDir = ColumnarPluginConfig.getTempFile
      val jarList = listJars.map(jarUrl => {
        logWarning(s"Get Codegened library Jar ${jarUrl}")
        UserAddedJarUtils.fetchJarFromSpark(
          jarUrl,
          execTempDir,
          s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
          sparkConf)
        s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
      })

      val vsmj = ColumnarSortMergeJoin.create(
        leftKeys,
        rightKeys,
        resultSchema,
        joinType,
        condition,
        left,
        right,
        isSkewJoin,
        jarList,
        joinTime,
        prepareTime,
        totaltime_sortmegejoin,
        numOutputRows,
        sparkConf)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        vsmj.close()
      })
      val vjoinResult = vsmj.columnarJoin(streamIter, buildIter)
      new CloseableColumnBatchIterator(vjoinResult)
    }
  }
}
