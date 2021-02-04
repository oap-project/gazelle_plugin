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
import org.apache.spark.util.{UserAddedJarUtils, Utils, ExecutorManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.ArrowUtils
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
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoin}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ColumnarShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    projectList: Seq[NamedExpression] = null)
    extends BinaryExecNode
    with ColumnarCodegenSupport
    with HashJoin {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = ColumnarPluginConfig.getConf.numaBindingInfo
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"))

  buildCheck()

  val (buildKeyExprs, streamedKeyExprs) = {
    require(
      leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys)
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  def buildCheck(): Unit = {
    // build check for condition
    val conditionExpr: Expression = condition.orNull
    if (conditionExpr != null) {
      ColumnarExpressionConverter.replaceWithColumnarExpression(conditionExpr)
    }
    // build check types
    for (attr <- streamedPlan.output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarShuffledHashJoinExec.")
      }
    }
    for (attr <- buildPlan.output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarShuffledHashJoinExec.")
      }
    }
    // build check for expr
    if (buildKeyExprs != null) {
      for (expr <- buildKeyExprs) {
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
      }
    }
    if (streamedKeyExprs != null) {
      for (expr <- streamedKeyExprs) {
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
      }
    }
  }

  override def output: Seq[Attribute] =
    if (projectList == null || projectList.isEmpty) super.output
    else projectList.map(_.toAttribute)

  /*protected lazy val (buildPlan, streamedPlan, buildKeys, streamKeys) = buildSide match {
    case BuildLeft => (left, right, leftKeys, rightKeys)
    case BuildRight => (right, left, rightKeys, leftKeys)
  }*/

  def getBuildPlan: SparkPlan = buildPlan
  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"ColumnarShuffledHashJoinExec doesn't support doExecute")
  }
  override def supportsColumnar = true
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      Seq(streamedPlan.executeColumnar())
  }
  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def getChild: SparkPlan = streamedPlan

  override def dependentPlanCtx: ColumnarCodegenContext = {
    val inputSchema = ConverterUtils.toArrowSchema(buildPlan.output)
    ColumnarCodegenContext(
      inputSchema,
      null,
      ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, buildPlan.output, 1))
  }

  override def supportColumnarCodegen: Boolean = true

  val output_skip_alias =
    if (projectList == null || projectList.isEmpty) super.output
    else projectList.map(expr => ConverterUtils.getAttrFromExpr(expr, true))
  def getKernelFunction(_type: Int = 0): TreeNode = {

    val buildInputAttributes = buildPlan.output.toList
    val streamInputAttributes = streamedPlan.output.toList
    // For Join, we need to do two things
    // 1. create buildHashRelation RDD ?
    // 2. create streamCodeGen and return

    ColumnarConditionedProbeJoin.prepareKernelFunction(
      buildKeyExprs,
      streamedKeyExprs,
      buildInputAttributes,
      streamInputAttributes,
      output_skip_alias,
      joinType,
      buildSide,
      condition,
      _type)
  }

  override def doCodeGen: ColumnarCodegenContext = {
    val childCtx = streamedPlan match {
      case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
        c.doCodeGen
      case _ =>
        null
    }
    val outputSchema = ConverterUtils.toArrowSchema(output)
    val (codeGenNode, inputSchema) = if (childCtx != null) {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction(1), childCtx.root),
          new ArrowType.Int(32, true)),
        childCtx.inputSchema)
    } else {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction(1)),
          new ArrowType.Int(32, true)),
        ConverterUtils.toArrowSchema(streamedPlan.output))
    }
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
// we will use previous codegen join to handle joins with condition
    if (condition.isDefined) {
      val enableHashCollisionCheck = ColumnarPluginConfig.getConf.hashCompare
      val hashTableType = if (enableHashCollisionCheck) 1 else 0
      return getCodeGenIterator(hashTableType)
    }

    // below only handles join without condition

    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val totalTime = longMetric("processTime")
    val buildTime = longMetric("buildTime")
    val joinTime = longMetric("joinTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) { (iter, depIter) =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      val hashRelationKernel = new ExpressionEvaluator()
      val hashRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()
      val enableHashCollisionCheck = ColumnarPluginConfig.getConf.hashCompare
      val hashTableType = if (enableHashCollisionCheck) 1 else 0
      val hash_relation_function =
        ColumnarConditionedProbeJoin
          .prepareHashBuildFunction(buildKeyExprs, buildPlan.output, hashTableType)
      val hash_relation_schema = ConverterUtils.toArrowSchema(buildPlan.output)
      val hash_relation_expr =
        TreeBuilder.makeExpression(
          hash_relation_function,
          Field.nullable("result", new ArrowType.Int(32, true)))
      hashRelationKernel.build(hash_relation_schema, Lists.newArrayList(hash_relation_expr), true)
      while (depIter.hasNext) {
        val dep_cb = depIter.next()
        (0 until dep_cb.numCols).toList.foreach(i =>
          dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
        hashRelationBatchHolder += dep_cb
        val beforeEval = System.nanoTime()
        val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
        hashRelationKernel.evaluate(dep_rb)
        ConverterUtils.releaseArrowRecordBatch(dep_rb)
        build_elapse += System.nanoTime() - beforeEval
      }
      val hashRelationResultIterator = hashRelationKernel.finishByIterator()

      val native_function = TreeBuilder.makeFunction(
        s"standalone",
        Lists.newArrayList(getKernelFunction(hashTableType)),
        new ArrowType.Int(32, true))
      val probe_expr =
        TreeBuilder
          .makeExpression(native_function, Field.nullable("result", new ArrowType.Int(32, true)))
      val probe_input_schema = ConverterUtils.toArrowSchema(streamedPlan.output)
      val probe_out_schema = ConverterUtils.toArrowSchema(output)
      val nativeKernel = new ExpressionEvaluator()
      nativeKernel
        .build(probe_input_schema, Lists.newArrayList(probe_expr), probe_out_schema, true)
      val nativeIterator = nativeKernel.finishByIterator()
      // we need to complete dependency RDD's firstly
      nativeIterator.setDependencies(Array(hashRelationResultIterator))

      def close = {
        joinTime += (eval_elapse / 1000000)
        buildTime += (build_elapse / 1000000)
        totalTime += ((eval_elapse + build_elapse) / 1000000)
        hashRelationBatchHolder.foreach(_.close)
        hashRelationKernel.close
        hashRelationResultIterator.close
        nativeKernel.close
        nativeIterator.close
      }

      // now we can return this wholestagecodegen iter
      val res = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): ColumnarBatch = {
          val cb = iter.next()
          val beforeEval = System.nanoTime()
          val input_rb =
            ConverterUtils.createArrowRecordBatch(cb)
          val output_rb = nativeIterator.process(probe_input_schema, input_rb)
          if (output_rb == null) {
            ConverterUtils.releaseArrowRecordBatch(input_rb)
            eval_elapse += System.nanoTime() - beforeEval
            val resultStructType = ArrowUtils.fromArrowSchema(probe_out_schema)
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val outputNumRows = output_rb.getLength
          ConverterUtils.releaseArrowRecordBatch(input_rb)
          val output = ConverterUtils.fromArrowRecordBatch(probe_out_schema, output_rb)
          ConverterUtils.releaseArrowRecordBatch(output_rb)
          eval_elapse += System.nanoTime() - beforeEval
          numOutputRows += outputNumRows
          new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
        }
      }
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        close
      })
      new CloseableColumnBatchIterator(res)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////
  def getResultSchema = {
    val attributes =
      if (projectList == null || projectList.isEmpty) super.output
      else projectList.map(expr => ConverterUtils.getAttrFromExpr(expr, true))
    ArrowUtils.fromAttributes(attributes)
  }

  def doCodeGenForStandalone: ColumnarCodegenContext = {
    val outputSchema = ConverterUtils.toArrowSchema(output)
    val (codeGenNode, inputSchema) = (
      TreeBuilder.makeFunction(
        s"child",
        Lists.newArrayList(getKernelFunction(1)),
        new ArrowType.Int(32, true)),
      ConverterUtils.toArrowSchema(streamedPlan.output))
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = ColumnarPluginConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
        sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
      } else {
        Seq()
      }
    } else {
      Seq()
    }

  def getCodeGenCtx: ColumnarCodegenContext = {
    var resCtx: ColumnarCodegenContext = null
    try {
      // If this BHJ contains condition, currently we only support doing codegen through WSCG
      val childCtx = doCodeGenForStandalone
      val wholeStageCodeGenNode = TreeBuilder.makeFunction(
        s"wholestagecodegen",
        Lists.newArrayList(childCtx.root),
        new ArrowType.Int(32, true))
      resCtx =
        ColumnarCodegenContext(childCtx.inputSchema, childCtx.outputSchema, wholeStageCodeGenNode)
    } catch {
      case e: UnsupportedOperationException
          if e.getMessage == "Unsupport to generate native expression from replaceable expression." =>
        logWarning(e.getMessage())
        ""
      case e: Throwable =>
        throw e
    }
    resCtx
  }

  def getCodeGenSignature(hashTableType: Int) = {
    if (hashTableType == 0) {
      try {
        ColumnarShuffledHashJoin.prebuild(
          leftKeys,
          rightKeys,
          getResultSchema,
          joinType,
          buildSide,
          condition,
          left,
          right,
          sparkConf)
      } catch {
        case e: UnsupportedOperationException
            if e.getMessage == "Unsupport to generate native expression from replaceable expression." =>
          logWarning(e.getMessage())
          ""
        case e: Throwable =>
          throw e
      }
    } else {
      val resCtx = getCodeGenCtx
      //resCtx = doCodeGen
      if (resCtx != null) {
        val expression =
          TreeBuilder.makeExpression(
            resCtx.root,
            Field.nullable("result", new ArrowType.Int(32, true)))
        val nativeKernel = new ExpressionEvaluator()
        nativeKernel.build(
          resCtx.inputSchema,
          Lists.newArrayList(expression),
          resCtx.outputSchema,
          true)
      } else {
        ""
      }
    }
  }

  def getCodeGenIterator(hashTableType: Int): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val totalTime = longMetric("processTime")
    val buildTime = longMetric("buildTime")
    val joinTime = longMetric("joinTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0

    val signature = getCodeGenSignature(hashTableType)
    val listJars = uploadAndListJars(signature)
    val hashRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()

    if (hashTableType == 1) {
      streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
        (streamIter, buildIter) =>
          ExecutorManager.tryTaskSet(numaBindingInfo)
          ColumnarPluginConfig.getConf
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
          val resCtx = getCodeGenCtx
          val expression =
            TreeBuilder
              .makeExpression(resCtx.root, Field.nullable("result", new ArrowType.Int(32, true)))
          val nativeKernel = new ExpressionEvaluator(jarList.toList.asJava)
          nativeKernel
            .build(resCtx.inputSchema, Lists.newArrayList(expression), resCtx.outputSchema, true)

          // received broadcast value contain a hashmap and raw recordBatch
          val beforeEval = System.nanoTime()
          val ctx = dependentPlanCtx
          val hashRelationKernel = new ExpressionEvaluator()
          val hash_relation_expression = TreeBuilder
            .makeExpression(ctx.root, Field.nullable("result", new ArrowType.Int(32, true)))
          hashRelationKernel
            .build(ctx.inputSchema, Lists.newArrayList(hash_relation_expression), true)
          // we need to set original recordBatch to hashRelationKernel
          while (buildIter.hasNext) {
            val dep_cb = buildIter.next()
            if (dep_cb.numRows > 0) {
              (0 until dep_cb.numCols).toList.foreach(i =>
                dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              hashRelationBatchHolder += dep_cb
              val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
              hashRelationKernel.evaluate(dep_rb)
              ConverterUtils.releaseArrowRecordBatch(dep_rb)
            }
          }
          val hashRelationResultIterator = hashRelationKernel.finishByIterator()
          val nativeIterator = nativeKernel.finishByIterator()
          nativeIterator.setDependencies(Array(hashRelationResultIterator))
          build_elapse += (System.nanoTime() - beforeEval)
          // now we can return this wholestagecodegen itervar closed = false
          var closed = false
          def close = {
            closed = true
            joinTime += (eval_elapse / 1000000)
            buildTime += (build_elapse / 1000000)
            totalTime += ((eval_elapse + build_elapse) / 1000000)
            hashRelationBatchHolder.foreach(_.close)
            hashRelationKernel.close
            hashRelationResultIterator.close
            nativeKernel.close
            nativeIterator.close
          }
          val resultStructType = ArrowUtils.fromArrowSchema(resCtx.outputSchema)
          val res = new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = {
              streamIter.hasNext
            }

            override def next(): ColumnarBatch = {
              val cb = streamIter.next()
              if (cb.numRows == 0) {
                val resultColumnVectors =
                  ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
                return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
              }
              val beforeEval = System.nanoTime()
              val input_rb =
                ConverterUtils.createArrowRecordBatch(cb)
              val output_rb = nativeIterator.process(resCtx.inputSchema, input_rb)
              if (output_rb == null) {
                ConverterUtils.releaseArrowRecordBatch(input_rb)
                eval_elapse += System.nanoTime() - beforeEval
                val resultColumnVectors =
                  ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
                return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
              }
              val outputNumRows = output_rb.getLength
              ConverterUtils.releaseArrowRecordBatch(input_rb)
              val output = ConverterUtils.fromArrowRecordBatch(resCtx.outputSchema, output_rb)
              ConverterUtils.releaseArrowRecordBatch(output_rb)
              eval_elapse += System.nanoTime() - beforeEval
              numOutputRows += outputNumRows
              new ColumnarBatch(
                output.map(v => v.asInstanceOf[ColumnVector]).toArray,
                outputNumRows)
            }
          }
          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            close
          })
          new CloseableColumnBatchIterator(res)
      }
    } else {
      streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
        (streamIter, buildIter) =>
          ExecutorManager.tryTaskSet(numaBindingInfo)
          ColumnarPluginConfig.getConf
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
          val vjoin = ColumnarShuffledHashJoin.create(
            leftKeys,
            rightKeys,
            getResultSchema,
            joinType,
            buildSide,
            condition,
            left,
            right,
            jarList,
            buildTime,
            joinTime,
            totalTime,
            numOutputRows,
            sparkConf)
          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            vjoin.close()
          })
          val vjoinResult = vjoin.columnarJoin(streamIter, buildIter)
          new CloseableColumnBatchIterator(vjoinResult)
      }
    }
  }

}
