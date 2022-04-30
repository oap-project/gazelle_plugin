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

import com.google.common.collect.Lists
import com.intel.oap.GazellePluginConfig
import com.intel.oap.expression._
import com.intel.oap.vectorized.{ExpressionEvaluator, _}
import com.intel.oap.sql.shims.SparkShimLoader
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.joins.{BaseJoinExec, HashJoin, ShuffledJoin}
import org.apache.spark.sql.execution.joins.HashedRelationInfo
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.{ExecutorManager, UserAddedJarUtils}
import org.apache.spark.sql.types.DecimalType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ColumnarBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    projectList: Seq[NamedExpression] = null,
    nullAware: Boolean = false)
    extends BaseJoinExec
    with ColumnarCodegenSupport
    with ColumnarShuffledJoin {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GazellePluginConfig.getConf.numaBindingInfo
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_broadcastHasedJoin"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"),
    "fetchTime" -> SQLMetrics.createTimingMetric(sparkContext, "broadcast result fetch time"))

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

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

  buildCheck()

  // A method in ShuffledJoin of spark3.2.
  def isSkewJoin: Boolean = false

  def buildCheck(): Unit = {
    joinType match {
      case _: InnerLike =>
      case LeftSemi | LeftOuter | RightOuter | LeftAnti =>
      case j: ExistenceJoin =>
      case _ =>
        throw new UnsupportedOperationException(s"Join Type ${joinType} is not supported yet.")
    }
    // build check for condition
    val conditionExpr: Expression = condition.orNull
    if (conditionExpr != null) {
      val columnarConditionExpr =
        ColumnarExpressionConverter.replaceWithColumnarExpression(conditionExpr)
      val supportCodegen =
        columnarConditionExpr.asInstanceOf[ColumnarExpression].supportColumnarCodegen(null)
      // Columnar BHJ with condition only has codegen version of implementation.
      if (!supportCodegen) {
        throw new UnsupportedOperationException(
          "Condition expression is not fully supporting codegen!")
      }
    }
    // build check types
    for (attr <- streamedPlan.output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
        //if (attr.dataType.isInstanceOf[DecimalType])
        //  throw new UnsupportedOperationException(s"Unsupported data type: ${attr.dataType}")
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarBroadcastHashJoinExec.")
      }
    }
    for (attr <- buildPlan.output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarBroadcastHashJoinExec.")
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
    if (projectList == null) super.output
    else projectList.map(_.toAttribute)
  def getBuildPlan: SparkPlan = buildPlan
  override def supportsColumnar = true
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"ColumnarBroadcastHashJoinExec doesn't support doExecute")
  }
  val isNullAwareAntiJoin : Boolean = nullAware

  override lazy val outputPartitioning: Partitioning = {
    val broadcastHashJoinOutputPartitioningExpandLimit: Int =
      SparkShimLoader
        .getSparkShims
        .getBroadcastHashJoinOutputPartitioningExpandLimit(this: SparkPlan)
    joinType match {
      case _: InnerLike if broadcastHashJoinOutputPartitioningExpandLimit > 0 =>
        streamedPlan.outputPartitioning match {
          case h: HashPartitioning => expandOutputPartitioning(h)
          case c: PartitioningCollection => expandOutputPartitioning(c)
          case other => other
        }
      case _ => streamedPlan.outputPartitioning
    }
  }

  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeyExprs.zip(buildKeyExprs).foreach {
      case (streamedKey, buildKey) =>
        val key = streamedKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ buildKey)
          case None => mapping.put(key, Seq(buildKey))
        }
    }
    mapping.toMap
  }

  // Expands the given partitioning collection recursively.
  private def expandOutputPartitioning(partitioning: PartitioningCollection): PartitioningCollection = {
    PartitioningCollection(partitioning.partitionings.flatMap {
      case h: HashPartitioning => expandOutputPartitioning(h).partitionings
      case c: PartitioningCollection => Seq(expandOutputPartitioning(c))
      case other => Seq(other)
    })
  }

  // Expands the given hash partitioning by substituting streamed keys with build keys.
  // For example, if the expressions for the given partitioning are Seq("a", "b", "c")
  // where the streamed keys are Seq("b", "c") and the build keys are Seq("x", "y"),
  // the expanded partitioning will have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  // The expanded expressions are returned as PartitioningCollection.
  private def expandOutputPartitioning(partitioning: HashPartitioning): PartitioningCollection = {
    val maxNumCombinations =
      SparkShimLoader
        .getSparkShims
        .getBroadcastHashJoinOutputPartitioningExpandLimit(this: SparkPlan)
    var currentNumCombinations = 0

    def generateExprCombinations(current: Seq[Expression],
                                 accumulated: Seq[Expression]): Seq[Seq[Expression]] = {
      if (currentNumCombinations >= maxNumCombinations) {
        Nil
      } else if (current.isEmpty) {
        currentNumCombinations += 1
        Seq(accumulated)
      } else {
        val buildKeysOpt = streamedKeyToBuildKeyMapping.get(current.head.canonicalized)
        generateExprCombinations(current.tail, accumulated :+ current.head) ++
          buildKeysOpt.map(_.flatMap(b => generateExprCombinations(current.tail, accumulated :+ b)))
            .getOrElse(Nil)
      }
    }

    PartitioningCollection(
      generateExprCombinations(partitioning.expressions, Nil)
        .map(HashPartitioning(_, partitioning.numPartitions)))
  }

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

  override def dependentPlanCtx: ColumnarCodegenContext = {
    val inputSchema = ConverterUtils.toArrowSchema(buildPlan.output)
    ColumnarCodegenContext(
      inputSchema,
      null,
      ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, buildPlan.output, 2))
  }

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def getChild: SparkPlan = streamedPlan

  override def supportColumnarCodegen: Boolean = true

  val output_skip_alias =
    if (projectList == null || projectList.isEmpty) super.output
    else projectList.map(expr => ConverterUtils.getAttrFromExpr(expr, true))

  def getKernelFunction: TreeNode = {

    val buildInputAttributes: List[Attribute] = buildPlan.output.toList
    val streamInputAttributes: List[Attribute] = streamedPlan.output.toList
    ColumnarConditionedProbeJoin.prepareKernelFunction(
      buildKeyExprs,
      streamedKeyExprs,
      buildInputAttributes,
      streamInputAttributes,
      output_skip_alias,
      joinType,
      buildSide,
      condition,
      1,
      isNullAwareAntiJoin = isNullAwareAntiJoin)
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
          Lists.newArrayList(getKernelFunction, childCtx.root),
          new ArrowType.Int(32, true)),
        childCtx.inputSchema)
    } else {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction),
          new ArrowType.Int(32, true)),
        ConverterUtils.toArrowSchema(streamedPlan.output))
    }
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  def doCodeGenForStandalone: ColumnarCodegenContext = {
    val outputSchema = ConverterUtils.toArrowSchema(output)
    val (codeGenNode, inputSchema) = (
      TreeBuilder.makeFunction(
        s"child",
        Lists.newArrayList(getKernelFunction),
        new ArrowType.Int(32, true)),
      ConverterUtils.toArrowSchema(streamedPlan.output))
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // we will use previous codegen join to handle joins with condition
    if (condition.isDefined) {
      return getCodeGenIterator
    }

    // below only handles join without condition
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val totalTime = longMetric("processTime")
    val buildTime = longMetric("buildTime")
    val joinTime = longMetric("joinTime")
    val fetchTime = longMetric("fetchTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    val buildInputByteBuf = buildPlan.executeBroadcast[ColumnarHashedRelation]()

    streamedPlan.executeColumnar().mapPartitions { iter =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      val hashRelationKernel = new ExpressionEvaluator()
      val hashRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()
      // received broadcast value contain a hashmap and raw recordBatch
      val beforeFetch = System.nanoTime()
      val relation = buildInputByteBuf.value.asReadOnlyCopy
      fetchTime += ((System.nanoTime() - beforeFetch) / 1000000)
      val beforeEval = System.nanoTime()
      val hashRelationObject = relation.hashRelationObj
      val depIter =
        new CloseableColumnBatchIterator(relation.getColumnarBatchAsIter)
      val hash_relation_function =
        ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, buildPlan.output, 2)
      val hash_relation_schema = ConverterUtils.toArrowSchema(buildPlan.output)
      val hash_relation_expr =
        TreeBuilder.makeExpression(
          hash_relation_function,
          Field.nullable("result", new ArrowType.Int(32, true)))
      hashRelationKernel.build(hash_relation_schema, Lists.newArrayList(hash_relation_expr), true)
      val hashRelationResultIterator = hashRelationKernel.finishByIterator()

      // we need to set original recordBatch to hashRelationKernel
      var numRows = 0
      while (depIter.hasNext) {
        val dep_cb = depIter.next()
        if (dep_cb.numRows > 0) {
          (0 until dep_cb.numCols).toList.foreach(i =>
            dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
          hashRelationBatchHolder += dep_cb
          val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
          hashRelationResultIterator.processAndCacheOne(hash_relation_schema, dep_rb)
          ConverterUtils.releaseArrowRecordBatch(dep_rb)
          numRows += dep_cb.numRows
        }
      }

      // we need to set hashRelationObject to hashRelationResultIterator
      hashRelationResultIterator.setHashRelationObject(hashRelationObject)

      val native_function = TreeBuilder.makeFunction(
        s"standalone",
        Lists.newArrayList(getKernelFunction),
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
      build_elapse += (System.nanoTime() - beforeEval)
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

      // now we can return this wholestagecodegen iter
      val resultStructType = ArrowUtils.fromArrowSchema(probe_out_schema)
      val res = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          iter.hasNext
        }

        override def next(): ColumnarBatch = {
          val cb = iter.next()
          val beforeEval = System.nanoTime()
          if (cb.numRows == 0) {
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val input_rb =
            ConverterUtils.createArrowRecordBatch(cb)
          val output_rb = nativeIterator.process(probe_input_schema, input_rb)
          if (output_rb == null) {
            ConverterUtils.releaseArrowRecordBatch(input_rb)
            eval_elapse += System.nanoTime() - beforeEval
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val outputNumRows = output_rb.getLength
          ConverterUtils.releaseArrowRecordBatch(input_rb)
          val resBatch = if (probe_out_schema.getFields.size() == 0) {
            // If no col is selected by Projection, empty batch will be returned.
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType)
            ConverterUtils.releaseArrowRecordBatch(output_rb)
            eval_elapse += System.nanoTime() - beforeEval
            new ColumnarBatch(
              resultColumnVectors.map(_.asInstanceOf[ColumnVector]), outputNumRows)
          } else {
            val output = ConverterUtils.fromArrowRecordBatch(probe_out_schema, output_rb)
            ConverterUtils.releaseArrowRecordBatch(output_rb)
            eval_elapse += System.nanoTime() - beforeEval
            new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
          }
          numOutputRows += outputNumRows
          resBatch
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

  def getCodeGenSignature = {
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

  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = GazellePluginConfig.getRandomTempDir
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

  def getCodeGenIterator: RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val totalTime = longMetric("processTime")
    val buildTime = longMetric("buildTime")
    val joinTime = longMetric("joinTime")
    val fetchTime = longMetric("fetchTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0

    val signature = getCodeGenSignature
    val listJars = uploadAndListJars(signature)
    val buildInputByteBuf = buildPlan.executeBroadcast[ColumnarHashedRelation]()
    val hashRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()

    streamedPlan.executeColumnar().mapPartitions { streamIter =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
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
      val resCtx = getCodeGenCtx
      val expression =
        TreeBuilder
          .makeExpression(resCtx.root, Field.nullable("result", new ArrowType.Int(32, true)))
      val nativeKernel = new ExpressionEvaluator(jarList.toList.asJava)
      nativeKernel
        .build(resCtx.inputSchema, Lists.newArrayList(expression), resCtx.outputSchema, true)

      // received broadcast value contain a hashmap and raw recordBatch
      val beforeFetch = System.nanoTime()
      val relation = buildInputByteBuf.value.asReadOnlyCopy
      fetchTime += ((System.nanoTime() - beforeFetch) / 1000000)
      val beforeEval = System.nanoTime()
      val hashRelationObject = relation.hashRelationObj
      val depIter =
        new CloseableColumnBatchIterator(relation.getColumnarBatchAsIter)
      val ctx = dependentPlanCtx
      val hash_relation_expression = TreeBuilder
        .makeExpression(ctx.root, Field.nullable("result", new ArrowType.Int(32, true)))
      val hashRelationKernel = new ExpressionEvaluator()
      hashRelationKernel
        .build(ctx.inputSchema, Lists.newArrayList(hash_relation_expression), true)
      val hashRelationResultIterator = hashRelationKernel.finishByIterator()
      // we need to set original recordBatch to hashRelationKernel
      while (depIter.hasNext) {
        val dep_cb = depIter.next()
        if (dep_cb.numRows > 0) {
          (0 until dep_cb.numCols).toList.foreach(i =>
            dep_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
          hashRelationBatchHolder += dep_cb
          val dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb)
          hashRelationResultIterator.processAndCacheOne(ctx.inputSchema, dep_rb)
          ConverterUtils.releaseArrowRecordBatch(dep_rb)
        }
      }
      // we need to set hashRelationObject to hashRelationResultIterator
      hashRelationResultIterator.setHashRelationObject(hashRelationObject)
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
          new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
        }
      }
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        close
      })
      new CloseableColumnBatchIterator(res)
    }

  }

  // For spark 3.2.
  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan):
  ColumnarBroadcastHashJoinExec =
    copy(left = newLeft, right = newRight)
}
