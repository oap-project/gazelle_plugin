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
import com.google.common.collect.Lists
import java.util.concurrent.TimeUnit._

import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{UserAddedJarUtils, Utils, ExecutorManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.unsafe.KVIterator

import scala.collection.Iterator

/**
 * Columnar Based HashAggregateExec.
 */
case class ColumnarHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends BaseAggregateExec
    with ColumnarCodegenSupport
    with AliasAwareOutputPartitioning {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = ColumnarPluginConfig.getConf.numaBindingInfo
  override def supportsColumnar = true

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Members declared in org.apache.spark.sql.execution.CodegenSupport
  protected def doProduce(ctx: CodegenContext): String = throw new UnsupportedOperationException()

  // Members declared in org.apache.spark.sql.catalyst.plans.QueryPlan
  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  // Members declared in org.apache.spark.sql.execution.SparkPlan
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException()

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation process"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashagg"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val aggTime = longMetric("aggTime")
  val totalTime = longMetric("processTime")
  numOutputRows.set(0)
  numOutputBatches.set(0)
  numInputBatches.set(0)

  buildCheck()

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    var eval_elapse: Long = 0
    child.executeColumnar().mapPartitions { iter =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      val native_function = TreeBuilder.makeFunction(
        s"standalone",
        Lists.newArrayList(getKernelFunction),
        new ArrowType.Int(32, true))
      val hash_aggr_expr =
        TreeBuilder
          .makeExpression(native_function, Field.nullable("result", new ArrowType.Int(32, true)))
      val hash_aggr_input_schema = ConverterUtils.toArrowSchema(child.output)
      val hash_aggr_out_schema = ConverterUtils.toArrowSchema(output)
      val resultStructType = ArrowUtils.fromArrowSchema(hash_aggr_out_schema)
      val nativeKernel = new ExpressionEvaluator()
      nativeKernel
        .build(
          hash_aggr_input_schema,
          Lists.newArrayList(hash_aggr_expr),
          hash_aggr_out_schema,
          true)
      val nativeIterator = nativeKernel.finishByIterator()

      def close = {
        aggTime += (eval_elapse / 1000000)
        totalTime += (eval_elapse / 1000000)
        nativeKernel.close
        nativeIterator.close
      }

      // now we can return this wholestagecodegen iter
      val res = new Iterator[ColumnarBatch] {
        var processed = false
        def process: Unit = {
          while (iter.hasNext) {
            val cb = iter.next()
            numInputBatches += 1
            if (cb.numRows != 0) {
              val beforeEval = System.nanoTime()
              val input_rb =
                ConverterUtils.createArrowRecordBatch(cb)
              nativeIterator.processAndCacheOne(hash_aggr_input_schema, input_rb)
              ConverterUtils.releaseArrowRecordBatch(input_rb)
              eval_elapse += System.nanoTime() - beforeEval
            }
          }
          processed = true
        }
        override def hasNext: Boolean = {
          if (!processed) process
          nativeIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          if (!processed) process
          val beforeEval = System.nanoTime()
          val output_rb = nativeIterator.next
          if (output_rb == null) {
            eval_elapse += System.nanoTime() - beforeEval
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
          val outputNumRows = output_rb.getLength
          val output = ConverterUtils.fromArrowRecordBatch(hash_aggr_out_schema, output_rb)
          ConverterUtils.releaseArrowRecordBatch(output_rb)
          eval_elapse += System.nanoTime() - beforeEval
          numOutputRows += outputNumRows
          numOutputBatches += 1
          new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]), outputNumRows)
        }
      }
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        close
      })
      new CloseableColumnBatchIterator(res)
    }
  }

  def buildCheck(): Unit = {
    // check input datatype
    for (attr <- child.output) {
      try {
        ConverterUtils.checkIfTypeSupported(attr.dataType)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarAggregation")
      }
    }
    // check output datatype
    resultExpressions.foreach(expr => {
      try {
        ConverterUtils.checkIfTypeSupported(expr.dataType)
      } catch {
        case e : UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${expr.dataType} is not supported in ColumnarAggregation")
      }
    })
    // check project
    for (expr <- aggregateExpressions) {
      val internalExpressionList = expr.aggregateFunction.children
      ColumnarProjection.buildCheck(child.output, internalExpressionList)
    }
    ColumnarProjection.buildCheck(child.output, groupingExpressions)
    ColumnarProjection.buildCheck(child.output, resultExpressions)
    // check aggregate expressions
    checkAggregate(aggregateExpressions)
  }

  def checkAggregate(aggregateExpressions: Seq[AggregateExpression]): Unit = {
    for (expr <- aggregateExpressions) {
      val mode = expr.mode
      val aggregateFunction = expr.aggregateFunction
      aggregateFunction match {
        case Average(_) | Sum(_) | Count(_) | Max(_) | Min(_) =>
        case StddevSamp(_) =>
          mode match {
            case Partial | Final =>
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
      mode match {
        case Partial | PartialMerge | Final =>
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    }
  }

  /** ColumnarCodegenSupport **/
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.getBuildPlans
    case _ =>
      Seq()
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

  override def supportColumnarCodegen: Boolean = true

  // override def canEqual(that: Any): Boolean = false

  def getKernelFunction: TreeNode = {
    ColumnarHashAggregation.prepareKernelFunction(
      groupingExpressions,
      child.output,
      aggregateExpressions,
      aggregateAttributes,
      resultExpressions,
      output,
      sparkConf)
  }

  override def doCodeGen: ColumnarCodegenContext = {

    val childCtx = child match {
      case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
        c.doCodeGen
      case _ =>
        null
    }
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
        ConverterUtils.toArrowSchema(child.output))
    }
    val outputSchema = ConverterUtils.toArrowSchema(output)
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  /****************************/
  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"ColumnarHashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"ColumnarHashAggregate(keys=$keyString, functions=$functionString)"
    }
  }
}
