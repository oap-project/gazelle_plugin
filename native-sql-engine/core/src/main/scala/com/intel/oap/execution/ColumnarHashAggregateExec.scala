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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.KVIterator
import scala.collection.JavaConverters._

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

  val onlyResultExpressions: Boolean =
    if (groupingExpressions.isEmpty && aggregateExpressions.isEmpty &&
      child.output.isEmpty && resultExpressions.nonEmpty) true
    else false

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

      var numRowsInput = 0
      var hasNextCount = 0
      // now we can return this wholestagecodegen iter
      val res = new Iterator[ColumnarBatch] {
        var processed = false
        /** Three special cases need to be handled in scala side:
         * (1) count_literal (2) only result expressions (3) empty input
         */
        var skip_native = false
        var onlyResExpr = false
        var emptyInput = false
        var count_num_row = 0
        def process: Unit = {
          while (iter.hasNext) {
            val cb = iter.next()
            numInputBatches += 1
            if (cb.numRows != 0) {
              numRowsInput += cb.numRows
              val beforeEval = System.nanoTime()
              if (hash_aggr_input_schema.getFields.size == 0 &&
                  aggregateExpressions.nonEmpty &&
                  aggregateExpressions.head.aggregateFunction.isInstanceOf[Count]) {
                // This is a special case used by only do count literal
                count_num_row += cb.numRows
                skip_native = true
              } else {
                val input_rb =
                  ConverterUtils.createArrowRecordBatch(cb)
                nativeIterator.processAndCacheOne(hash_aggr_input_schema, input_rb)
                ConverterUtils.releaseArrowRecordBatch(input_rb)
              }
              eval_elapse += System.nanoTime() - beforeEval
            }
          }
          processed = true
        }
        override def hasNext: Boolean = {
          hasNextCount += 1
          if (!processed) process
          if (skip_native) {
            count_num_row > 0
          } else if (onlyResultExpressions && hasNextCount == 1) {
            onlyResExpr = true
            true
          } else if (!onlyResultExpressions && groupingExpressions.isEmpty &&
                     numRowsInput == 0 && hasNextCount == 1) {
            emptyInput = true
            true
          } else {
            nativeIterator.hasNext
          }
        }

        override def next(): ColumnarBatch = {
          if (!processed) process
          val beforeEval = System.nanoTime()
          if (skip_native) {
            // special handling for only count literal in this operator
            getResForCountLiteral
          } else if (onlyResExpr) {
            // special handling for only result expressions
            getResForOnlyResExpr
          } else if (emptyInput) {
            // special handling for empty input batch
            getResForEmptyInput
          } else {
            val output_rb = nativeIterator.next
            if (output_rb == null) {
              eval_elapse += System.nanoTime() - beforeEval
              val resultColumnVectors =
                ArrowWritableColumnVector.allocateColumns(0, resultStructType)
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
        def getResForCountLiteral: ColumnarBatch = {
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType)
          if (count_num_row == 0) {
            new ColumnarBatch(
              resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          } else {
            val out_res = count_num_row
            count_num_row = 0
            for (idx <- resultColumnVectors.indices) {
              resultColumnVectors(idx).dataType match {
                case t: IntegerType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].intValue)
                case t: LongType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].longValue)
                case t: DoubleType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].doubleValue())
                case t: FloatType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].floatValue())
                case t: ByteType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].byteValue())
                case t: ShortType =>
                  resultColumnVectors(idx)
                    .put(0, out_res.asInstanceOf[Number].shortValue())
                case t: StringType =>
                  val values = (out_res :: Nil).map(_.toByte).toArray
                  resultColumnVectors(idx)
                    .putBytes(0, 1, values, 0)
              }
            }
            new ColumnarBatch(
              resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 1)
          }
        }
        def getResForOnlyResExpr: ColumnarBatch = {
          // This function has limited support for only-result-expression case.
          // Fake input for projection:
          val inputColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType)
          val valueVectors =
            inputColumnVectors.map(columnVector => columnVector.getValueVector).toList
          val projector = ColumnarProjection.create(child.output, resultExpressions)
          val resultColumnVectorList = projector.evaluate(1, valueVectors)
          new ColumnarBatch(
            resultColumnVectorList.map(v => v.asInstanceOf[ColumnVector]).toArray,
            1)
        }
        def getResForEmptyInput: ColumnarBatch = {
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType)
          if (aggregateExpressions.isEmpty) {
            // To align with spark, in this case, one empty row is returned.
            return new ColumnarBatch(
              resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 1)
          }
          // If groupby is not required, for Final mode, a default value will be
          // returned if input is empty.
          var idx = 0
          for (expr <- aggregateExpressions) {
            expr.aggregateFunction match {
              case Average(_) | StddevSamp(_) | Sum(_) | Max(_) | Min(_) =>
                expr.mode match {
                  case Final =>
                    resultColumnVectors(idx).putNull(0)
                    idx += 1
                  case _ =>
                }
              case Count(_) =>
                expr.mode match {
                  case Final =>
                    val out_res = 0
                    resultColumnVectors(idx).dataType match {
                      case t: IntegerType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].intValue)
                      case t: LongType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].longValue)
                      case t: DoubleType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].doubleValue())
                      case t: FloatType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].floatValue())
                      case t: ByteType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].byteValue())
                      case t: ShortType =>
                        resultColumnVectors(idx)
                          .put(0, out_res.asInstanceOf[Number].shortValue())
                      case t: StringType =>
                        val values = (out_res :: Nil).map(_.toByte).toArray
                        resultColumnVectors(idx)
                          .putBytes(0, 1, values, 0)
                    }
                    idx += 1
                  case _ =>
                }
              case other =>
                throw new UnsupportedOperationException(s"not currently supported: $other.")
            }
          }
          // will only put default value for Final mode
          aggregateExpressions.head.mode match {
            case Final =>
              new ColumnarBatch(
                resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 1)
            case _ =>
              new ColumnarBatch(
                resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
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
        case e: UnsupportedOperationException =>
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
