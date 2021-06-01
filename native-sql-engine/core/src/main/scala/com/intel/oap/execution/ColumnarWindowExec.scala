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

import java.util.concurrent.TimeUnit

import com.google.flatbuffers.FlatBufferBuilder
import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.expression.{CodeGeneration, ConverterUtils}
import com.intel.oap.vectorized.{ArrowWritableColumnVector, CloseableColumnBatchIterator, ExpressionEvaluator}
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Cast, Descending, Expression, Literal, MakeDecimal, NamedExpression, PredicateHelper, Rank, SortOrder, UnscaledValue, WindowExpression, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ExecutorManager
import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils

case class ColumnarWindowExec(windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends WindowExecBase {

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  // We no longer require for sorted input for columnar window
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "totalTime" -> SQLMetrics
        .createTimingMetric(sparkContext, "totaltime_window"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val totalTime = longMetric("totalTime")

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = ColumnarPluginConfig.getConf.numaBindingInfo

  def checkAggFunctionSpec(windowSpec: WindowSpecDefinition): Unit = {
    if (windowSpec.orderSpec.nonEmpty) {
      throw new UnsupportedOperationException("unsupported operation for " +
          "aggregation window function: " + windowSpec)
    }
  }

  def checkRankSpec(windowSpec: WindowSpecDefinition): Unit = {
    // leave it empty for now
  }

  def validateWindowFunctions(): Seq[(String, Expression)] = {
    val windowFunctions = windowExpression
        .map(e => e.asInstanceOf[Alias])
        .map(a => a.child.asInstanceOf[WindowExpression])
        .map(w => (w, w.windowFunction))
        .map {
          case (expr, func) =>
            (expr, func match {
              case a: AggregateExpression => a.aggregateFunction
              case b: WindowFunction => b
              case f =>
                throw new UnsupportedOperationException("unsupported window function type: " +
                    f)
            })
        }
        .map {
          case (expr, func) =>
            val name = func match {
              case _: Sum =>
                checkAggFunctionSpec(expr.windowSpec)
                "sum"
              case _: Average =>
                checkAggFunctionSpec(expr.windowSpec)
                "avg"
              case _: Min =>
                checkAggFunctionSpec(expr.windowSpec)
                "min"
              case _: Max =>
                checkAggFunctionSpec(expr.windowSpec)
                "max"
              case c: Count =>
                checkAggFunctionSpec(expr.windowSpec)
                if (c.children.exists(_.isInstanceOf[Literal])) {
                  "count_literal"
                } else {
                  "count"
                }
              case _: Rank =>
                checkRankSpec(expr.windowSpec)
                val desc: Option[Boolean] = orderSpec.foldLeft[Option[Boolean]](None) {
                  (desc, s) =>
                    val currentDesc = s.direction match {
                      case Ascending => false
                      case Descending => true
                      case _ => throw new IllegalStateException
                    }
                    if (desc.isEmpty) {
                      Some(currentDesc)
                    } else if (currentDesc == desc.get) {
                      Some(currentDesc)
                    } else {
                      throw new UnsupportedOperationException("Rank: clashed rank order found")
                    }
                }
                desc match {
                  case Some(true) => "rank_desc"
                  case Some(false) => "rank_asc"
                  case None => "rank_asc"
                }
              case f => throw new UnsupportedOperationException("unsupported window function: " + f)
            }
            (name, func)
        }
    if (windowFunctions.isEmpty) {
      throw new UnsupportedOperationException("zero window functions" +
          "specified in window")
    }
    windowFunctions
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val windowFunctions = validateWindowFunctions()
    child.executeColumnar().mapPartitionsWithIndex { (partIndex, iter) =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val prev1 = System.nanoTime()
        val gWindowFunctions = windowFunctions.map { case (n, f) =>
          TreeBuilder.makeFunction(n,
            f.children
              .flatMap {
                case a: AttributeReference =>
                  Some(TreeBuilder.makeField(
                    Field.nullable(a.name,
                      CodeGeneration.getResultType(a.dataType))))
                case c: Cast =>
                  Some(TreeBuilder.makeField(
                    Field.nullable(c.child.asInstanceOf[AttributeReference].name,
                      CodeGeneration.getResultType(c.dataType))
                  ))
                case _: Literal =>
                  None
                case _ =>
                  throw new IllegalStateException()
              }.toList.asJava,
            NoneType.NONE_TYPE)
        }
        val groupingExpressions = partitionSpec.map(e => e.asInstanceOf[AttributeReference])

        val gPartitionSpec = TreeBuilder.makeFunction("partitionSpec",
          groupingExpressions.map(e => TreeBuilder.makeField(
            Field.nullable(e.name,
              CodeGeneration.getResultType(e.dataType)))).toList.asJava,
          NoneType.NONE_TYPE)
        // Workaround:
        // Gandiva doesn't support serializing Struct type so far. Use a fake Binary type instead.
        val returnType = ArrowType.Binary.INSTANCE
        val fieldType = new FieldType(false, returnType, null)
        val resultField = new Field("window_res", fieldType,
          windowFunctions.map { case (_, f) =>
            CodeGeneration.getResultType(f.dataType)
          }.zipWithIndex.map { case (t, i) =>
            Field.nullable(s"window_res_" + i, t)
          }.asJava)

        val window = TreeBuilder.makeFunction("window",
          (gWindowFunctions.toList ++ List(gPartitionSpec)).asJava, returnType)

        val evaluator = new ExpressionEvaluator()
        val resultSchema = new Schema(resultField.getChildren)
        val arrowSchema = ArrowUtils.toArrowSchema(child.schema,
          SparkSchemaUtils.getGandivaCompatibleTimeZoneID())
        evaluator.build(arrowSchema,
          List(TreeBuilder.makeExpression(window,
            resultField)).asJava, resultSchema, true)
        val inputCache = new ListBuffer[ColumnarBatch]()
        val buildCost = System.nanoTime() - prev1
        totalTime += TimeUnit.NANOSECONDS.toMillis(buildCost)
        iter.foreach(c => {
          numInputBatches += 1
          val prev2 = System.nanoTime()
          inputCache += c
          (0 until c.numCols()).map(c.column)
              .foreach(_.asInstanceOf[ArrowWritableColumnVector].retain())
          val recordBatch = ConverterUtils.createArrowRecordBatch(c)
          try {
            evaluator.evaluate(recordBatch)
          } finally {
            recordBatch.close()
          }
          val evaluationCost = System.nanoTime() - prev2
          totalTime += TimeUnit.NANOSECONDS.toMillis(evaluationCost)
        })

        val prev3 = System.nanoTime()
        val batches = evaluator.finish()
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => evaluator.close())
        val windowFinishCost = System.nanoTime() - prev3
        totalTime += TimeUnit.NANOSECONDS.toMillis(windowFinishCost)
        val itr = batches.zipWithIndex.map {
          case (recordBatch, i) =>
            val prev4 = System.nanoTime()
            val length = recordBatch.getLength
            val vectors = try {
              ArrowWritableColumnVector.loadColumns(length, resultSchema, recordBatch)
            } finally {
              recordBatch.close()
            }
            val correspondingInputBatch = inputCache(i)
            val batch = new ColumnarBatch(
              (0 until correspondingInputBatch.numCols())
                  .map(i => correspondingInputBatch.column(i))
                  .toArray
                  ++ vectors, correspondingInputBatch.numRows())
            val emitCost = System.nanoTime() - prev4
            totalTime += TimeUnit.NANOSECONDS.toMillis(emitCost)
            numOutputRows += batch.numRows()
            numOutputBatches += 1
            batch
        }.toIterator
        new CloseableColumnBatchIterator(itr)
      }
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarWindowExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarWindowExec =>
      (that canEqual this) && (that eq this)
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(this)

  private object NoneType {
    val NONE_TYPE = new NoneType
  }

  private class NoneType extends ArrowType {
    override def getTypeID: ArrowType.ArrowTypeID = {
      return ArrowTypeID.NONE
    }

    override def getType(builder: FlatBufferBuilder): Int = {
      throw new UnsupportedOperationException()
    }

    override def toString: String = {
      return "NONE"
    }

    override def accept[T](visitor: ArrowType.ArrowTypeVisitor[T]): T = {
      throw new UnsupportedOperationException()
    }

    override def isComplex: Boolean = false
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

object ColumnarWindowExec extends Logging {

  object AddProjectionsAroundWindow extends Rule[SparkPlan] with PredicateHelper {
    def makeInputProject(ex: Expression, inputProjects: ListBuffer[NamedExpression]): Expression = {
      ex match {
        case ae: AggregateExpression => ae.withNewChildren(
          ae.children.map(makeInputProject(_, inputProjects)))
        case ae: WindowExpression => ae.withNewChildren(
          ae.children.map(makeInputProject(_, inputProjects)))
        case func @ (_: AggregateFunction | _: WindowFunction) =>
          val params = func.children
          // rewrite
          val rewritten = func match {
            case _: Average =>
              // rewrite params for AVG
              params.map {
                param =>
                  param.dataType match {
                    case _: LongType | _: DecimalType =>
                      Cast(param, DoubleType)
                    case _ => param
                  }
              }
            case _ => params
          }

          // alias
          func.withNewChildren(rewritten.map {
            case param @ (_: Cast | _: UnscaledValue) =>
              val aliasName = "__alias_%d__".format(Random.nextLong())
              val alias = Alias(param, aliasName)()
              inputProjects.append(alias)
              alias.toAttribute
            case other => other
          })
        case other => other
      }
    }

    def sameType(from: DataType, to: DataType): Boolean = {
      if (from == null || to == null) {
        throw new IllegalArgumentException("null type found during type enforcement")
      }
      if (from == to) {
        return true
      }
      DataType.equalsStructurally(from, to)
    }

    def makeOutputProject(ex: Expression, windows: ListBuffer[NamedExpression],
        inputProjects: ListBuffer[NamedExpression]): Expression = {
      val out = ex match {
        case we: WindowExpression =>
          val aliasName = "__alias_%d__".format(Random.nextLong())
          val alias = Alias(makeInputProject(we, inputProjects), aliasName)()
          windows.append(alias)
          alias.toAttribute
        case _ =>
          ex.withNewChildren(ex.children.map(makeOutputProject(_, windows, inputProjects)))
      }
      // forcibly cast to original type against possible rewriting
      val casted = try {
        if (sameType(out.dataType, ex.dataType)) {
          out
        } else {
          Cast(out, ex.dataType)
        }
      } catch {
        case t: Throwable =>
          // scalastyle:off println
          System.err.println("Warning: " + t.getMessage)
          Cast(out, ex.dataType)
        // scalastyle:on println
      }
      casted
    }

    override def apply(plan: SparkPlan): SparkPlan = plan transformUp {
      case p @ ColumnarWindowExec(windowExpression, partitionSpec, orderSpec, child) =>
        val windows = ListBuffer[NamedExpression]()
        val inProjectExpressions = ListBuffer[NamedExpression]()
        val outProjectExpressions = windowExpression.map(e => e.asInstanceOf[Alias])
          .map { a =>
            a.withNewChildren(List(makeOutputProject(a.child, windows, inProjectExpressions)))
              .asInstanceOf[NamedExpression]
          }
        val inputProject = ColumnarConditionProjectExec(null,
          child.output ++ inProjectExpressions, child)
        val window = new ColumnarWindowExec(windows, partitionSpec, orderSpec, inputProject)
        val outputProject = ColumnarConditionProjectExec(null,
          child.output ++ outProjectExpressions, window)
        outputProject
    }
  }

  object RemoveSort extends Rule[SparkPlan] with PredicateHelper {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case p1 @ ColumnarWindowExec(_, _, _, p2 @ (_: SortExec | _: ColumnarSortExec)) =>
        p1.withNewChildren(p2.children)
    }
  }

  object RemoveCoalesceBatches extends Rule[SparkPlan] with PredicateHelper {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case p1 @ ColumnarWindowExec(_, _, _, p2: CoalesceBatchesExec) =>
        p1.withNewChildren(p2.children)
    }
  }

  /**
   * FIXME casting solution for timestamp/date32 support
   */
  object CastMutableTypes extends Rule[SparkPlan] with PredicateHelper {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case p: ColumnarWindowExec => p.transformExpressionsDown {
        case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _, _), _) => af match {
          case Min(e) => e.dataType match {
            case t @ (_: TimestampType) =>
              Cast(we.copy(
                windowFunction =
                    ae.copy(aggregateFunction = Min(Cast(e, LongType)))), TimestampType)
            case t @ (_: DateType) =>
              Cast(
                Cast(we.copy(
                  windowFunction =
                      ae.copy(aggregateFunction = Min(Cast(Cast(e, TimestampType,
                        Some(SparkSchemaUtils.getGandivaCompatibleTimeZoneID())), LongType)))),
                  TimestampType), DateType, Some(SparkSchemaUtils.getGandivaCompatibleTimeZoneID()))
            case _ => we
          }
          case Max(e) => e.dataType match {
            case t @ (_: TimestampType) =>
              Cast(we.copy(
                windowFunction =
                    ae.copy(aggregateFunction = Max(Cast(e, LongType)))), TimestampType)
            case t @ (_: DateType) =>
              Cast(
                Cast(we.copy(
                  windowFunction =
                      ae.copy(aggregateFunction = Max(Cast(Cast(e, TimestampType,
                        Some(SparkSchemaUtils.getGandivaCompatibleTimeZoneID())), LongType)))),
                  TimestampType), DateType, Some(SparkSchemaUtils.getGandivaCompatibleTimeZoneID()))
            case _ => we
          }
          case _ => we
        }
      }
    }
  }

  object Validate extends Rule[SparkPlan] with PredicateHelper {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case w: ColumnarWindowExec =>
        w.validateWindowFunctions()
        w
    }
  }

  object ColumnarWindowOptimizations extends RuleExecutor[SparkPlan] {
    override protected def batches: Seq[ColumnarWindowOptimizations.Batch] =
      Batch("Remove Sort", FixedPoint(10), RemoveSort) ::
          Batch("Remove Coalesce Batches", FixedPoint(10), RemoveCoalesceBatches) ::
//          Batch("Cast Mutable Types", Once, CastMutableTypes) ::
          Batch("Add Projections", FixedPoint(1), AddProjectionsAroundWindow) ::
          Batch("Validate", Once, Validate) ::
          Nil
  }

  def optimize(plan: ColumnarWindowExec): SparkPlan = {
    ColumnarWindowOptimizations.execute(plan)
  }

  def createWithOptimizations(windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): SparkPlan = {
    val columnar = new ColumnarWindowExec(
      windowExpression,
      partitionSpec,
      orderSpec,
      child)
    ColumnarWindowExec.optimize(columnar)
  }
}
