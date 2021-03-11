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
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Cast, Descending, Expression, MakeDecimal, NamedExpression, Rank, SortOrder, UnscaledValue, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Sum}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, DoubleType, LongType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ExecutorManager

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

class ColumnarWindowExec(windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends WindowExec(windowExpression,
  partitionSpec, orderSpec, child) {

  override def supportsColumnar = true

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  // We no longer require for sorted input for columnar window
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

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

  val windowFunctions: Seq[(String, Expression)] = windowExpression
      .map(e => e.asInstanceOf[Alias])
      .map(a => a.child.asInstanceOf[WindowExpression])
      .map(w => w.windowFunction)
      .map {
        case a: AggregateExpression => a.aggregateFunction
        case b: WindowFunction => b
        case f =>
          throw new UnsupportedOperationException("unsupported window function type: " +
              f)
      }
      .map { f =>
        val name = f match {
          case _: Sum => "sum"
          case _: Average => "avg"
          case _: Rank =>
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
        (name, f)
      }

  if (windowFunctions.isEmpty) {
    throw new UnsupportedOperationException("zero window functions" +
        "specified in window")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitionsWithIndex { (partIndex, iter) =>
      ExecutorManager.tryTaskSet(numaBindingInfo)
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val prev1 = System.nanoTime()
        val gWindowFunctions = windowFunctions.map { case (n, f) =>
          TreeBuilder.makeFunction(n,
            f.children
                .map(e =>
                  e match {
                    case a: AttributeReference =>
                      TreeBuilder.makeField(
                        Field.nullable(a.name,
                          CodeGeneration.getResultType(a.dataType)))
                    case c: Cast =>
                      TreeBuilder.makeField(
                        Field.nullable(c.child.asInstanceOf[AttributeReference].name,
                          CodeGeneration.getResultType(c.dataType))
                      )
                  }).toList.asJava,
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
        val arrowSchema = ArrowUtils.toArrowSchema(child.schema, SQLConf.get.sessionLocalTimeZone)
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
        val itr = batches.zipWithIndex.map { case (recordBatch, i) => {
          val prev4 = System.nanoTime()
          val length = recordBatch.getLength
          val vectors = try {
             ArrowWritableColumnVector.loadColumns(length, resultSchema, recordBatch)
          } finally {
            recordBatch.close()
          }
          val correspondingInputBatch = inputCache(i)
          val batch = new ColumnarBatch(
            (0 until correspondingInputBatch.numCols()).map(i => correspondingInputBatch.column(i)).toArray
                ++ vectors, correspondingInputBatch.numRows())
          val emitCost = System.nanoTime() - prev4
          totalTime += TimeUnit.NANOSECONDS.toMillis(emitCost)
          numOutputRows += batch.numRows()
          numOutputBatches += 1
          batch
        }}.toIterator
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
}

object ColumnarWindowExec {

  def createWithProjection(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): SparkPlan = {

    def makeInputProject(ex: Expression, inputProjects: ListBuffer[NamedExpression]): Expression = {
      ex match {
        case ae: AggregateExpression => ae.withNewChildren(ae.children.map(makeInputProject(_, inputProjects)))
        case ae: WindowExpression => ae.withNewChildren(ae.children.map(makeInputProject(_, inputProjects)))
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

    def makeOutputProject(ex: Expression, windows: ListBuffer[NamedExpression], inputProjects: ListBuffer[NamedExpression]): Expression = {
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
          System.err.println("Warning: " + t.getMessage)
          Cast(out, ex.dataType)
      }
      casted
    }

    val windows = ListBuffer[NamedExpression]()
    val inProjectExpressions = ListBuffer[NamedExpression]()
    val outProjectExpressions = windowExpression.map(e => e.asInstanceOf[Alias])
        .map { a =>
          a.withNewChildren(List(makeOutputProject(a.child, windows, inProjectExpressions)))
              .asInstanceOf[NamedExpression]
        }

    val inputProject = ColumnarConditionProjectExec(null, child.output ++ inProjectExpressions, child)

    val window = new ColumnarWindowExec(windows, partitionSpec, orderSpec, inputProject)

    val outputProject = ColumnarConditionProjectExec(null, child.output ++ outProjectExpressions, window)

    outputProject
  }

  def create(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): SparkPlan = {
    createWithProjection(windowExpression, partitionSpec, orderSpec, child)
  }
}
