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

package com.intel.oap.expression

import org.apache.arrow.memory.ArrowBuf
import java.util.ArrayList
import java.util.Collections
import java.util.concurrent.TimeUnit._
import util.control.Breaks._

import com.intel.oap.GazellePluginConfig
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.util.ArrowUtils
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.TaskContext

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

class ColumnarHashAggregation(
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    output: Seq[Attribute],
    sparkConf: SparkConf)
    extends Logging {

  var inputAttrQueue: scala.collection.mutable.Queue[Attribute] = _
  val resultType = CodeGeneration.getResultType()
  val NaN_check : Boolean = GazellePluginConfig.getConf.enableColumnarNaNCheck

  def getColumnarFuncNode(expr: Expression): TreeNode = {
    try {
      if (expr.isInstanceOf[AttributeReference] && expr
            .asInstanceOf[AttributeReference]
            .name == "none") {
        throw new UnsupportedOperationException(
          s"Unsupport to generate native expression from replaceable expression.")
      }
      var columnarExpr: Expression =
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
      var inputList: java.util.List[Field] = Lists.newArrayList()
      val (node, _resultType) =
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(inputList)
      node
    } catch {
      case e: Throwable =>
        val errmsg = e.getStackTrace.mkString("\n")
        logError(s"getColumnarFuncNode failed, expr is ${expr}, forward throw this exception")
        throw e
    }
  }

  def getColumnarFuncNode(aggregateExpression: AggregateExpression): TreeNode = {
    val aggregateFunc = aggregateExpression.aggregateFunction
    val mode = aggregateExpression.mode
    try {
      aggregateFunc match {
        case _: Average =>
          mode match {
            case Partial =>
              val childrenColumnarFuncNodeList =
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              TreeBuilder
                .makeFunction("action_sum_count", childrenColumnarFuncNodeList.asJava, resultType)
            case PartialMerge =>
              val childrenColumnarFuncNodeList =
                List(inputAttrQueue.dequeue, inputAttrQueue.dequeue).map(attr =>
                  getColumnarFuncNode(attr))
              TreeBuilder
                .makeFunction(
                  "action_sum_count_merge",
                  childrenColumnarFuncNodeList.asJava,
                  resultType)
            case Final =>
              val childrenColumnarFuncNodeList =
                List(inputAttrQueue.dequeue, inputAttrQueue.dequeue).map(attr =>
                  getColumnarFuncNode(attr))
              TreeBuilder.makeFunction(
                "action_avgByCount",
                childrenColumnarFuncNodeList.asJava,
                resultType)
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case _: Sum =>
            mode match {
              case Partial =>
                val childrenColumnarFuncNodeList =
                  aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
                TreeBuilder.makeFunction(
                  "action_sum_partial",
                  childrenColumnarFuncNodeList.asJava, resultType)
              case Final | PartialMerge =>
                val childrenColumnarFuncNodeList =
                  List(inputAttrQueue.dequeue).map(attr => getColumnarFuncNode(attr))
                //FIXME(): decimal adds isEmpty column
                val sum = aggregateFunc.asInstanceOf[Sum]
                val attrBuf = sum.inputAggBufferAttributes
                if (attrBuf.size == 2) {
                  inputAttrQueue.dequeue
                }
                TreeBuilder.makeFunction(
                  "action_sum",
                  childrenColumnarFuncNodeList.asJava, resultType)
              case other =>
                throw new UnsupportedOperationException(s"not currently supported: $other.")
            }
          
        case Count(_) =>
          mode match {
            case Partial =>
              val childrenColumnarFuncNodeList =
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              if (aggregateFunc.children(0).isInstanceOf[Literal]) {
                TreeBuilder.makeFunction(
                  s"action_countLiteral_${aggregateFunc.children(0)}",
                  Lists.newArrayList(),
                  resultType)
              } else {
                TreeBuilder
                  .makeFunction("action_count", childrenColumnarFuncNodeList.asJava, resultType)
              }
            case Final | PartialMerge =>
              val childrenColumnarFuncNodeList =
                List(inputAttrQueue.dequeue).map(attr => getColumnarFuncNode(attr))
              TreeBuilder
                .makeFunction("action_sum", childrenColumnarFuncNodeList.asJava, resultType)
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Max(_) =>
          val childrenColumnarFuncNodeList =
            mode match {
              case Partial =>
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              case Final | PartialMerge =>
                List(inputAttrQueue.dequeue).map(attr => getColumnarFuncNode(attr))
              case other =>
                throw new UnsupportedOperationException(s"not currently supported: $other.")
            }
          val actionName = "action_max" + s"_$NaN_check"
          TreeBuilder.makeFunction(actionName, childrenColumnarFuncNodeList.asJava, resultType)
        case Min(_) =>
          val childrenColumnarFuncNodeList =
            mode match {
              case Partial =>
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              case Final | PartialMerge =>
                List(inputAttrQueue.dequeue).map(attr => getColumnarFuncNode(attr))
              case other =>
                throw new UnsupportedOperationException(s"not currently supported: $other.")
            }
          val actionName = "action_min" + s"_$NaN_check"
          TreeBuilder.makeFunction(actionName, childrenColumnarFuncNodeList.asJava, resultType)
        case StddevSamp(_, _) =>
          mode match {
            case Partial =>
              val childrenColumnarFuncNodeList =
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              TreeBuilder.makeFunction(
                "action_stddev_samp_partial",
                childrenColumnarFuncNodeList.asJava,
                resultType)
            case PartialMerge =>
              throw new UnsupportedOperationException("not currently supported: PartialMerge.")
            case Final =>
              val childrenColumnarFuncNodeList =
                List(inputAttrQueue.dequeue, inputAttrQueue.dequeue, inputAttrQueue.dequeue)
                  .map(attr => getColumnarFuncNode(attr))
              // nullOnDivideByZero controls the result to be null or NaN when count == 1.
              TreeBuilder.makeFunction(
                "action_stddev_samp_final_" +
                  s"${aggregateFunc.asInstanceOf[StddevSamp].nullOnDivideByZero}",
                childrenColumnarFuncNodeList.asJava,
                resultType)
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case First(_, _) =>
          // TODO: it seems
          mode match {
            case Partial =>
              val childrenColumnarFuncNodeList =
                aggregateFunc.children.toList.map(expr => getColumnarFuncNode(expr))
              TreeBuilder
                .makeFunction("action_first_partial", childrenColumnarFuncNodeList.asJava, resultType)
            case PartialMerge =>
              throw new UnsupportedOperationException("PartialMerge is NOT supported" +
                " for First agg func.!")
            case Final =>
              val childrenColumnarFuncNodeList =
                List(inputAttrQueue.dequeue, inputAttrQueue.dequeue).map(attr =>
                  getColumnarFuncNode(attr))
              TreeBuilder.makeFunction(
                "action_first_final",
                childrenColumnarFuncNodeList.asJava,
                resultType)
          }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    } catch {
      case e: Throwable =>
        val errmsg = e.getStackTrace.mkString("\n")
        logError(
          s"getColumnarFuncNode failed, expr is ${aggregateExpression}, forward throw this exception")
        throw e
    }
  }

  def getAttrForAggregateExpr(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributeList: Seq[Attribute]): List[Attribute] = {
    var aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var res_index = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      val mode = exp.mode
      val aggregateFunc = exp.aggregateFunction
      aggregateFunc match {
        case _: Average =>
          mode match {
            case Partial => {
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- 0 until aggBufferAttr.size) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 2
            }
            case PartialMerge => {
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- 0 until aggBufferAttr.size) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 1
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case _: Sum =>
          mode match {
            case Partial | PartialMerge => {
              val sum = aggregateFunc.asInstanceOf[Sum]
              val aggBufferAttr = sum.inputAggBufferAttributes
              if (aggBufferAttr.size == 2) {
                // decimal sum check sum.resultType
                val sum_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
                aggregateAttr += sum_attr
                val isempty_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(1))
                aggregateAttr += isempty_attr
                res_index += 2
              } else {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
                aggregateAttr += attr
                res_index += 1
              }
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Count(_) =>
          mode match {
            case Partial | PartialMerge => {
              val count = aggregateFunc.asInstanceOf[Count]
              val aggBufferAttr = count.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
              aggregateAttr += attr
              res_index += 1
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Max(_) =>
          mode match {
            case Partial | PartialMerge => {
              val max = aggregateFunc.asInstanceOf[Max]
              val aggBufferAttr = max.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
              aggregateAttr += attr
              res_index += 1
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Min(_) =>
          mode match {
            case Partial | PartialMerge => {
              val min = aggregateFunc.asInstanceOf[Min]
              val aggBufferAttr = min.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
              aggregateAttr += attr
              res_index += 1
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case StddevSamp(_,_) =>
          mode match {
            case Partial => {
              val stddevSamp = aggregateFunc.asInstanceOf[StddevSamp]
              val aggBufferAttr = stddevSamp.inputAggBufferAttributes
              for (index <- 0 until aggBufferAttr.size) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 3
            }
            case PartialMerge => {
              throw new UnsupportedOperationException("not currently supported: PartialMerge.")
            }
            case Final => {
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            }
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    }
    aggregateAttr.toList
  }

  def existsAttrNotFound(allAggregateResultAttributes: List[Attribute]): Unit = {
    if (resultExpressions.size == allAggregateResultAttributes.size) {
      var resAllAttr = true
      breakable {
        for (expr <- resultExpressions) {
          if (!expr.isInstanceOf[AttributeReference]) {
            resAllAttr = false
            break
          }
        }
      }
      if (resAllAttr) {
        for (attr <- resultExpressions) {
          if (allAggregateResultAttributes
              .indexOf(attr.asInstanceOf[AttributeReference]) == -1) {
            throw new IllegalArgumentException(
              s"$attr in resultExpressions is not found in allAggregateResultAttributes!")
          }
        }
      }
    }
  }

  def prepareKernelFunction: TreeNode = {
    // build gandiva projection here.
    GazellePluginConfig.getConf

    val mode = if (aggregateExpressions.size > 0) {
      aggregateExpressions(0).mode
    } else {
      null
    }

    val originalInputFieldList = originalInputAttributes.toList.map(attr => {
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })

    //////////////// Project original input to aggregateExpression input //////////////////
    // 1. create grouping native Expression
    val resultField = Field.nullable("res", resultType)
    var i: Int = 0
    val groupingAttributes = groupingExpressions.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).toAttribute
    })
    val groupingNativeFuncNodes =
      groupingExpressions.toList.map(expr => {
        val funcNode = getColumnarFuncNode(expr)
        TreeBuilder
          .makeFunction(
            "action_groupby",
            Lists.newArrayList(funcNode),
            resultType /*this arg won't be used*/ )
      })

    // 2. create aggregate native Expression
    // we need to remove who is in grouping and from partial mode AggregateExpression
    val partialProjectOrdinalList = ListBuffer[Int]()
    aggregateExpressions.zipWithIndex.foreach {
      case (expr, index) =>
        expr.mode match {
          case Partial => {
            val internalExpressionList = expr.aggregateFunction.children
            val ordinalList = ColumnarProjection.binding(
              originalInputAttributes,
              internalExpressionList,
              index,
              skipLiteral = false)
            ordinalList.foreach { i =>
              {
                partialProjectOrdinalList += i
              }
            }
          }
          case _ => {}
        }
    }
    val inputAttrs = originalInputAttributes.zipWithIndex
      .filter {
        case (attr, i) =>
          !groupingAttributes.contains(attr) && !partialProjectOrdinalList.toList.contains(i)
      }
      .map(_._1)
    inputAttrQueue = scala.collection.mutable.Queue(inputAttrs: _*)
    val aggrNativeFuncNodes =
      aggregateExpressions.toList.map(expr => getColumnarFuncNode(expr))

    // 3. map aggregateAttribute to aggregateExpression
    val allAggregateResultAttributes: List[Attribute] =
      groupingAttributes.toList ::: getAttrForAggregateExpr(
        aggregateExpressions,
        aggregateAttributes)

    val aggregateAttributeFieldList =
      allAggregateResultAttributes.map(attr => {
        Field
          .nullable(
            s"${attr.name}#${attr.exprId.id}",
            CodeGeneration.getResultType(attr.dataType))
      })

    val nativeFuncNodes = groupingNativeFuncNodes ::: aggrNativeFuncNodes

    // 4. prepare after aggregate result expressions
    val resultExprFuncNodes = resultExpressions.toList.map(expr => getColumnarFuncNode(expr))

    // 5. create nativeAggregate evaluator
    val nativeSchemaNode = TreeBuilder.makeFunction(
      "inputSchema",
      originalInputFieldList
        .map(field => TreeBuilder.makeField(field))
        .asJava,
      resultType /*dummy ret type, won't be used*/ )
    val aggrActionNode = TreeBuilder.makeFunction(
      "aggregateActions",
      nativeFuncNodes.asJava,
      resultType /*dummy ret type, won't be used*/ )
    val aggregateResultNode = TreeBuilder.makeFunction(
      "resultSchema",
      aggregateAttributeFieldList
        .map(field => TreeBuilder.makeField(field))
        .asJava,
      resultType /*dummy ret type, won't be used*/ )
    val resultExprNode = TreeBuilder.makeFunction(
      "resultExpression",
      resultExprFuncNodes.asJava,
      resultType /*dummy ret type, won't be used*/ )
    val resultProjectNode = ColumnarConditionProjector.prepareKernelFunction(
      null,
      resultExpressions,
      allAggregateResultAttributes)
    TreeBuilder.makeFunction(
      "hashAggregateArrays",
      Lists.newArrayList(nativeSchemaNode, aggrActionNode, aggregateResultNode, resultExprNode),
      resultType /*dummy ret type, won't be used*/ )

  }

}

object ColumnarHashAggregation extends Logging {

  def prepareKernelFunction(
      groupingExpressions: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      resultExpressions: Seq[NamedExpression],
      output: Seq[Attribute],
      sparkConf: SparkConf): TreeNode = {
    val ins = new ColumnarHashAggregation(
      groupingExpressions,
      originalInputAttributes,
      aggregateExpressions,
      aggregateAttributes,
      resultExpressions,
      output,
      sparkConf)
    ins.prepareKernelFunction

  }
}
