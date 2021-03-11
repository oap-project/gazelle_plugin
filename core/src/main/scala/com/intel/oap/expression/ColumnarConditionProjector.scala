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

import java.util.Collections
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import com.intel.oap.vectorized.ArrowWritableColumnVector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.arrow.memory.ArrowBuf
import scala.collection.JavaConverters._

class ColumnarConditionProjector(
    condPrepareList: (TreeNode, ArrowType),
    conditionFieldList: java.util.List[Field],
    var projPrepareList: Seq[(ExpressionTree, ArrowType)],
    var projectFieldList: java.util.List[Field],
    var skip: Boolean,
    originalInputAttributes: Seq[Attribute],
    numInputBatches: SQLMetric,
    numOutputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    procTime: SQLMetric)
    extends Logging {
  var proc_time: Long = 0
  var elapseTime_make: Long = 0
  val start_make: Long = System.nanoTime()
  var selectionBuffer: ArrowBuf = null
  if (projectFieldList.size == 0 && conditionFieldList.size == 0
      && (projPrepareList == null || projPrepareList.isEmpty)) {
    skip = true
  } else {
    skip = false
  }

  val conditionOrdinalList: List[Int] = conditionFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  if (projectFieldList.size == 0) {
    if (conditionFieldList.size > 0) {
      projectFieldList = originalInputAttributes.zipWithIndex.toList.map {
        case (attr, i) =>
          Field.nullable(s"c_${i}", CodeGeneration.getResultType(attr.dataType))
      }.asJava
    }
  }
  val projectOrdinalList: List[Int] = projectFieldList.asScala.toList.map(field => {
    field.getName.replace("c_", "").toInt
  })

  val projectResultFieldList = if (projPrepareList != null) {
    projPrepareList.map(expr => Field.nullable(s"result", expr._2)).toList.asJava
  } else {
    projPrepareList = projectFieldList.asScala.map(field => {
      (TreeBuilder.makeExpression(TreeBuilder.makeField(field), field), field.getType)
    })
    projectFieldList
  }

  val conditionArrowSchema = new Schema(conditionFieldList)
  val projectionArrowSchema = new Schema(projectFieldList)
  val projectionSchema = ArrowUtils.fromArrowSchema(projectionArrowSchema)
  val resultArrowSchema = new Schema(projectResultFieldList)
  val resultSchema = ArrowUtils.fromArrowSchema(resultArrowSchema)
  if (skip) {
    logWarning(
      s"Will do skip!!!\nconditionArrowSchema is ${conditionArrowSchema}, conditionOrdinalList is ${conditionOrdinalList}, \nprojectionArrowSchema is ${projectionArrowSchema}, projectionOrinalList is ${projectOrdinalList}, \nresult schema is ${resultArrowSchema}")
  }

  val conditioner = if (skip == false && condPrepareList != null) {
    createFilter(conditionArrowSchema, condPrepareList)
  } else {
    null
  }
  val withCond: Boolean = if (conditioner != null) {
    true
  } else {
    false
  }
  val projector = if (skip == false) {
    createProjector(projectionArrowSchema, projPrepareList, withCond)
  } else {
    null
  }

  elapseTime_make = System.nanoTime() - start_make
  logInfo(s"Gandiva make total ${TimeUnit.NANOSECONDS.toMillis(elapseTime_make)} ms.")

  val allocator = ArrowWritableColumnVector.getAllocator

  def createFilter(arrowSchema: Schema, prepareList: (TreeNode, ArrowType)): Filter =
    synchronized {
      if (conditioner != null) {
        return conditioner
      }
      Filter.make(arrowSchema, TreeBuilder.makeCondition(prepareList._1))
    }

  def createProjector(
      arrowSchema: Schema,
      prepareList: Seq[(ExpressionTree, ArrowType)],
      withCond: Boolean): Projector = synchronized {
    if (projector != null) {
      return projector
    }
    val fieldNodesList = prepareList.map(_._1).toList.asJava
    try {
      if (withCond) {
        Projector.make(arrowSchema, fieldNodesList, SelectionVectorType.SV_INT16)
      } else {
        Projector.make(arrowSchema, fieldNodesList)
      }
    } catch {
      case e =>
        logError(
          s"\noriginalInputAttributes is ${originalInputAttributes} ${originalInputAttributes.map(
            _.dataType)}, \narrowSchema is ${arrowSchema}, \nProjection is ${prepareList.map(_._1.toProtobuf)}")
        throw e
    }
  }

  def createStructType(arrowSchema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(arrowSchema)
  }

  def close(): Unit = {
    if (selectionBuffer != null) {
      selectionBuffer.close()
      selectionBuffer = null
    }
    if (conditioner != null) {
      conditioner.close()
    }
    if (projector != null) {
      projector.close()
    }
    procTime.set(proc_time)
  }

  def createIterator(cbIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      private var columnarBatch: ColumnarBatch = null
      private var resColumnarBatch: ColumnarBatch = null
      private var nextCalled = false

      override def hasNext: Boolean = {
        if (!nextCalled && resColumnarBatch != null) {
          return true
        }
        nextCalled = false
        var beforeEval: Long = 0
        var afterEval: Long = 0
        var numRows = 0
        var input: ArrowRecordBatch = null
        var selectionVector: SelectionVectorInt16 = null
        while (numRows == 0) {

          if (cbIterator.hasNext) {
            columnarBatch = cbIterator.next()
            numInputBatches += 1
          } else {
            resColumnarBatch = null
            logInfo(s"has no next, return false")
            return false
          }
          beforeEval = System.nanoTime()
          numRows = columnarBatch.numRows()
          if (numRows > 0) {
            if (skip == true) {
              resColumnarBatch = if (projectOrdinalList.size < columnarBatch.numCols) {
                (0 until columnarBatch.numCols).toList.foreach(i =>
                  columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
                // Since all these cols share same root, we need to retain them all or retained vector may be closed.
                val cols = projectOrdinalList
                  .map(i => {
                    columnarBatch.column(i).asInstanceOf[ColumnVector]
                  })
                  .toArray
                new ColumnarBatch(cols, numRows)
              } else {
                logInfo("Use original ColumnarBatch")
                (0 until columnarBatch.numCols).toList.foreach(i =>
                  columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
                columnarBatch
              }
              return true
            }
            if (conditioner != null) {
              // do conditioner here
              numRows = columnarBatch.numRows
              if (selectionBuffer != null) {
                selectionBuffer.close()
                selectionBuffer = null
              }
              selectionBuffer = allocator.buffer(numRows * 2)
              selectionVector = new SelectionVectorInt16(selectionBuffer)
              val cols = conditionOrdinalList.map(i => {
                columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
              })
              afterEval = System.nanoTime()
              proc_time += ((System.nanoTime() - beforeEval) / (1000 * 1000))
              input = ConverterUtils.createArrowRecordBatch(numRows, cols)
              conditioner.evaluate(input, selectionVector)
              ConverterUtils.releaseArrowRecordBatch(input)
              numRows = selectionVector.getRecordCount()
              if (projPrepareList == null && numRows == columnarBatch.numRows()) {
                logInfo(
                  "No projection and conditioned row number is as same as original row number. Directly use original ColumnarBatch")
                resColumnarBatch = columnarBatch
                (0 until resColumnarBatch.numCols).toList.foreach(i =>
                  resColumnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
                return true
              }
            }
          }
          if (numRows == 0) {
            logInfo(s"Got empty ColumnarBatch from child or after filter")
          }
        }

        // for now, we either filter one columnarBatch who has valid rows or we only need to do project
        // either scenario we will need to output one columnarBatch.
        beforeEval = System.nanoTime()
        val resultColumnVectors =
          ArrowWritableColumnVector.allocateColumns(numRows, resultSchema).toArray
        val outputVectors = resultColumnVectors
          .map(columnVector => {
            columnVector.getValueVector()
          })
          .toList
          .asJava

        val cols = projectOrdinalList.map(i => {
          columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
        })
        input = ConverterUtils.createArrowRecordBatch(columnarBatch.numRows, cols)
        if (conditioner != null) {
          projector.evaluate(input, selectionVector, outputVectors);
        } else {
          projector.evaluate(input, outputVectors);
        }

        ConverterUtils.releaseArrowRecordBatch(input)
        val outputBatch =
          new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), numRows)
        proc_time += ((System.nanoTime() - beforeEval) / (1000 * 1000))
        resColumnarBatch = outputBatch
        true
      }

      override def next(): ColumnarBatch = {
        nextCalled = true
        if (resColumnarBatch == null) {
          throw new UnsupportedOperationException(
            "Iterator has no next columnar batch or it hasn't been called by hasNext.")
        }
        numOutputBatches += 1
        numOutputRows += resColumnarBatch.numRows
        val numCols = resColumnarBatch.numCols
        //logInfo(s"result has ${resColumnarBatch.numRows}, first row is ${(0 until numCols).map(resColumnarBatch.column(_).getUTF8String(0))}")
        resColumnarBatch
      }

    } // end of Iterator
  } // end of createIterator

} // end of class

object ColumnarConditionProjector extends Logging {
  def init(
      condExpr: Expression,
      projectList: Seq[Expression],
      originalInputAttributes: Seq[Attribute],
      do_init: Boolean = true): (
      (TreeNode, ArrowType),
      java.util.List[Field],
      Seq[(ExpressionTree, ArrowType)],
      java.util.List[Field],
      Boolean) = {
    logInfo(
      s"originalInputAttributes is ${originalInputAttributes}, \nCondition is ${condExpr}, \nProjection is ${projectList}")
    val conditionInputList: java.util.List[Field] = Lists.newArrayList()
    val (condPrepareList, skip_filter) = if (condExpr != null) {
      val columnarCondExpr: Expression = ColumnarExpressionConverter
        .replaceWithColumnarExpression(condExpr, originalInputAttributes)
      if (do_init == false) {
        (null, true)
      } else {
        val (cond, resultType) =
          columnarCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionInputList)
        ((cond, resultType), false)
      }
    } else {
      (null, true)
    }
    //Collections.sort(conditionFieldList, (l: Field, r: Field) => { l.getName.compareTo(r.getName)})
    val conditionFieldList = conditionInputList.asScala.toList.distinct.asJava;

    var projectInputList: java.util.List[Field] = Lists.newArrayList()
    val (projPrepareList, skip_project): (Seq[(ExpressionTree, ArrowType)], Boolean) =
      if (projectList != null && projectList.size != 0) {
        val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
          ColumnarExpressionConverter.replaceWithColumnarExpression(expr, originalInputAttributes)
        })
        if (do_init == false) {
          (null, true)
        } else {
          var should_skip = true
          (columnarProjExprs.map(columnarExpr => {
            val (node, resultType) =
              columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(projectInputList)
            val result = Field.nullable("result", resultType)
            if (s"${node.toProtobuf}".contains("functionNode"))
              should_skip = false
            logDebug(
              s"gandiva node is ${node.toProtobuf}, result is ${result}, should_skip is ${should_skip}")
            (TreeBuilder.makeExpression(node, result), resultType)
          }), should_skip)
        }
      } else {
        (null, true)
      }
    val projectFieldList = projectInputList.asScala.toList.distinct.asJava
    (
      condPrepareList,
      conditionFieldList,
      projPrepareList,
      projectFieldList,
      skip_filter && skip_project)
  }

  def prepareKernelFunction(
      condExpr: Expression,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute]): (TreeNode, TreeNode) = {
    val conditionInputList: java.util.List[Field] = Lists.newArrayList()
    var projectInputList: java.util.List[Field] = Lists.newArrayList()
    val inputNodeList: List[TreeNode] = originalInputAttributes.toList.map(attr => {
      val field = Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
      TreeBuilder.makeField(field)
    })
    val input_node = TreeBuilder.makeFunction(
      "codegen_input_schema",
      inputNodeList.asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )

    val conditionNode = if (condExpr != null) {
      val columnarCondExpr: Expression = ColumnarExpressionConverter
        .replaceWithColumnarExpression(condExpr)
      val (thisNode, resultType) =
        columnarCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(conditionInputList)
      TreeBuilder.makeFunction(
        "filter",
        Lists.newArrayList(input_node, thisNode),
        new ArrowType.Int(32, true))
    } else {
      null
    }

    val projectionNode = if (projectList != null && projectList.size != 0) {
      val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
      })

      val thisNodeList = columnarProjExprs.toList.map(columnarExpr => {
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(projectInputList)._1
      })
      val project_node = TreeBuilder.makeFunction(
        "codegen_project",
        thisNodeList.asJava,
        new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
      TreeBuilder.makeFunction(
        "project",
        Lists.newArrayList(input_node, project_node),
        new ArrowType.Int(32, true))
    } else {
      null
    }

    (conditionNode, projectionNode)

  }

  def prebuild(
      condition: Expression,
      projectList: Seq[NamedExpression],
      inputSchema: Seq[Attribute]): Unit = {
    init(condition, projectList, inputSchema, false)
  }

  def create(
      condition: Expression,
      projectList: Seq[NamedExpression],
      inputSchema: Seq[Attribute],
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      numOutputRows: SQLMetric,
      procTime: SQLMetric): ColumnarConditionProjector = synchronized {
    val (condPrepareList, conditionFieldList, projPrepareList, projectFieldList, skip) =
      init(condition, projectList, inputSchema)
    new ColumnarConditionProjector(
      condPrepareList,
      conditionFieldList,
      projPrepareList,
      projectFieldList,
      skip,
      inputSchema,
      numInputBatches,
      numOutputBatches,
      numOutputRows,
      procTime)
  }
}
