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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.util.ExecutorManager
import org.apache.spark.sql.util.StructTypeFWD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import com.google.common.collect.Lists
import com.intel.oap.GazellePluginConfig

import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkVectorUtils;

case class ColumnarConditionProjectExec(
    condition: Expression,
    projectList: Seq[NamedExpression],
    child: SparkPlan)
    extends UnaryExecNode
    with ColumnarCodegenSupport
    with PredicateHelper
    with AliasAwareOutputPartitioning
    with Logging {

  val numaBindingInfo = GazellePluginConfig.getConf.numaBindingInfo

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar = true

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_condproject"))

  buildCheck(condition, projectList, child.output)

  def buildCheck(condExpr: Expression, projectList: Seq[Expression],
                 originalInputAttributes: Seq[Attribute]): Unit = {
    // check datatype
    originalInputAttributes.toList.foreach(attr => {
      try {
        ConverterUtils.checkIfTypeSupportedInProjection(attr.dataType)
      } catch {
        case e : UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarConditionProjector.")
      }
    })
    // check expr
    if (condExpr != null) {
      try {
        ConverterUtils.checkIfTypeSupportedInProjection(condExpr.dataType)
      } catch {
        case e : UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${condExpr.dataType} is not supported in ColumnarConditionProjector.")
      }
      ColumnarExpressionConverter.replaceWithColumnarExpression(condExpr)
    }
    if (projectList != null) {
      for (expr <- projectList) {
        try {
          ConverterUtils.checkIfTypeSupportedInProjection(expr.dataType)
        } catch {
          case e : UnsupportedOperationException =>
            throw new UnsupportedOperationException(
              s"${expr.dataType} is not supported in ColumnarConditionProjector.")
        }
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
      }
    }
  }

  // In spark 3.2, PredicateHelper has already introduced isNullIntolerant with completely same
  // code. If we use the same method name, override keyword is required. But in spark3.1, no
  // method is overridden. So we use an independent method name.
  def isNullIntolerantInternal(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerantInternal)
    case _ => false
  }

  override protected def outputExpressions: Seq[NamedExpression] =
    if (projectList != null) projectList else output

  val notNullAttributes = if (condition != null) {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => isNullIntolerantInternal(a) && a.references.subsetOf(child.outputSet)
      case _ => false
    }
    notNullPreds.flatMap(_.references).distinct.map(_.exprId)
  } else {
    null
  }
  override def output: Seq[Attribute] =
    if (projectList != null) {
      projectList.map(_.toAttribute)
    } else if (condition != null) {
      val res = child.output.map { a =>
        if (a.nullable && notNullAttributes.contains(a.exprId)) {
          a.withNullability(false)
        } else {
          a
        }
      }
      res
    } else {
      val res = child.output.map { a => a }
      res
    }

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

  override def supportColumnarCodegen: Boolean = {
    if (condition != null) {
      val colCondExpr = ColumnarExpressionConverter.replaceWithColumnarExpression(condition)
      // support codegen if cond expression and proj expression both supports codegen
      if (!colCondExpr.asInstanceOf[ColumnarExpression].supportColumnarCodegen(Lists.newArrayList())) {
        return false
      }
    }
    if (projectList != null) {
      for (expr <- projectList) {
        val colExpr = ColumnarExpressionConverter.replaceWithColumnarExpression(expr)
        if (!colExpr.asInstanceOf[ColumnarExpression].supportColumnarCodegen(Lists.newArrayList())) {
          return false
        }
      }
    }
    true
  }

  def getKernelFunction(childTreeNode: TreeNode): TreeNode = {
    val (filterNode, projectNode) =
      ColumnarConditionProjector.prepareKernelFunction(condition, projectList, child.output)
    if (filterNode != null && projectNode != null) {
      val nestedFilterNode = if (childTreeNode != null) {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(filterNode, childTreeNode),
          new ArrowType.Int(32, true))
      } else {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(filterNode),
          new ArrowType.Int(32, true))
      }
      TreeBuilder.makeFunction(
        s"child",
        Lists.newArrayList(projectNode, nestedFilterNode),
        new ArrowType.Int(32, true))
    } else if (filterNode != null) {
      if (childTreeNode != null) {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(filterNode, childTreeNode),
          new ArrowType.Int(32, true))
      } else {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(filterNode),
          new ArrowType.Int(32, true))
      }
    } else if (projectNode != null) {
      if (childTreeNode != null) {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(projectNode, childTreeNode),
          new ArrowType.Int(32, true))
      } else {
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(projectNode),
          new ArrowType.Int(32, true))
      }
    } else {
      null
    }
  }

  override def doCodeGen: ColumnarCodegenContext = {
    val (childCtx, kernelFunction) = child match {
      case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
        val ctx = c.doCodeGen
        (ctx, getKernelFunction(ctx.root))
      case _ =>
        (null, getKernelFunction(null))
    }
    if (kernelFunction == null) {
      return childCtx
    }
    val inputSchema = if (childCtx != null) { childCtx.inputSchema }
    else { ConverterUtils.toArrowSchema(child.output) }
    val outputSchema = ConverterUtils.toArrowSchema(output)
    ColumnarCodegenContext(inputSchema, outputSchema, kernelFunction)
  }

  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  ColumnarConditionProjector.prebuild(condition, projectList, child.output)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputBatches = longMetric("numInputBatches")
    val procTime = longMetric("processTime")
    numOutputRows.set(0)
    numOutputBatches.set(0)
    numInputBatches.set(0)

    child.executeColumnar().mapPartitions { iter =>
      GazellePluginConfig.getConf
      ExecutorManager.tryTaskSet(numaBindingInfo)
      val condProj = ColumnarConditionProjector.create(
        condition,
        projectList,
        child.output,
        numInputBatches,
        numOutputBatches,
        numOutputRows,
        procTime)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((tc: TaskContext) => {
        condProj.close()
      })
      new CloseableColumnBatchIterator(condProj.createIterator(iter))
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): ColumnarConditionProjectExec =
    copy(child = newChild)
}

case class ColumnarUnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  // updating nullability to make all the children consistent

  buildCheck()

  def buildCheck(): Unit = {
    for (child <- children) {
      for (schema <- child.schema) {
        try {
          ConverterUtils.checkIfTypeSupported(schema.dataType)
        } catch {
          case e: UnsupportedOperationException =>
            throw new UnsupportedOperationException(
              s"${schema.dataType} is not supported in ColumnarUnionExec")
        }
      }
    }
  }
  override def supportsColumnar = true
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId,
          firstAttr.qualifier)
      }
    }
  }
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  // For spark 3.2.
  protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): ColumnarUnionExec =
    copy(children = newChildren)
}

//TODO(): consolidate locallimit and globallimit
case class ColumnarLocalLimitExec(limit: Int, child: SparkPlan) extends LimitExec {
  // updating nullability to make all the children consistent

  buildCheck()

  def buildCheck(): Unit = {
    for (child <- children) {
      for (schema <- child.schema) {
        try {
          ConverterUtils.checkIfTypeSupported(schema.dataType)
        } catch {
          case e: UnsupportedOperationException =>
            throw new UnsupportedOperationException(
              s"${schema.dataType} is not supported in ColumnarLocalLimitExec")
        }
      }
    }
  }

  
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar = true
  override def output: Seq[Attribute] = child.output
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
    val hasInput = iter.hasNext
      val res = if (hasInput) {
        new Iterator[ColumnarBatch] {
          var rowCount = 0
          override def hasNext: Boolean = {
            val hasNext = iter.hasNext
            hasNext && (rowCount < limit)
          }

          override def next(): ColumnarBatch = {

            if (!hasNext) {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }

            if (rowCount < limit) {
              val delta = iter.next()
              val preRowCount = rowCount
              rowCount += delta.numRows
              if (rowCount > limit) {
                val newSize = limit - preRowCount
                SparkVectorUtils.setNumRows(delta, newSize)
              }
              delta
            } else {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }
          }
        }
      } else {
        Iterator.empty
      }
      new CloseableColumnBatchIterator(res)
    }
  }
  
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  protected def withNewChildInternal(newChild: SparkPlan):
  ColumnarLocalLimitExec =
    copy(child = newChild)

}

case class ColumnarGlobalLimitExec(limit: Int, child: SparkPlan) extends LimitExec {
  // updating nullability to make all the children consistent

  buildCheck()

  def buildCheck(): Unit = {
    for (child <- children) {
      for (schema <- child.schema) {
        try {
          ConverterUtils.checkIfTypeSupported(schema.dataType)
        } catch {
          case e: UnsupportedOperationException =>
            throw new UnsupportedOperationException(
              s"${schema.dataType} is not supported in ColumnarGlobalLimitExec")
        }
      }
    }
  }

  
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def supportsColumnar = true
  override def output: Seq[Attribute] = child.output
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val hasInput = iter.hasNext
      val res = if (hasInput) {
        new Iterator[ColumnarBatch] {
          var rowCount = 0
          override def hasNext: Boolean = {
            val hasNext = iter.hasNext
            hasNext && (rowCount < limit)
          }

          override def next(): ColumnarBatch = {

            if (!hasNext) {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }

            if (rowCount < limit) {
              val delta = iter.next()
              val preRowCount = rowCount
              rowCount += delta.numRows
              if (rowCount > limit) {
                val newSize = limit - preRowCount
                SparkVectorUtils.setNumRows(delta, newSize)
              }
              delta
            } else {
              throw new NoSuchElementException("End of ColumnarBatch iterator")
            }
          }
        }
      } else {
        Iterator.empty
      }
      new CloseableColumnBatchIterator(res)
    }
  }
  
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  protected def withNewChildInternal(newChild: SparkPlan):
  ColumnarGlobalLimitExec =
    copy(child = newChild)
}