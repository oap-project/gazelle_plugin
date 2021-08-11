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

package org.apache.spark.sql.execution.python

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicBoolean

import com.intel.oap.expression._
import com.intel.oap.vectorized._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector.{ValueVector, VectorLoader, VectorSchemaRoot}
import org.apache.spark.{ContextAwareIterator, SparkEnv}
import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector, ArrowColumnVector}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.rdd.RDD

case class ColumnarArrowEvalPythonExec(udfs: Seq[PythonUDF], resultAttrs: Seq[Attribute], child: SparkPlan,
    evalType: Int)
    extends EvalPythonExec {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_arrow_udf"))
  
  buildCheck()

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  override def supportsColumnar = true

  def buildCheck(): Unit = {
    val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (allInputs.exists(_.semanticEquals(e))) {
          allInputs.indexWhere(_.semanticEquals(e))
        } else {
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1
        }
      }.toArray
    }.toArray
    ColumnarProjection.buildCheck(child.output, allInputs.toSeq)
  }
  
  protected def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    throw new NotImplementedError("evaluate Internal row is not supported")
  }

  protected def evaluateColumnar(
    funcs: Seq[ChainedPythonFunctions],
    argOffsets: Array[Array[Int]],
    iter: Iterator[ColumnarBatch],
    schema: StructType,
    context: TaskContext): Iterator[ColumnarBatch] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // Use Coalecse to improve performance in future
    val batchIter = new CloseableColumnBatchIterator(iter)

    val columnarBatchIter = new ColumnarArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.map { batch =>
      val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
      assert(outputTypes == actualDataTypes, "Invalid schema from arrow_udf: " +
        s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
      batch
    }
  }

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputBatches = longMetric("numInputBatches")
    val procTime = longMetric("processTime")

    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, iter)

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray

      // start to work on input data and data type
      val projector = ColumnarProjection.create(child.output, allInputs.toSeq)
      val projected_ordinal_list = projector.getOrdinalList
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toSeq)
      val input_cb_cache = new ArrayBuffer[ColumnarBatch]()

      // original spark will cache input into files using HybridRowQueue
      // we will retain columnar batch in memory firstly
      var start_time: Long = 0
      val projectedColumnarBatchIter = contextAwareIterator.map { input_cb =>
        numInputBatches += input_cb.numRows
        start_time = System.nanoTime()
        // 0. cache input for later merge
        (0 until input_cb.numCols).foreach(i => {
          input_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
        })
        input_cb_cache += input_cb
        // 1. doing projection to input
        val valueVectors = (0 until input_cb.numCols).toList.map(i =>
              input_cb.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector())
        val ret = if (projector.needEvaluate) {
          val projectedInput = projector.evaluate(input_cb.numRows, valueVectors)
          new ColumnarBatch(projectedInput.toArray, input_cb.numRows)
        } else {
          // for no-need project evaluate, do another retain
          (0 until input_cb.numCols).foreach(i => {
            input_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
          })
          new ColumnarBatch(projected_ordinal_list.toArray.map(i => input_cb.column(i)), input_cb.numRows)
        }
        ret
      }.map(batch => {
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(dataTypes == actualDataTypes, "Invalid schema for arrow_udf: " +
          s"expected ${dataTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
        batch
      })
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((tc: TaskContext) => {
        projector.close
        input_cb_cache.foreach(_.close)
      })

      val outputColumnarBatchIterator = evaluateColumnar(
        pyFuncs, argOffsets, projectedColumnarBatchIter, schema, context)

      // val resultProj = ColumnarProjection.create(output, output)

      new CloseableColumnBatchIterator(
        outputColumnarBatchIterator.zipWithIndex.map { case (output_cb, batchId) =>
          val input_cb = input_cb_cache(batchId)
          // retain for input_cb since we are passing it to next operator
          (0 until input_cb.numCols).foreach(i => {
            input_cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain()
          })
          val joinedVectors = (0 until input_cb.numCols).toArray.map(i => input_cb.column(i)) ++ (0 until output_cb.numCols).toArray.map(i => output_cb.column(i))
          val numRows = input_cb.numRows
          numOutputBatches += 1
          numOutputRows += numRows
          procTime += (System.nanoTime() - start_time) / 1000000
          new ColumnarBatch(joinedVectors, numRows)
          /* below is for in case there will be some scala projection in demand
          val valueVectors = joinedVectors.toList.map(_.asInstanceOf[ArrowWritableColumnVector].getValueVector())
          val projectedOutput = resultProj.evaluate(numRows, valueVectors)
          new ColumnarBatch(projectedOutput.toArray.map(_.asInstanceOf[ColumnVector]), numRows)*/
        }
      )
    }
  }

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
