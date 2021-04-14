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

import org.apache.commons.lang3.StringUtils

import com.intel.oap.expression._
import com.intel.oap.vectorized.ArrowWritableColumnVector
import com.intel.oap.vectorized.CloseableColumnBatchIterator
import org.apache.arrow.memory.ArrowBuf
import org.apache.spark.TaskContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.columnar.{
  CachedBatch,
  CachedBatchSerializer,
  SimpleMetricsCachedBatch,
  SimpleMetricsCachedBatchSerializer
}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.{WritableColumnVector}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{LongAccumulator, Utils}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class ArrowCachedBatch(numRows: Int, buffer: Array[Byte], stats: InternalRow = null)
    extends SimpleMetricsCachedBatch {
  override def sizeInBytes: Long = buffer.size
}

/**
 * The default implementation of CachedBatchSerializer.
 */
class ArrowColumnarCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] =
    throw new IllegalStateException("InternalRow input is not supported")

  def convertForCacheInternal(
      input: RDD[ColumnarBatch],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    input.mapPartitions { iter =>
      var processed = false
      new Iterator[ArrowCachedBatch] {
        def next(): ArrowCachedBatch = {
          processed = true
          var _numRows: Int = 0
          val _input = new ArrayBuffer[ColumnarBatch]()
          while (iter.hasNext) {
            val batch = iter.next
            if (batch.numRows > 0) {
              (0 until batch.numCols).foreach(i =>
                batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              _numRows += batch.numRows
              _input += batch
            }
          }
          val cacheBuffer = ConverterUtils.convertToNetty(_input.toArray)
          _input.foreach(_.close)

          ArrowCachedBatch(_numRows, cacheBuffer)
        }

        def hasNext: Boolean = !processed
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val columnIndices =
      selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray
    def createAndDecompressColumn(cachedIter: Iterator[CachedBatch]): Iterator[ColumnarBatch] = {
      val res = new Iterator[ColumnarBatch] {
        var iter: Iterator[ColumnarBatch] = null
        if (cachedIter.hasNext) {
          val cachedColumnarBatch: ArrowCachedBatch =
            cachedIter.next.asInstanceOf[ArrowCachedBatch]
          val rowCount = cachedColumnarBatch.numRows
          val rawData = cachedColumnarBatch.buffer

          iter = ConverterUtils.convertFromNetty(cacheAttributes, Array(rawData), columnIndices)
        }
        def next(): ColumnarBatch =
          if (iter != null) {
            iter.next
          } else {
            val resultStructType = StructType(selectedAttributes.map(a =>
              StructField(a.name, a.dataType, a.nullable, a.metadata)))
            val resultColumnVectors =
              ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
            new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
          }
        def hasNext: Boolean = iter.hasNext
      }
      new CloseableColumnBatchIterator(res)
    }
    input.mapPartitions(createAndDecompressColumn)
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    // Find the ordinals and data types of the requested columns.
    val columnarBatchRdd =
      convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    columnarBatchRdd.mapPartitions { batches =>
      val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)
      batches.flatMap { batch => batch.rowIterator().asScala.map(toUnsafe) }
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = true

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(classOf[ArrowWritableColumnVector].getName))

}
