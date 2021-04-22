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

import java.io._
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
import sun.misc.Cleaner

private class Deallocator(var arrowColumnarBatch: Array[ColumnarBatch]) extends Runnable {

  override def run(): Unit = {
    try {
      Option(arrowColumnarBatch).foreach(_.foreach(_.close))
    } catch {
      case e: Exception =>
        // We should suppress all possible errors in Cleaner to prevent JVM from being shut down
        System.err.println("ColumnarHashedRelation: Error running deaallocator")
        e.printStackTrace()
    }
  }
}

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class ArrowCachedBatch(
    var numRows: Int,
    var buffer: Array[ColumnarBatch],
    stats: InternalRow)
    extends SimpleMetricsCachedBatch
    with Externalizable {
  Cleaner.create(this, new Deallocator(buffer))
  def this() = {
    this(0, null, null)
  }
  override def sizeInBytes: Long = buffer.size
  override def writeExternal(out: ObjectOutput): Unit = {
    // System.out.println(s"writeExternal for $this")
    val rawArrowData = ConverterUtils.convertToNetty(buffer)
    out.writeObject(rawArrowData)
    buffer.foreach(_.close)
  }

  override def readExternal(in: ObjectInput): Unit = {
    numRows = 0
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    buffer = ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    // System.out.println(s"readExternal for $this")
  }
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
          // To avoid mem copy, we only save columnVector reference here
          val res = ArrowCachedBatch(_numRows, _input.toArray, null)
          // System.out.println(s"convertForCacheInternal cachedBatch is ${res}")
          res
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
          // System.out.println(
          //   s"convertCachedBatchToColumnarBatch cachedBatch is ${cachedColumnarBatch}")
          val rawData = cachedColumnarBatch.buffer

          iter = new Iterator[ColumnarBatch] {
            val numBatches = rawData.size
            var batchIdx = 0
            override def hasNext: Boolean = batchIdx < numBatches
            override def next(): ColumnarBatch = {
              val vectors = columnIndices.map(i => rawData(batchIdx).column(i))
              vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].retain())
              val numRows = rawData(batchIdx).numRows
              batchIdx += 1
              new ColumnarBatch(vectors, numRows)
            }
          }
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
