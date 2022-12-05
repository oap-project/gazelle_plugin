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
package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import java.net.URLDecoder

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.Lists
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowPartitionReaderFactory.ColumnarBatchRetainer
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowSQLConf._
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.spark.TaskContext

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class ArrowPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: ArrowOptions)
    extends FilePartitionReaderFactory {

  private val batchSize = sqlConf.parquetVectorizedReaderBatchSize
  private val enableFilterPushDown: Boolean = sqlConf.arrowFilterPushDown
  private val caseSensitive: Boolean = sqlConf.caseSensitiveAnalysis

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    // disable row based read
    throw new UnsupportedOperationException
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val path = partitionedFile.filePath
    val factory = ArrowUtils.makeArrowDiscovery(URLDecoder.decode(path, "UTF-8"),
      partitionedFile.start, partitionedFile.length, options)
    val parquetFileFields = factory.inspect().getFields.asScala
    val caseInsensitiveFieldMap = mutable.Map[String, String]()
    // TODO: support array/map/struct types in out-of-order schema reading.
    val requestColNames = readDataSchema.map(_.name)
    val actualReadFields = if (caseSensitive) {
      new Schema(parquetFileFields.filter { field =>
        requestColNames.exists(_.equals(field.getName))
      }.asJava)
    } else {
      readDataSchema.foreach { readField =>
        // TODO: check schema inside of complex type
        val matchedFields =
          parquetFileFields.filter(_.getName.equalsIgnoreCase(readField.name))
        if (matchedFields.size > 1) {
          // Need to fail if there is ambiguity, i.e. more than one field is matched
          val fieldsString = matchedFields.map(_.getName).mkString("[", ", ", "]")
          throw new RuntimeException(
            s"""
               |Found duplicate field(s) "${readField.name}": $fieldsString
               |in case-insensitive mode""".stripMargin.replaceAll("\n", " "))
        }
      }
      new Schema(parquetFileFields.filter { field =>
        requestColNames.exists(_.equalsIgnoreCase(field.getName))
      }.asJava)
    }
    val actualReadFieldNames = actualReadFields.getFields.asScala.map(_.getName).toArray
    val actualReadSchema = if (caseSensitive) {
      new StructType(actualReadFieldNames.map(f => readDataSchema.find(_.name.equals(f)).get))
    } else {
      new StructType(
        actualReadFieldNames.map(f => readDataSchema.find(_.name.equalsIgnoreCase(f)).get))
    }
    val missingSchema =
      new StructType(readDataSchema.filterNot(actualReadSchema.contains).toArray)
    val dataset = factory.finish(actualReadFields)

    val hasMissingColumns = actualReadFields.getFields.size() != readDataSchema.size
    val filter = if (enableFilterPushDown) {
      val filters = if (hasMissingColumns) {
        ArrowFilters.evaluateMissingFieldFilters(pushedFilters, actualReadFieldNames).toArray
      } else {
        pushedFilters
      }
      if (filters == null) {
        null
      } else {
        ArrowFilters.translateFilters(
          ArrowFilters.pruneWithSchema(pushedFilters, readDataSchema),
          caseInsensitiveFieldMap.toMap)
      }
    } else {
      org.apache.arrow.dataset.filter.Filter.EMPTY
    }
    if (filter == null) {
      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = false
        override def get(): ColumnarBatch = null
        override def close(): Unit = {
          // Nothing will be done
        }
      }
    } else {
      val scanOptions = new ScanOptions(actualReadFieldNames, filter, batchSize)
      val scanner = dataset.newScan(scanOptions)

      val taskList = scanner
        .scan()
        .iterator()
        .asScala
        .toList

      val vsrItrList = taskList
        .map(task => task.execute())

      val partitionVectors = ArrowUtils.loadPartitionColumns(
        batchSize, readPartitionSchema, partitionedFile.partitionValues)

      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((_: TaskContext) => {
        partitionVectors.foreach(_.close())
      })

      val nullVectors = if (hasMissingColumns) {
        val vectors =
          ArrowWritableColumnVector.allocateColumns(batchSize, missingSchema)
        vectors.foreach { vector =>
          vector.putNulls(0, batchSize)
          vector.setValueCount(batchSize)
        }

        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((_: TaskContext) => {
          vectors.foreach(_.close())
        })
        vectors
      } else {
        Array.empty[ArrowWritableColumnVector]
      }

      val batchItr = vsrItrList
        .toIterator
        .flatMap(itr => itr.asScala)
        .map(batch => ArrowUtils.loadBatch(
          batch, actualReadSchema, readDataSchema, partitionVectors, nullVectors))

      new PartitionReader[ColumnarBatch] {
        val holder = new ColumnarBatchRetainer()

        override def next(): Boolean = {
          holder.release()
          batchItr.hasNext
        }

        override def get(): ColumnarBatch = {
          val batch = batchItr.next()
          holder.retain(batch)
          batch
        }

        override def close(): Unit = {
          holder.release()
          vsrItrList.foreach(itr => itr.close())
          taskList.foreach(task => task.close())
          scanner.close()
          dataset.close()
          factory.close()
        }
      }
    }
  }
}

object ArrowPartitionReaderFactory {
  private class ColumnarBatchRetainer {
    private var retained: Option[ColumnarBatch] = None

    def retain(batch: ColumnarBatch): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }
}
