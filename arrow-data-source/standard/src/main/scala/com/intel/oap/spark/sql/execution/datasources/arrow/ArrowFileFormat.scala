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

package com.intel.oap.spark.sql.execution.datasources.arrow

import java.net.URLDecoder

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.intel.oap.spark.sql.ArrowWriteExtension.FakeRow
import com.intel.oap.spark.sql.ArrowWriteQueue
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.{ArrowFilters, ArrowOptions, ArrowUtils}
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowSQLConf._
import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.codec.CodecConfig

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkVectorUtils}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils.UnsafeItr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ArrowFileFormat extends FileFormat with DataSourceRegister with Serializable {


  override def isSplitable(sparkSession: SparkSession,
      options: Map[String, String], path: Path): Boolean = {
    ArrowUtils.isOriginalFormatSplitable(
      new ArrowOptions(new CaseInsensitiveStringMap(options.asJava).asScala.toMap))
  }

  def convert(files: Seq[FileStatus], options: Map[String, String]): Option[StructType] = {
    ArrowUtils.readSchema(files, new CaseInsensitiveStringMap(options.asJava))
  }

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val arrowOptions = new ArrowOptions(new CaseInsensitiveStringMap(options.asJava).asScala.toMap)
    ArrowUtils.getFormat(arrowOptions) match {
      case _: org.apache.arrow.dataset.file.format.ParquetFileFormat =>
        ParquetUtils.inferSchema(sparkSession, options, files)
      case _ =>
        convert(files, options)
    }
  }

  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val arrowOptions = new ArrowOptions(new CaseInsensitiveStringMap(options.asJava).asScala.toMap)
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        ArrowUtils.getFormat(arrowOptions) match {
          case _: org.apache.arrow.dataset.file.format.ParquetFileFormat =>
            CodecConfig.from(context).getCodec.getExtension + ".parquet"
          case f => throw new IllegalArgumentException("Unimplemented file type to write: " + f)
        }
      }

      override def newInstance(path: String, dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val originPath = path
        val writeQueue = new ArrowWriteQueue(ArrowUtils.toArrowSchema(dataSchema),
          ArrowUtils.getFormat(arrowOptions), originPath)

        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            val batch = row.asInstanceOf[FakeRow].batch
            writeQueue.enqueue(SparkVectorUtils
              .toArrowRecordBatch(batch))
          }

          override def close(): Unit = {
            writeQueue.close()
          }

          // Do NOT add override keyword for compatibility on spark 3.1.
          def path(): String = {
            originPath
          }
        }
      }
    }
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf;
    val batchSize = sqlConf.parquetVectorizedReaderBatchSize
    val enableFilterPushDown = sqlConf.arrowFilterPushDown
    val caseSensitive = sqlConf.caseSensitiveAnalysis

    (file: PartitionedFile) => {
      val factory = ArrowUtils.makeArrowDiscovery(
        URLDecoder.decode(file.filePath, "UTF-8"), file.start, file.length,
        new ArrowOptions(
          new CaseInsensitiveStringMap(
            options.asJava).asScala.toMap))

      // todo predicate validation / pushdown
      val parquetFileFields = factory.inspect().getFields.asScala
      val caseInsensitiveFieldMap = mutable.Map[String, String]()
      // TODO: support array/map/struct types in out-of-order schema reading.
      val requestColNames = requiredSchema.map(_.name)
      val actualReadFields = if (caseSensitive) {
        new Schema(parquetFileFields.filter { field =>
          requestColNames.exists(_.equals(field.getName))
        }.asJava)
      } else {
        requiredSchema.foreach { readField =>
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
        new StructType(actualReadFieldNames.map(f => requiredSchema.find(_.name.equals(f)).get))
      } else {
        new StructType(
          actualReadFieldNames.map(f => requiredSchema.find(_.name.equalsIgnoreCase(f)).get))
      }
      val missingSchema =
        new StructType(requiredSchema.filterNot(actualReadSchema.contains).toArray)
      val dataset = factory.finish(actualReadFields)

      val hasMissingColumns = actualReadFields.getFields.size() != requiredSchema.size
      val filter = if (enableFilterPushDown) {
        val pushedFilters = if (hasMissingColumns) {
          ArrowFilters.evaluateMissingFieldFilters(filters, actualReadFieldNames)
        } else {
          filters
        }
        if (pushedFilters == null) {
          null
        } else {
          ArrowFilters.translateFilters(
            pushedFilters, caseInsensitiveFieldMap.toMap)
        }
      } else {
        org.apache.arrow.dataset.filter.Filter.EMPTY
      }

      if (filter == null) {
        new Iterator[InternalRow] {
          override def hasNext: Boolean = false
          override def next(): InternalRow = null
        }
      } else {
        val scanOptions = new ScanOptions(
          actualReadFieldNames,
          filter,
          batchSize)
        val scanner = dataset.newScan(scanOptions)

        val taskList = scanner
          .scan()
          .iterator()
          .asScala
          .toList
        val itrList = taskList
          .map(task => task.execute())

        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => {
          itrList.foreach(_.close())
          taskList.foreach(_.close())
          scanner.close()
          dataset.close()
          factory.close()
        }))

        val partitionVectors =
          ArrowUtils.loadPartitionColumns(batchSize, partitionSchema, file.partitionValues)

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

        val itr = itrList
          .toIterator
          .flatMap(itr => itr.asScala)
          .map(batch => ArrowUtils.loadBatch(
            batch, actualReadSchema, requiredSchema, partitionVectors, nullVectors))
        new UnsafeItr(itr).asInstanceOf[Iterator[InternalRow]]
      }
    }
  }

  override def vectorTypes(requiredSchema: StructType, partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)(
      classOf[ArrowWritableColumnVector].getName
    ))
  }

  override def shortName(): String = "arrow"
}

object ArrowFileFormat {
}
