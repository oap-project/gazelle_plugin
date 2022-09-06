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

import com.intel.oap.sql.shims.SparkShimLoader
import java.util.Locale

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.arrow.ScanUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ArrayBuffer

case class ArrowScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
    extends FileScan {

  // Use the default value for org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD.
  val IO_WARNING_LARGEFILETHRESHOLD: Long = 1024 * 1024 * 1024
  var openCostInBytesFinal = sparkSession.sessionState.conf.filesOpenCostInBytes

  override def isSplitable(path: Path): Boolean = {
    ArrowUtils.isOriginalFormatSplitable(
      new ArrowOptions(new CaseInsensitiveStringMap(options).asScala.toMap))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap().asScala.toMap
    val hconf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hconf))
    ArrowPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      new ArrowOptions(options.asScala.toMap))
  }

  override def withFilters(partitionFilters: Seq[Expression],
                           dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  // compute maxSplitBytes
//  def maxSplitBytes(sparkSession: SparkSession,
//                    selectedPartitions: Seq[PartitionDirectory]): Long = {
//    // TODO: unify it with PREFERRED_PARTITION_SIZE_UPPER_BOUND.
//    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
//    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
//    // val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
//    //  .getOrElse(sparkSession.leafNodeDefaultParallelism)
//    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
//      .getOrElse(SparkShimLoader.getSparkShims.leafNodeDefaultParallelism(sparkSession))
//    val PREFERRED_PARTITION_SIZE_LOWER_BOUND: Long = 256 * 1024 * 1024
//    val PREFERRED_PARTITION_SIZE_UPPER_BOUND: Long = 1024 * 1024 * 1024
//    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
//    var maxBytesPerCore = totalBytes / minPartitionNum
//    var bytesPerCoreFinal = maxBytesPerCore
//    var bytesPerCore = maxBytesPerCore
//
//    if (bytesPerCore > PREFERRED_PARTITION_SIZE_UPPER_BOUND) {
//      // Adjust partition size.
//      var i = 2
//      while (bytesPerCore > PREFERRED_PARTITION_SIZE_UPPER_BOUND) {
//        bytesPerCore = maxBytesPerCore / i
//        if (bytesPerCore > PREFERRED_PARTITION_SIZE_LOWER_BOUND) {
//          bytesPerCoreFinal = bytesPerCore
//        }
//        i = i + 1
//      }
//      Math.min(PREFERRED_PARTITION_SIZE_UPPER_BOUND, bytesPerCoreFinal)
//      // Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
//    } else {
//      // adjust open cost.
//      var i = 2
//      while (bytesPerCore < PREFERRED_PARTITION_SIZE_LOWER_BOUND) {
//        val dynamicOpenCostInBytes = openCostInBytesFinal * i
//        val totalBytes =
//          selectedPartitions.flatMap(_.files.map(_.getLen + dynamicOpenCostInBytes)).sum
//        maxBytesPerCore = totalBytes / minPartitionNum
//        if (maxBytesPerCore < PREFERRED_PARTITION_SIZE_UPPER_BOUND) {
//          openCostInBytesFinal = dynamicOpenCostInBytes
//          bytesPerCoreFinal = maxBytesPerCore
//        }
//        i = i + 1
//      }
//      Math.max(PREFERRED_PARTITION_SIZE_LOWER_BOUND, bytesPerCoreFinal)
//    }
//  }

  // This implementation is ported from spark FilePartition.scala with changes for
  // adjusting openCost.
  def getFilePartitions(sparkSession: SparkSession,
                        partitionedFiles: Seq[PartitionedFile],
                        maxSplitBytes: Long): Seq[FilePartition] = {
    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }

  // This implementation is ported from spark FileScan with only changes for computing
  // maxSplitBytes.
//  override def partitions: Seq[FilePartition] = {
//    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
//    // val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
//    val maxSplitBytes = this.maxSplitBytes(sparkSession, selectedPartitions)
//    // val partitionAttributes = fileIndex.partitionSchema.toAttributes
//    val partitionAttributes = ScanUtils.toAttributes(fileIndex)
//    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
//    val readPartitionAttributes = readPartitionSchema.map { readField =>
//      attributeMap.get(normalizeName(readField.name)).getOrElse {
//        // throw QueryCompilationErrors.cannotFindPartitionColumnInPartitionSchemaError(
//        //  readField, fileIndex.partitionSchema)
//        throw new RuntimeException(s"Can't find required partition column ${readField.name} " +
//          s"in partition schema ${fileIndex.partitionSchema}")
//      }
//    }
//    lazy val partitionValueProject =
//      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
//    val splitFiles = selectedPartitions.flatMap { partition =>
//      // Prune partition values if part of the partition columns are not required.
//      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
//        partitionValueProject(partition.values).copy()
//      } else {
//        partition.values
//      }
//      partition.files.flatMap { file =>
//        val filePath = file.getPath
//        PartitionedFileUtil.splitFiles(
//          sparkSession = sparkSession,
//          file = file,
//          filePath = filePath,
//          isSplitable = isSplitable(filePath),
//          maxSplitBytes = maxSplitBytes,
//          partitionValues = partitionValues
//        )
//      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
//    }
//
//    if (splitFiles.length == 1) {
//      val path = new Path(splitFiles(0).filePath)
//      if (!isSplitable(path) && splitFiles(0).length >
//        IO_WARNING_LARGEFILETHRESHOLD) {
//        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
//          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
//      }
//    }
//
//    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
//  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
