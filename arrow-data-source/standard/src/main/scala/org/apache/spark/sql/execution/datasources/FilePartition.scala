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
package org.apache.spark.sql.execution.datasources

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowSQLConf

import org.apache.spark.Partition
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition

/**
 * A collection of file blocks that should be read as a single task
 * (possibly from multiple partitioned directories).
 */
case class FilePartition(index: Int, files: Array[PartitionedFile])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

object FilePartition extends Logging {

  private val taskParallelismNum = {
    val sparkConf = SparkSession.active.sparkContext.conf
    sparkConf.get(config.EXECUTOR_CORES) / sparkConf.get(config.CPUS_PER_TASK) *
      SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf)
  }

  def getFilePartitions(
      sparkSession: SparkSession,
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

    val sqlConf = sparkSession.sessionState.conf
    val arrowSQLConf = new ArrowSQLConf(sqlConf)
    var openCostInBytes = sqlConf.filesOpenCostInBytes
    var maxPartitionBytes = maxSplitBytes
    val maxFilesInPartition = arrowSQLConf.filesMaxNumInPartition
    if (arrowSQLConf.filesDynamicMergeEnabled) {
      maxPartitionBytes = maxPartitionBytes
      val expectedFilePartitionNum =
        arrowSQLConf.filesExpectedPartitionNum.getOrElse(taskParallelismNum)
      if (partitionedFiles.size < expectedFilePartitionNum) {
        openCostInBytes = maxPartitionBytes
      } else {
        val totalSize = partitionedFiles.foldLeft(0L) {
          (totalSize, file) => totalSize + file.length + openCostInBytes
        }
        val expectFilePartitionSize = totalSize / expectedFilePartitionNum
        if (expectFilePartitionSize < maxPartitionBytes) {
          maxPartitionBytes = expectFilePartitionSize
        }
      }
    }
    logInfo(s"Using $openCostInBytes as openCost.")
    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      // TODO: find a better way to average the file count in each partition
      if (currentSize + file.length > maxPartitionBytes ||
        maxFilesInPartition.exists(fileNum => currentFiles.size >= fileNum)) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }

  def maxSplitBytes(
      sparkSession: SparkSession,
      selectedPartitions: Seq[PartitionDirectory]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.conf.get("spark.sql.leafNodeDefaultParallelism",
        sparkSession.sparkContext.defaultParallelism.toString).toInt)
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
