/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.sql.shims

import com.intel.oap.spark.sql.ArrowWriteQueue
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf

sealed abstract class ShimDescriptor

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch"
}

trait SparkShims {
  def getShimDescriptor: ShimDescriptor

  def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: IndexShuffleBlockResolver,
                                         shuffleId: int, mapId: long, partitionLengths: Array[Long], dataTmp: File): Unit

  def getDatetimeRebaseMode(fileMetaData: FileMetaData, parquetOptions: ParquetOptions): SQLConf.LegacyBehaviorPolicy.Value

  def newParquetFilters(parquetSchema: MessageType,
                           pushDownDate: Boolean,
                           pushDownTimestamp: Boolean,
                           pushDownDecimal: Boolean,
                           pushDownStringStartWith: Boolean,
                           pushDownInFilterThreshold: Int,
                           isCaseSensitive: Boolean,
                           datetimeRebaseMode: LegacyBehaviorPolicy.Value): ParquetFilters

  def newOutputWriter(writeQueue: ArrowWriteQueue, path: String): OutputWriter

  def newColumnarBatchScanExec(plan: BatchScanExec): ColumnarBatchScanExec

  def getBroadcastHashJoinOutputPartitioningExpandLimit(sqlContext: SQLContext, conf: SQLConf): Int

  def newSortShuffleWriter(resolver: IndexShuffleBlockResolver, BaseShuffleHandle,
                           mapId: Long, context: TaskContext,
                           shuffleExecutorComponents: ShuffleExecutorComponents): SortShuffleWriter
  def getMaxBroadcastRows(mode: BroadcastMode): Long

  def getSparkSession(plan: SparkPlan): SparkSession

  def doFetchFile(urlString: String, targetDirHandler: File, targetFileName: String, sparkConf: SparkConf): Unit

//   We already have some code refactor to fix compatibility issues in ColumnarCustomShuffleReaderExec.
//  def outputPartitioningForColumnarCustomShuffleReaderExec(child: SparkPlan): Partitioning

  def newBroadcastQueryStageExec(id: Int, plan: SparkPlan): BroadcastQueryStageExec

  def isCustomShuffleReaderExec(plan: SparkPlan): Boolean

  def getChildOfCustomShuffleReaderExec(plan: SparkPlan): SparkPlan

  def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan): ShufflePartitionSpec
}
