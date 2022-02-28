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
import java.io.File

import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.MigratableResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf

sealed abstract class ShimDescriptor

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch"
}

trait SparkShims {
  def getShimDescriptor: ShimDescriptor

  def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                         shuffleId: Int, mapId: Long, partitionLengths: Array[Long], dataTmp: File): Unit

  def getDatetimeRebaseMode(fileMetaData: FileMetaData, parquetOptions: ParquetOptions): SQLConf.LegacyBehaviorPolicy.Value

  def newParquetFilters(parquetSchema: MessageType,
                           pushDownDate: Boolean,
                           pushDownTimestamp: Boolean,
                           pushDownDecimal: Boolean,
                           pushDownStringStartWith: Boolean,
                           pushDownInFilterThreshold: Int,
                           isCaseSensitive: Boolean,
                           datetimeRebaseMode: SQLConf.LegacyBehaviorPolicy.Value): ParquetFilters

  def newOutputWriter(writeQueue: ArrowWriteQueue, path: String): OutputWriter

  /**
    * Use BatchScanExec as return type to avoid cyclic dependency. But a ColumnarBatchScanExec MUST
    * be returned.
    */
  def newColumnarBatchScanExec(plan: BatchScanExec): BatchScanExec

  def getBroadcastHashJoinOutputPartitioningExpandLimit(sqlContext: SQLContext, conf: SQLConf): Int

  /**
    * The access modifier of IndexShuffleBlockResolver & BaseShuffleHandle is private[spark]. So we
    * use their corresponding base types here. They will be checked and converted at implementation place.
    * SortShuffleWriter's access modifier is private[spark], so we let the return type be AnyRef and
    * make the conversion at the place where this method is called.
    * */
  def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
                           mapId: Long, context: TaskContext,
                           shuffleExecutorComponents: ShuffleExecutorComponents): AnyRef

  def getMaxBroadcastRows(mode: BroadcastMode): Long

  def getSparkSession(plan: SparkPlan): SparkSession

  def doFetchFile(urlString: String, targetDirHandler: File, targetFileName: String, sparkConf: SparkConf): Unit

//   We already have some code refactor to fix compatibility issues in ColumnarCustomShuffleReaderExec.
//  def outputPartitioningForColumnarCustomShuffleReaderExec(child: SparkPlan): Partitioning

  def newBroadcastQueryStageExec(id: Int, plan: SparkPlan): BroadcastQueryStageExec

  def isCustomShuffleReaderExec(plan: SparkPlan): Boolean

  /**
    * Return SparkPlan type since the type name is changed from spark 3.2.
    * TODO: need tests.
    */
  def newCustomShuffleReaderExec(child: SparkPlan, partitionSpecs : Seq[ShufflePartitionSpec]): SparkPlan

  def getChildOfCustomShuffleReaderExec(plan: SparkPlan): SparkPlan

  def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan): ShufflePartitionSpec

}
