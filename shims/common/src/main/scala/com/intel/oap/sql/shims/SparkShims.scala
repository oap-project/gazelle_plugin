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
import java.time.ZoneId

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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetOptions, ParquetReadSupport, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

sealed abstract class ShimDescriptor

// The append arg can either be "" for release version or be "-SNAPSHOT" for snapshot version.
case class SparkShimDescriptor(major: Int, minor: Int, patch: Int,
                               append: String = "") extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch$append"
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
                        fileMetaData: FileMetaData,
                        parquetOptions: ParquetOptions): ParquetFilters

  def newVectorizedParquetRecordReader(convertTz: ZoneId,
                                       fileMetaData: FileMetaData,
                                       parquetOptions: ParquetOptions,
                                       useOffHeap: Boolean,
                                       capacity: Int): VectorizedParquetRecordReader

  def newParquetReadSupport(convertTz: Option[ZoneId],
                            enableVectorizedReader: Boolean,
                            fileMetaData: FileMetaData,
                            parquetOptions: ParquetOptions): ParquetReadSupport

  def getRuntimeFilters(plan: BatchScanExec): Seq[Expression]

  def getBroadcastHashJoinOutputPartitioningExpandLimit(plan: SparkPlan): Int

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

  def newBroadcastQueryStageExec(id: Int, plan: BroadcastExchangeExec): BroadcastQueryStageExec

  def isCustomShuffleReaderExec(plan: SparkPlan): Boolean

  /**
    * Return SparkPlan type since the type name is changed from spark 3.2.
    * TODO: need tests.
    */
  def newCustomShuffleReaderExec(child: SparkPlan, partitionSpecs : Seq[ShufflePartitionSpec]): SparkPlan

  def getChildOfCustomShuffleReaderExec(plan: SparkPlan): SparkPlan

  def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan): Seq[ShufflePartitionSpec]

  /**
    * REPARTITION is changed to REPARTITION_BY_COL from spark 3.2.
    */
  def isRepartition(shuffleOrigin: ShuffleOrigin): Boolean
}
