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

package com.intel.oap.sql.shims.spark321

import com.intel.oap.spark.sql.ArrowWriteQueue
import com.intel.oap.sql.shims.{ShimDescriptor, SparkShims}
import java.io.File
import java.time.ZoneId

import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.MigratableResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.{CoalescedMapperPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetOptions, ParquetReadSupport, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkVectorUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, OutputWriter}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, REPARTITION_BY_COL, ReusedExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.ShimUtils

class Spark321Shims extends SparkShims {

  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                                  shuffleId: Int, mapId: Long,
                                                  partitionLengths: Array[Long],
                                                  dataTmp: File): Unit =
    ShimUtils.shuffleBlockResolverWriteAndCommit(
      shuffleBlockResolver, shuffleId, mapId, partitionLengths, dataTmp)

  def getDatetimeRebaseSpec(fileMetaData: FileMetaData, parquetOptions: ParquetOptions): RebaseSpec = {
    DataSourceUtils.datetimeRebaseSpec(
      fileMetaData.getKeyValueMetaData.get,
      parquetOptions.datetimeRebaseModeInRead)
  }

  def getInt96RebaseSpec(fileMetaData: FileMetaData, parquetOptions: ParquetOptions): RebaseSpec = {
    DataSourceUtils.int96RebaseSpec(
      fileMetaData.getKeyValueMetaData.get,
      parquetOptions.datetimeRebaseModeInRead)
  }

  override def getDatetimeRebaseMode(fileMetaData: FileMetaData, parquetOptions: ParquetOptions):
  SQLConf.LegacyBehaviorPolicy.Value = {
    getDatetimeRebaseSpec(fileMetaData, parquetOptions).mode
  }

  override def newParquetFilters(parquetSchema: MessageType,
                                 pushDownDate: Boolean,
                                 pushDownTimestamp: Boolean,
                                 pushDownDecimal: Boolean,
                                 pushDownStringStartWith: Boolean,
                                 pushDownInFilterThreshold: Int,
                                 isCaseSensitive: Boolean,
                                 fileMetaData: FileMetaData,
                                 parquetOptions: ParquetOptions):
  ParquetFilters = {
    return new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold,
      isCaseSensitive,
      getDatetimeRebaseSpec(fileMetaData, parquetOptions))
  }

  override def newVectorizedParquetRecordReader(convertTz: ZoneId,
                                                fileMetaData: FileMetaData,
                                                parquetOptions: ParquetOptions,
                                                useOffHeap: Boolean,
                                                capacity: Int): VectorizedParquetRecordReader = {
    val rebaseSpec = getDatetimeRebaseSpec(fileMetaData: FileMetaData, parquetOptions: ParquetOptions)
    // TODO: int96RebaseMode & int96RebaseTz are set to "", need to verify.
    new VectorizedParquetRecordReader(convertTz, rebaseSpec.mode.toString,
      rebaseSpec.timeZone, "", "", useOffHeap, capacity)
  }

  override def newParquetReadSupport(convertTz: Option[ZoneId],
                                     enableVectorizedReader: Boolean,
                                     fileMetaData: FileMetaData,
                                     parquetOptions: ParquetOptions): ParquetReadSupport = {
    val datetimeRebaseSpec = getDatetimeRebaseSpec(fileMetaData, parquetOptions)
    val int96RebaseSpec = getInt96RebaseSpec(fileMetaData, parquetOptions)
    new ParquetReadSupport(convertTz, enableVectorizedReader, datetimeRebaseSpec, int96RebaseSpec)
  }

  override def getRuntimeFilters(plan: BatchScanExec): Seq[Expression] = {
    return plan.runtimeFilters
  }

  override def getBroadcastHashJoinOutputPartitioningExpandLimit(plan: SparkPlan): Int = {
    plan.conf.broadcastHashJoinOutputPartitioningExpandLimit
  }

  override def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
                                    mapId: Long, context: TaskContext,
                                    shuffleExecutorComponents: ShuffleExecutorComponents):
  AnyRef = {
    ShimUtils.newSortShuffleWriter(
      resolver,
      shuffleHandle,
      mapId,
      context,
      shuffleExecutorComponents)
  }

  /** TODO: to see whether the below piece of code can be used for both spark 3.1/3.2.
    * */
  override def getMaxBroadcastRows(mode: BroadcastMode): Long = {
    // The below code is ported from BroadcastExchangeExec of spark 3.2.
    val maxBroadcastRows = mode match {
      case HashedRelationBroadcastMode(key, _)
        // NOTE: LongHashedRelation is used for single key with LongType. This should be kept
        // consistent with HashedRelation.apply.
        if !(key.length == 1 && key.head.dataType == LongType) =>
        // Since the maximum number of keys that BytesToBytesMap supports is 1 << 29,
        // and only 70% of the slots can be used before growing in UnsafeHashedRelation,
        // here the limitation should not be over 341 million.
        (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong
      case _ => 512000000
    }
    maxBroadcastRows
  }

  override def getSparkSession(plan: SparkPlan): SparkSession = {
    plan.session
  }

  override def doFetchFile(urlString: String, targetDirHandler: File,
                           targetFileName: String, sparkConf: SparkConf): Unit = {
    ShimUtils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf)
  }

  override def newBroadcastQueryStageExec(id: Int, plan: BroadcastExchangeExec):
  BroadcastQueryStageExec = {
    BroadcastQueryStageExec(id, plan, plan.doCanonicalize)
  }

  /**
    * CustomShuffleReaderExec is renamed to AQEShuffleReadExec from spark 3.2.
    */
  override def isCustomShuffleReaderExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: AQEShuffleReadExec => true
      case _ => false
    }
  }

  override def newCustomShuffleReaderExec(child: SparkPlan, partitionSpecs:
  Seq[ShufflePartitionSpec]): SparkPlan = {
    AQEShuffleReadExec(child, partitionSpecs)
  }

  /**
    * Only applicable to AQEShuffleReadExec. Otherwise, an exception will be thrown.
    */
  override def getChildOfCustomShuffleReaderExec(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: AQEShuffleReadExec => p.child
      case _ => throw new RuntimeException("AQEShuffleReadExec is expected!")
    }
  }

  /**
    * Only applicable to AQEShuffleReadExec. Otherwise, an exception will be thrown.
    */
  override def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan):
  Seq[ShufflePartitionSpec] = {
    plan match {
      case p: AQEShuffleReadExec => p.partitionSpecs
      case _ => throw new RuntimeException("AQEShuffleReadExec is expected!")
    }
  }

  override def isRepartition(shuffleOrigin: ShuffleOrigin): Boolean = {
    shuffleOrigin match {
      case REPARTITION_BY_COL => true
      case _ => false
    }
  }

  override def isCoalescedMapperPartitionSpec(spec: ShufflePartitionSpec): Boolean = {
    spec match {
      case _: CoalescedMapperPartitionSpec => true
      case _ => false
    }
  }

  override def getStartMapIndexOfCoalescedMapperPartitionSpec(spec: ShufflePartitionSpec): Int = {
    spec match {
      case c: CoalescedMapperPartitionSpec => c.startMapIndex
      case _ => throw new RuntimeException("CoalescedMapperPartitionSpec is expected!")
    }
  }

  override def getEndMapIndexOfCoalescedMapperPartitionSpec(spec: ShufflePartitionSpec): Int = {
    spec match {
      case c: CoalescedMapperPartitionSpec => c.endMapIndex
      case _ => throw new RuntimeException("CoalescedMapperPartitionSpec is expected!")
    }
  }

  override def getNumReducersOfCoalescedMapperPartitionSpec(spec: ShufflePartitionSpec): Int = {
    spec match {
      case c: CoalescedMapperPartitionSpec => c.numReducers
      case _ => throw new RuntimeException("CoalescedMapperPartitionSpec is expected!")
    }
  }

  override def toAttributes(fileIndex: PartitioningAwareFileIndex): Seq[AttributeReference] = {
    ShimUtils.toAttributes(fileIndex)
  }
}