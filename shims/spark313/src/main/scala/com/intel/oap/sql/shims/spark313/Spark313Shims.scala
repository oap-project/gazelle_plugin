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

package com.intel.oap.sql.shims.spark313

import com.intel.oap.execution.ColumnarBatchScanExec
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
import org.apache.spark.util.ShimUtils
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, CustomShuffleReaderExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetOptions, ParquetReadSupport, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkVectorUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, OutputWriter}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, REPARTITION, ReusedExchangeExec, ShuffleExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

class Spark313Shims extends SparkShims {

  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                                  shuffleId: Int, mapId: Long, partitionLengths: Array[Long], dataTmp: File): Unit =
  ShimUtils.shuffleBlockResolverWriteAndCommit(shuffleBlockResolver, shuffleId, mapId, partitionLengths, dataTmp)

  override def getDatetimeRebaseMode(fileMetaData: FileMetaData, parquetOptions: ParquetOptions):
  SQLConf.LegacyBehaviorPolicy.Value = {
    DataSourceUtils.datetimeRebaseMode(
      fileMetaData.getKeyValueMetaData.get,
      SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ))
  }

  override def newParquetFilters(parquetSchema: MessageType,
                                 pushDownDate: Boolean,
                                 pushDownTimestamp: Boolean,
                                 pushDownDecimal: Boolean,
                                 pushDownStringStartWith: Boolean,
                                 pushDownInFilterThreshold: Int,
                                 isCaseSensitive: Boolean,
                                 fileMetaData: FileMetaData,
                                 parquetOptions: ParquetOptions
                                ): ParquetFilters = {
    new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
  }

  override def newVectorizedParquetRecordReader(convertTz: ZoneId,
                                                fileMetaData: FileMetaData,
                                                parquetOptions: ParquetOptions,
                                                useOffHeap: Boolean,
                                                capacity: Int): VectorizedParquetRecordReader = {
    new VectorizedParquetRecordReader(
      convertTz,
      getDatetimeRebaseMode(fileMetaData, parquetOptions).toString,
      "",
      useOffHeap,
      capacity)
  }

  override def newParquetReadSupport(convertTz: Option[ZoneId],
                            enableVectorizedReader: Boolean,
                            fileMetaData: FileMetaData,
                            parquetOptions: ParquetOptions): ParquetReadSupport = {
    val datetimeRebaseMode = getDatetimeRebaseMode(fileMetaData, parquetOptions)
    new ParquetReadSupport(
      convertTz, enableVectorizedReader = false, datetimeRebaseMode, SQLConf.LegacyBehaviorPolicy.LEGACY)
  }

  /**
    * The runtimeFilters is just available from spark 3.2.
    */
  override def getRuntimeFilters(plan: BatchScanExec): Seq[Expression] = {
    return null
  }

  override def getBroadcastHashJoinOutputPartitioningExpandLimit(plan: SparkPlan): Int = {
    plan.sqlContext.getConf(
      "spark.sql.execution.broadcastHashJoin.outputPartitioningExpandLimit").trim().toInt
  }

  override def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
    mapId: Long, context: TaskContext,
    shuffleExecutorComponents: ShuffleExecutorComponents): AnyRef = {
    ShimUtils.newSortShuffleWriter(
      resolver,
      shuffleHandle,
      mapId,
      context,
      shuffleExecutorComponents)
  }

  override def getMaxBroadcastRows(mode: BroadcastMode): Long = {
    BroadcastExchangeExec.MAX_BROADCAST_TABLE_ROWS
  }

  override def getSparkSession(plan: SparkPlan): SparkSession = {
    plan.sqlContext.sparkSession
  }

  override def doFetchFile(urlString: String, targetDirHandler: File,
                           targetFileName: String, sparkConf: SparkConf): Unit = {
    ShimUtils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf)
  }

  override def newBroadcastQueryStageExec(id: Int, plan: BroadcastExchangeExec):
  BroadcastQueryStageExec = {
    BroadcastQueryStageExec(id, plan)
  }

  /**
    * CustomShuffleReaderExec is renamed to AQEShuffleReadExec from spark 3.2.
    */
  override def isCustomShuffleReaderExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: CustomShuffleReaderExec => true
      case _ => false
    }
  }

  override def newCustomShuffleReaderExec(child: SparkPlan, partitionSpecs : Seq[ShufflePartitionSpec]): SparkPlan = {
    CustomShuffleReaderExec(child, partitionSpecs)
  }

  /**
    * Only applicable to CustomShuffleReaderExec. Otherwise, an exception will be thrown.
    */
  override def getChildOfCustomShuffleReaderExec(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan: CustomShuffleReaderExec => plan.child
      case _ => throw new RuntimeException("CustomShuffleReaderExec is expected!")
    }
  }

  /**
    * Only applicable to CustomShuffleReaderExec. Otherwise, an exception will be thrown.
    */
  override def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan): Seq[ShufflePartitionSpec] = {
    plan match {
      case plan: CustomShuffleReaderExec => plan.partitionSpecs
      case _ => throw new RuntimeException("CustomShuffleReaderExec is expected!")
    }
  }

  override def isRepartition(shuffleOrigin: ShuffleOrigin): Boolean = {
    shuffleOrigin match {
      case REPARTITION => true
      case _ => false
    }
  }

}