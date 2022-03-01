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

package com.intel.oap.sql.shims.spark311

import com.intel.oap.execution.ColumnarBatchScanExec
import com.intel.oap.spark.sql.ArrowWriteQueue
import com.intel.oap.sql.shims.{ShimDescriptor, SparkShims}
import java.io.File

import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.MigratableResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleUtil
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.ColumnarShuffleExchangeAdaptor
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, CustomShuffleReaderExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkVectorUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, OutputWriter}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

class Spark311Shims extends SparkShims {

  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                                  shuffleId: Int, mapId: Long, partitionLengths: Array[Long], dataTmp: File): Unit =
  ShuffleUtil.shuffleBlockResolverWriteAndCommit(shuffleBlockResolver, shuffleId, mapId, partitionLengths, dataTmp)

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
                           datetimeRebaseMode: SQLConf.LegacyBehaviorPolicy.Value): ParquetFilters = {
    new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
  }

  override def newOutputWriter(writeQueue: ArrowWriteQueue, path: String): OutputWriter = {
    new OutputWriter {
      override def write(row: InternalRow): Unit = {
        val batch = row.asInstanceOf[FakeRow].batch
        writeQueue.enqueue(SparkVectorUtils
          .toArrowRecordBatch(batch))
      }

      override def close(): Unit = {
        writeQueue.close()
      }
    }
  }

  override def newColumnarBatchScanExec(plan: BatchScanExec): BatchScanExec = {
    new ColumnarBatchScanExec(plan.output, plan.scan)
  }

  override def getBroadcastHashJoinOutputPartitioningExpandLimit(sqlContext: SQLContext, conf: SQLConf): Int = {
    sqlContext.getConf(
      "spark.sql.execution.broadcastHashJoin.outputPartitioningExpandLimit").trim().toInt
  }

  override def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
    mapId: Long, context: TaskContext,
    shuffleExecutorComponents: ShuffleExecutorComponents): AnyRef = {
    ShuffleUtil.newSortShuffleWriter(
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
    Utils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf, null, null)
  }

//  /**
//    * Fix compatibility issue that ShuffleQueryStageExec has an additional argument in spark 3.2.
//    * ShuffleExchangeExec replaces ColumnarShuffleExchangeAdaptor to avoid cyclic dependency. This
//    * changes need futher test to verify.
//    */
//  override def outputPartitioningForColumnarCustomShuffleReaderExec(child: SparkPlan): Partitioning = {
//    child match {
//      case ShuffleQueryStageExec(_, s: ShuffleExchangeExec) =>
//        s.child.outputPartitioning
//      case ShuffleQueryStageExec(
//      _,
//      r @ ReusedExchangeExec(_, s: ShuffleExchangeExec)) =>
//        s.child.outputPartitioning match {
//          case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
//          case other => other
//        }
//      case _ =>
//        throw new IllegalStateException("operating on canonicalization plan")
//    }
//  }

  override def newBroadcastQueryStageExec(id: Int, plan: SparkPlan): BroadcastQueryStageExec = {
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
  override def getPartitionSpecsOfCustomShuffleReaderExec(plan: SparkPlan): ShufflePartitionSpec = {
    plan match {
      case plan: CustomShuffleReaderExec => plan.partitionSpecs
      case _ => throw new RuntimeException("CustomShuffleReaderExec is expected!")
    }
  }

}