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

package org.apache.spark.sql.execution.columnar

import org.apache.commons.lang3.StringUtils

import org.apache.spark.TaskContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.columnar.{
  CachedBatch,
  CachedBatchSerializer,
  SimpleMetricsCachedBatch,
  SimpleMetricsCachedBatchSerializer
}
import org.apache.spark.sql.execution.{
  InputAdapter,
  QueryExecution,
  SparkPlan,
  WholeStageCodegenExec,
  ColumnarToRowExec
}
import org.apache.spark.sql.execution.vectorized.{
  OffHeapColumnVector,
  OnHeapColumnVector,
  WritableColumnVector
}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{
  BooleanType,
  ByteType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StructType,
  UserDefinedType
}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class DefaultCachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
    extends SimpleMetricsCachedBatch

private[sql] case class CachedRDDBuilder(
    serializer: CachedBatchSerializer,
    storageLevel: StorageLevel,
    @transient cachedPlan: SparkPlan,
    tableName: Option[String]) {

  @transient @volatile private var _cachedColumnBuffers
      : RDD[org.apache.spark.sql.columnar.CachedBatch] = null

  val sizeInBytesStats: LongAccumulator = cachedPlan.sqlContext.sparkContext.longAccumulator
  val rowCountStats: LongAccumulator = cachedPlan.sqlContext.sparkContext.longAccumulator

  val cachedName = tableName
    .map(n => s"In-memory table $n")
    .getOrElse(StringUtils.abbreviate(cachedPlan.toString, 1024))

  def cachedColumnBuffers: RDD[org.apache.spark.sql.columnar.CachedBatch] = {
    if (_cachedColumnBuffers == null) {
      synchronized {
        if (_cachedColumnBuffers == null) {
          _cachedColumnBuffers = buildBuffers()
        }
      }
    }
    _cachedColumnBuffers
  }

  def clearCache(blocking: Boolean = false): Unit = {
    if (_cachedColumnBuffers != null) {
      synchronized {
        if (_cachedColumnBuffers != null) {
          _cachedColumnBuffers.foreach(buffer => buffer match {
            case b: com.intel.oap.execution.ArrowCachedBatch =>
              b.release
            case other =>
          })
          _cachedColumnBuffers.unpersist(blocking)
          _cachedColumnBuffers = null
        }
      }
    }
  }

  def isCachedColumnBuffersLoaded: Boolean = {
    _cachedColumnBuffers != null
  }

  private def buildBuffers(): RDD[org.apache.spark.sql.columnar.CachedBatch] = {
    val cb = serializer.convertColumnarBatchToCachedBatch(
      cachedPlan.executeColumnar(),
      cachedPlan.output,
      storageLevel,
      cachedPlan.conf)

    val cached = cb
      .map { batch =>
        sizeInBytesStats.add(batch.sizeInBytes)
        rowCountStats.add(batch.numRows)
        batch
      }
      .persist(storageLevel)
    cached.setName(cachedName)
    cached
  }
}

object InMemoryRelation {

  private[this] var ser: Option[CachedBatchSerializer] = None
  private[this] def getSerializer(sqlConf: SQLConf): CachedBatchSerializer = synchronized {
    if (ser.isEmpty) {
      val serClass =
        Utils.classForName("com.intel.oap.execution.ArrowColumnarCachedBatchSerializer")
      val instance = serClass.getConstructor().newInstance().asInstanceOf[CachedBatchSerializer]
      ser = Some(instance)
    }
    ser.get
  }

  /* Visible for testing */
  private[columnar] def clearSerializer(): Unit = synchronized { ser = None }

  def convertToColumnarIfPossible(plan: SparkPlan): SparkPlan = plan match {
    case gen: WholeStageCodegenExec =>
      gen.child match {
        case c2r: ColumnarToRowExec =>
          c2r.child match {
            case ia: InputAdapter => ia.child
            case _ => plan
          }
        case _ => plan
      }
    case c2r: ColumnarToRowExec => // This matches when whole stage code gen is disabled.
      c2r.child
    case _ => plan
  }

  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      optimizedPlan: LogicalPlan): InMemoryRelation = {
    val serializer = getSerializer(optimizedPlan.conf)
    val columnarChild = convertToColumnarIfPossible(child)
    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, columnarChild, tableName)
    val relation =
      new InMemoryRelation(columnarChild.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(
             storageLevel: StorageLevel,
             qe: QueryExecution,
             tableName: Option[String]): InMemoryRelation = {
    val optimizedPlan = qe.optimizedPlan
    val serializer = getSerializer(optimizedPlan.conf)
    val child = if (serializer.supportsColumnarInput(optimizedPlan.output)) {
      convertToColumnarIfPossible(qe.executedPlan)
    } else {
      qe.executedPlan
    }
    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  /**
   * This API is intended only to be used for testing.
   */
  def apply(
      serializer: CachedBatchSerializer,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      optimizedPlan: LogicalPlan): InMemoryRelation = {
    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(cacheBuilder: CachedRDDBuilder, qe: QueryExecution): InMemoryRelation = {
    val optimizedPlan = qe.optimizedPlan
    val newBuilder = if (cacheBuilder.serializer.supportsColumnarInput(optimizedPlan.output)) {
      cacheBuilder.copy(cachedPlan = convertToColumnarIfPossible(qe.executedPlan))
    } else {
      cacheBuilder.copy(cachedPlan = qe.executedPlan)
    }
    val relation =
      new InMemoryRelation(newBuilder.cachedPlan.output, newBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(
      output: Seq[Attribute],
      cacheBuilder: CachedRDDBuilder,
      outputOrdering: Seq[SortOrder],
      statsOfPlanToCache: Statistics): InMemoryRelation = {
    val relation = InMemoryRelation(output, cacheBuilder, outputOrdering)
    relation.statsOfPlanToCache = statsOfPlanToCache
    relation
  }
}

case class InMemoryRelation(
    output: Seq[Attribute],
    @transient cacheBuilder: CachedRDDBuilder,
    override val outputOrdering: Seq[SortOrder])
    extends logical.LeafNode
    with MultiInstanceRelation {

  @volatile var statsOfPlanToCache: Statistics = null

  override def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)

  override def doCanonicalize(): logical.LogicalPlan =
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, cachedPlan.output)),
      cacheBuilder,
      outputOrdering)

  @transient val partitionStatistics = new PartitionStatistics(output)

  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan

  private[sql] def updateStats(rowCount: Long, newColStats: Map[Attribute, ColumnStat]): Unit =
    this.synchronized {
      val newStats = statsOfPlanToCache.copy(
        rowCount = Some(rowCount),
        attributeStats = AttributeMap((statsOfPlanToCache.attributeStats ++ newColStats).toSeq))
      statsOfPlanToCache = newStats
    }

  override def computeStats(): Statistics = {
    if (!cacheBuilder.isCachedColumnBuffersLoaded) {
      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
      statsOfPlanToCache
    } else {
      statsOfPlanToCache.copy(
        sizeInBytes = cacheBuilder.sizeInBytesStats.value.longValue,
        rowCount = Some(cacheBuilder.rowCountStats.value.longValue))
    }
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation =
    InMemoryRelation(newOutput, cacheBuilder, outputOrdering, statsOfPlanToCache)

  override def newInstance(): this.type = {
    InMemoryRelation(
      output.map(_.newInstance()),
      cacheBuilder,
      outputOrdering,
      statsOfPlanToCache).asInstanceOf[this.type]
  }

  // override `clone` since the default implementation won't carry over mutable states.
  override def clone(): LogicalPlan = {
    val cloned = this.copy()
    cloned.statsOfPlanToCache = this.statsOfPlanToCache
    cloned
  }

  override def simpleString(maxFields: Int): String =
    s"InMemoryRelation [${truncatedString(output, ", ", maxFields)}], ${cacheBuilder.storageLevel}"
}
