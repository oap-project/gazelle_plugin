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

package org.apache.spark.util

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.adaptive.OptimizeSkewedJoin.supportedJoinTypes
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, OptimizeSkewedJoin, ShuffleQueryStageExec, ShuffleStage, ShuffleStageInfo}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REPARTITION}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkContext, SparkEnv}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec, SortExec, SparkPlan}
import org.apache.spark.storage.BlockManagerId

import java.io.{Externalizable, ObjectInput, ObjectOutput}

object ShufflePartitionUtils {

  def withCustomShuffleReaders(plan: SparkPlan): Boolean = {
    plan.children.forall(_.isInstanceOf[CustomShuffleReaderExec])
  }

  def isShuffledHashJoinTypeOptimizable(joinType: JoinType): Boolean = {
    joinType match {
      case a @ (Inner | Cross | LeftSemi | LeftAnti | LeftOuter | RightOuter) =>
        true
      case _ => false
    }
  }

  def reoptimizeShuffledHashJoinInput(plan: ShuffledHashJoinExec): ShuffledHashJoinExec =
    plan match {
      case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
      s1 @ ShuffleStage(leftStageInfo: ShuffleStageInfo),
      s2 @ ShuffleStage(rightStageInfo: ShuffleStageInfo))
        if isShuffledHashJoinTypeOptimizable(joinType) =>

        val left = plan.left
        val right = plan.right
        val shuffleStages = Array(left, right)
            .map(c => c.asInstanceOf[CustomShuffleReaderExec]
                .child.asInstanceOf[ShuffleQueryStageExec]).toList

        if (shuffleStages.isEmpty) {
          return plan
        }

        if (!shuffleStages.forall(s => s.shuffle.shuffleOrigin match {
          case ENSURE_REQUIREMENTS => true
          case REPARTITION => true
          case _ => false
        })) {
          return plan
        }

        if (plan.left.outputPartitioning != plan.right.outputPartitioning) {
          return plan
        }

        val sparkConf = SparkContext.getOrCreate().getConf

        val dynAllocEnabled =
          sparkConf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = false)
        if (dynAllocEnabled) {
          // doesn't support dynamic allocation yet
          return plan
        }


        // also see SparkEnv.get.mapOutputTracker
        if (shuffleStages(0).mapStats.isEmpty || shuffleStages(1).mapStats.isEmpty) {
          return plan
        }

        val offHeapSize =
          sparkConf.getSizeAsBytes("spark.memory.offHeap.size", 0L)

        if (offHeapSize == 0L) {
          return plan
        }

        val buildSizeLimit =
          sparkConf.getSizeAsBytes("spark.oap.sql.columnar.shuffledhashjoin.buildsizelimit", "100m")

        val offHeapOptimizationTarget = buildSizeLimit

        val leftSpecs = left.asInstanceOf[CustomShuffleReaderExec].partitionSpecs
        val rightSpecs = right.asInstanceOf[CustomShuffleReaderExec].partitionSpecs

        if (leftSpecs.size != rightSpecs.size) {
          throw new IllegalStateException("Input partition mismatch for ColumnarShuffledHashJoin")
        }

        def canSplitLeftSide(joinType: JoinType) = {
          joinType == Inner || joinType == Cross || joinType == LeftSemi ||
              joinType == LeftAnti || joinType == LeftOuter
        }

        def canSplitRightSide(joinType: JoinType) = {
          joinType == Inner || joinType == Cross || joinType == RightOuter
        }

        val leftJoinedParts = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
        val rightJoinedParts = mutable.ArrayBuffer.empty[ShufflePartitionSpec]

        for (specIndex <- leftSpecs.indices) {
          val leftSpec = leftSpecs(specIndex).asInstanceOf[CoalescedPartitionSpec]
          val rightSpec = rightSpecs(specIndex).asInstanceOf[CoalescedPartitionSpec]
          val startReducerIndex = leftSpec.startReducerIndex
          val endReducerIndex = leftSpec.endReducerIndex

          val leftOptimizedPartitionSpecs = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
          val rightOptimizedPartitionSpecs = mutable.ArrayBuffer.empty[ShufflePartitionSpec]

          plan.buildSide match {
            case BuildLeft =>
              if (!canSplitLeftSide(plan.joinType)) {
                leftOptimizedPartitionSpecs += leftSpec
              } else {
                val numReducers = leftStageInfo.shuffleStage.shuffle.numPartitions
                leftOptimizedPartitionSpecs ++= ShufflePartitionUtils.createSkewPartitionSpecs(
                  leftStageInfo.mapStats.shuffleId, startReducerIndex, endReducerIndex,
                  numReducers, offHeapOptimizationTarget).getOrElse(Seq(leftSpec))
              }
              rightOptimizedPartitionSpecs += rightSpec
            case BuildRight =>
              leftOptimizedPartitionSpecs += leftSpec
              if (!canSplitRightSide(plan.joinType)) {
                rightOptimizedPartitionSpecs += rightSpec
              } else {
                val numReducers = rightStageInfo.shuffleStage.shuffle.numPartitions
                rightOptimizedPartitionSpecs ++= ShufflePartitionUtils.createSkewPartitionSpecs(
                  rightStageInfo.mapStats.shuffleId, startReducerIndex, endReducerIndex,
                  numReducers, offHeapOptimizationTarget).getOrElse(Seq(rightSpec))
              }
          }

          for {
            leftSidePartition <- leftOptimizedPartitionSpecs
                rightSidePartition <- rightOptimizedPartitionSpecs
          } {
            leftJoinedParts += leftSidePartition
            rightJoinedParts += rightSidePartition
          }
        }

        val leftReader = left.asInstanceOf[CustomShuffleReaderExec]
        val rightReader = right.asInstanceOf[CustomShuffleReaderExec]

        // todo equality check?
        plan.withNewChildren(
          Array(
            CustomShuffleReaderExec(leftReader.child,
              leftJoinedParts),
            CustomShuffleReaderExec(rightReader.child,
              rightJoinedParts)
          )).asInstanceOf[ShuffledHashJoinExec]
      case _ =>
        plan
    }

  private object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleStageInfo] = plan match {
      case s: ShuffleQueryStageExec
        if s.mapStats.isDefined &&
            OptimizeSkewedJoin.supportedShuffleOrigins.contains(s.shuffle.shuffleOrigin) =>
        val mapStats = s.mapStats.get
        val sizes = mapStats.bytesByPartitionId
        val partitions = sizes.zipWithIndex.map {
          case (size, i) => CoalescedPartitionSpec(i, i + 1) -> size
        }
        Some(ShuffleStageInfo(s, mapStats, s.getRuntimeStatistics, partitions))

      case CustomShuffleReaderExec(s: ShuffleQueryStageExec, partitionSpecs)
        if s.mapStats.isDefined && partitionSpecs.nonEmpty &&
            OptimizeSkewedJoin.supportedShuffleOrigins.contains(s.shuffle.shuffleOrigin) =>
        val statistics = s.getRuntimeStatistics
        val mapStats = s.mapStats.get
        val sizes = mapStats.bytesByPartitionId
        val partitions = partitionSpecs.map {
          case spec @ CoalescedPartitionSpec(start, end) =>
            var sum = 0L
            var i = start
            while (i < end) {
              sum += sizes(i)
              i += 1
            }
            spec -> sum
          case other => throw new IllegalArgumentException(
            s"Expect CoalescedPartitionSpec but got $other")
        }
        Some(ShuffleStageInfo(s, mapStats, s.getRuntimeStatistics, partitions))

      case _ => None
    }
  }

  private case class ShuffleStageInfo(
      shuffleStage: ShuffleQueryStageExec,
      mapStats: MapOutputStatistics,
      exchangeStats: Statistics,
      partitionsWithSizes: Seq[(CoalescedPartitionSpec, Long)])

  def createSkewPartitionSpecs(
      shuffleId: Int,
      startReducerId: Int,
      endReducerId: Int,
      numAllReducers: Int,
      targetSize: Long): Option[Seq[ShufflePartitionSpec]] = {
    if (startReducerId + 1 == endReducerId) {
      return createSkewPartitionSpecs(shuffleId, startReducerId, numAllReducers, targetSize)
    }
    val optimizedPartitionSpecs = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var toBeCoalesced = mutable.ArrayBuffer.empty[Int]
    var toBeCoalescedSize = 0L
    for (redId <- startReducerId until endReducerId) {
      val reduceSize = getReduceSize(shuffleId, redId, numAllReducers)
      if (reduceSize > targetSize) {
        val optimized = createSkewPartitionSpecs(shuffleId, redId, numAllReducers, targetSize)
        if (optimized.nonEmpty) {
          optimizedPartitionSpecs ++= optimized.get
        } else {
          optimizedPartitionSpecs += CoalescedPartitionSpec(redId, redId + 1)
        }
      } else {
        if (toBeCoalescedSize + reduceSize > targetSize) {
          if (toBeCoalesced.isEmpty) {
            throw new IllegalStateException()
          }
          optimizedPartitionSpecs += CoalescedPartitionSpec(toBeCoalesced.head, toBeCoalesced
              .last + 1)
          toBeCoalesced = mutable.ArrayBuffer.empty[Int]
          toBeCoalescedSize = 0L
        }
        toBeCoalescedSize += reduceSize
        toBeCoalesced += redId
      }
    }
    if (toBeCoalesced.nonEmpty) {
      optimizedPartitionSpecs += CoalescedPartitionSpec(toBeCoalesced.head, toBeCoalesced
          .last + 1)
    }
    if (optimizedPartitionSpecs.isEmpty) {
      None
    } else {
      Some(optimizedPartitionSpecs)
    }
  }

  /**
   * The logic differs from
   * org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil#splitSizeListByTargetSize. We
   * adopt a hard limit for each merged partition to get rid of OOM.
   */
  def splitSizeListByTargetSize(sizes: Seq[Long], targetSize: Long): Array[Int] = {
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var currentPartitionSize = 0L
    var lastPartitionSize = -1L

    def tryMergePartitions(): Unit = {
      val shouldMergePartitions = lastPartitionSize > -1 &&
          ((currentPartitionSize + lastPartitionSize) < targetSize)
      if (shouldMergePartitions) {
        partitionStartIndices.remove(partitionStartIndices.length - 1)
        lastPartitionSize += currentPartitionSize
      } else {
        lastPartitionSize = currentPartitionSize
      }
    }

    while (i < sizes.length) {
      if (i > 0 && currentPartitionSize + sizes(i) > targetSize) {
        tryMergePartitions()
        partitionStartIndices += i
        currentPartitionSize = sizes(i)
      } else {
        currentPartitionSize += sizes(i)
      }
      i += 1
    }
    tryMergePartitions()
    partitionStartIndices.toArray
  }

  def createSkewPartitionSpecs(
      shuffleId: Int,
      reducerId: Int,
      numAllReducers: Int,
      targetSize: Long): Option[Seq[PartialReducerPartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId, numAllReducers)
    val mapStartIndices = splitSizeListByTargetSize(
      mapPartitionSizes, targetSize)
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        val dataSize = startMapIndex.until(endMapIndex).map(mapPartitionSizes(_)).sum
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
    }
  }

  def getMapSizesForReduceId(shuffleId: Int, partitionId: Int, numPartitions: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses.map {
      _.getSizeForBlock(numPartitions + partitionId)
    }
  }

  def getReduceSize(shuffleId: Int, partitionId: Int, numPartitions: Int): Long = {
    getMapSizesForReduceId(shuffleId, partitionId, numPartitions).sum
  }
}
