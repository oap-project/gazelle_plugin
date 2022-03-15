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

package com.intel.oap.extension

import com.intel.oap.GazellePluginConfig
import com.intel.oap.GazelleSparkExtensionsInjector
import com.intel.oap.execution._
import com.intel.oap.extension.columnar.ColumnarGuardRule
import com.intel.oap.extension.columnar.RowGuard
import com.intel.oap.sql.execution.RowToArrowColumnarExec
import com.intel.oap.sql.shims.SparkShimLoader

import org.apache.spark.{MapOutputStatistics, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{ShuffleStageInfo, _}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, ColumnarArrowEvalPythonExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.spark.util.ShufflePartitionUtils

import scala.collection.mutable

case class ColumnarPreOverrides() extends Rule[SparkPlan] {
  val columnarConf: GazellePluginConfig = GazellePluginConfig.getSessionConf
  var isSupportAdaptive: Boolean = true


  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case RowGuard(child: SparkPlan)
      if SparkShimLoader.getSparkShims.isCustomShuffleReaderExec(child) =>
      replaceWithColumnarPlan(child)
    case plan: RowGuard =>
      val actualPlan = plan.child match {
        case p: BroadcastHashJoinExec =>
          p.withNewChildren(p.children.map {
            case RowGuard(queryStage: BroadcastQueryStageExec) =>
              fallBackBroadcastQueryStage(queryStage)
            case queryStage: BroadcastQueryStageExec =>
              fallBackBroadcastQueryStage(queryStage)
            case plan: BroadcastExchangeExec =>
              // if BroadcastHashJoin is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case other =>
          other
      }
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithColumnarPlan))
    case plan: ArrowEvalPythonExec =>
      val columnarChild = replaceWithColumnarPlan(plan.child)
      ColumnarArrowEvalPythonExec(plan.udfs, plan.resultAttrs, columnarChild, plan.evalType)
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      val runtimeFilters = SparkShimLoader.getSparkShims.getRuntimeFilters(plan)
      new ColumnarBatchScanExec(plan.output, plan.scan, runtimeFilters) {
        // This method is a commonly shared implementation for ColumnarBatchScanExec.
        // We move it outside of shim layer to break the cyclic dependency caused by
        // ColumnarDataSourceRDD.
        override def doExecuteColumnar(): RDD[ColumnarBatch] = {
          val numOutputRows = longMetric("numOutputRows")
          val numInputBatches = longMetric("numInputBatches")
          val numOutputBatches = longMetric("numOutputBatches")
          val scanTime = longMetric("scanTime")
          val inputSize = longMetric("inputSize")
          val inputColumnarRDD =
            new ColumnarDataSourceRDD(sparkContext, partitions, readerFactory,
              true, scanTime, numInputBatches, inputSize, tmpDir)
          inputColumnarRDD.map { r =>
            numOutputRows += r.numRows()
            numOutputBatches += 1
            r
          }
        }
      }
    case plan: CoalesceExec =>
      ColumnarCoalesceExec(plan.numPartitions, replaceWithColumnarPlan(plan.child))
    case plan: InMemoryTableScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarInMemoryTableScanExec(plan.attributes, plan.predicates, plan.relation)
    case plan: ProjectExec =>
      val columnarChild = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      columnarChild match {
        case ch: ColumnarConditionProjectExec =>
          if (ch.projectList == null) {
            ColumnarConditionProjectExec(ch.condition, plan.projectList, ch.child)
          } else {
            ColumnarConditionProjectExec(null, plan.projectList, columnarChild)
          }
        case _ =>
          ColumnarConditionProjectExec(null, plan.projectList, columnarChild)
      }
    case plan: FilterExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarConditionProjectExec(plan.condition, null, child)
    case plan: HashAggregateExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarHashAggregateExec(
        plan.requiredChildDistributionExpressions,
        plan.groupingExpressions,
        plan.aggregateExpressions,
        plan.aggregateAttributes,
        plan.initialInputBufferOffset,
        plan.resultExpressions,
        child)
    case plan: UnionExec =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarUnionExec(children)
    case plan: LocalLimitExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarLocalLimitExec(plan.limit, child)
    case plan: GlobalLimitExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarGlobalLimitExec(plan.limit, child)
    case plan: ExpandExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarExpandExec(plan.projections, plan.output, child)
    case plan: SortExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if (child.isInstanceOf[ExpandExec]) {
        //FIXME: quick for Sort spill bug
        return plan.withNewChildren(Seq(child))
      }
      child match {
        case p: CoalesceBatchesExec =>
          ColumnarSortExec(plan.sortOrder, plan.global, p.child, plan.testSpillFrequency)
        case _ =>
          ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      }
    case plan: ShuffleExchangeExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if ((child.supportsColumnar || columnarConf.enablePreferColumnar) && columnarConf.enableColumnarShuffle) {
        if (isSupportAdaptive) {
          new ColumnarShuffleExchangeAdaptor(
            plan.outputPartitioning,
            child)
        } else {
          CoalesceBatchesExec(
            ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child))
        }
      } else {
        plan.withNewChildren(Seq(child))
      }
    case plan: ShuffledHashJoinExec =>
      val maybeOptimized = if (
        GazellePluginConfig.getSessionConf.resizeShuffledHashJoinInputPartitions &&
            ShufflePartitionUtils.withCustomShuffleReaders(plan)) {
        // We are on AQE execution. Try repartitioning inputs
        // to avoid OOM as ColumnarShuffledHashJoin doesn't spill
        // input data.
        ShufflePartitionUtils.reoptimizeShuffledHashJoinInput(plan)
      } else {
        plan
      }
      val left = replaceWithColumnarPlan(maybeOptimized.left)
      val right = replaceWithColumnarPlan(maybeOptimized.right)
      logDebug(s"Columnar Processing for ${maybeOptimized.getClass} is currently supported.")
      ColumnarShuffledHashJoinExec(
        maybeOptimized.leftKeys,
        maybeOptimized.rightKeys,
        maybeOptimized.joinType,
        maybeOptimized.buildSide,
        maybeOptimized.condition,
        left,
        right)
    case plan: BroadcastQueryStageExec =>
      logDebug(
        s"Columnar Processing for ${plan.getClass} is currently supported, actual plan is ${plan.plan.getClass}.")
      plan
    case plan: BroadcastExchangeExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if (isSupportAdaptive)
        new ColumnarBroadcastExchangeAdaptor(plan.mode, child)
      else
        ColumnarBroadcastExchangeExec(plan.mode, child)
    case plan: BroadcastHashJoinExec =>
      if (columnarConf.enableColumnarBroadcastJoin) {
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarBroadcastHashJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.buildSide,
          plan.condition,
          left,
          right,
          nullAware = plan.isNullAwareAntiJoin)
      } else {
        val children = plan.children.map(replaceWithColumnarPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }

    case plan: SortMergeJoinExec =>
      if (columnarConf.enableColumnarSortMergeJoin) {
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")

        ColumnarSortMergeJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      } else {
        val children = plan.children.map(replaceWithColumnarPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }

    case plan: ShuffleQueryStageExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      plan

    case plan
      if (SparkShimLoader.getSparkShims.isCustomShuffleReaderExec(plan)
        && columnarConf.enableColumnarShuffle) =>
      val child = SparkShimLoader.getSparkShims.getChildOfCustomShuffleReaderExec(plan)
      val partitionSpecs =
        SparkShimLoader.getSparkShims.getPartitionSpecsOfCustomShuffleReaderExec(plan)
      child match {
        case shuffle: ColumnarShuffleExchangeAdaptor =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(child, partitionSpecs))
        // Use the below code to replace the above to realize compatibility on spark 3.1 & 3.2.
        case shuffleQueryStageExec: ShuffleQueryStageExec =>
          shuffleQueryStageExec.plan match {
            case s: ColumnarShuffleExchangeAdaptor =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              CoalesceBatchesExec(
                ColumnarCustomShuffleReaderExec(child, partitionSpecs))
            case r @ ReusedExchangeExec(_, s: ColumnarShuffleExchangeAdaptor) =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              CoalesceBatchesExec(
                ColumnarCustomShuffleReaderExec(
                  child,
                  partitionSpecs))
            case _ =>
              plan
          }

        case _ =>
          plan
      }

    case plan: WindowExec =>
      try {
        ColumnarWindowExec.createWithOptimizations(
          plan.windowExpression,
          plan.partitionSpec,
          plan.orderSpec,
          isLocalized = false,
          replaceWithColumnarPlan(plan.child))
      } catch {
        case _: Throwable =>
          logInfo("Columnar Window: Falling back to regular Window...")
          plan
      }
    case plan: LocalWindowExec =>
      try {
        ColumnarWindowExec.createWithOptimizations(
          plan.windowExpression,
          plan.partitionSpec,
          plan.orderSpec,
          isLocalized = true,
          replaceWithColumnarPlan(plan.child))
      } catch {
        case _: Throwable =>
          logInfo("Localized Columnar Window: Falling back to regular Window...")
          plan
      }
    case p =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${p.getClass} is currently not supported.")
      p.withNewChildren(children.map(fallBackBroadcastExchangeOrNot))
  }

  def fallBackBroadcastQueryStage(curPlan: BroadcastQueryStageExec): BroadcastQueryStageExec = {
    curPlan.plan match {
      case originalBroadcastPlan: ColumnarBroadcastExchangeAdaptor =>
        val newBroadcast = BroadcastExchangeExec(
          originalBroadcastPlan.mode,
          DataToArrowColumnarExec(originalBroadcastPlan, 1))
        SparkShimLoader.getSparkShims.newBroadcastQueryStageExec(curPlan.id, newBroadcast)
      case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeAdaptor) =>
        val newBroadcast = BroadcastExchangeExec(
          originalBroadcastPlan.mode,
          DataToArrowColumnarExec(curPlan.plan, 1))
        SparkShimLoader.getSparkShims.newBroadcastQueryStageExec(curPlan.id, newBroadcast)
      case _ =>
        curPlan
    }
  }

  def fallBackBroadcastExchangeOrNot(plan: SparkPlan): SparkPlan = plan match {
    case p: ColumnarBroadcastExchangeExec =>
      // aqe is disabled
      BroadcastExchangeExec(p.mode, DataToArrowColumnarExec(p, 1))
    case p: ColumnarBroadcastExchangeAdaptor =>
      // aqe is disabled
      BroadcastExchangeExec(p.mode, DataToArrowColumnarExec(p, 1))
    case p: BroadcastQueryStageExec =>
      // ape is enabled
      fallBackBroadcastQueryStage(p)
    case other => other
  }
  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

}

case class ColumnarPostOverrides() extends Rule[SparkPlan] {
  val columnarConf = GazellePluginConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      if (columnarConf.enableArrowRowToColumnar) {
        logDebug(s"ColumnarPostOverrides ArrowRowToColumnarExec(${child.getClass})")
        try {
          ArrowRowToColumnarExec(child)
        } catch {
          case _: Throwable =>
            logInfo("ArrowRowToColumnar: Falling back to RowToColumnar...")
            RowToArrowColumnarExec(child)
        }
      } else {
        logDebug(s"ColumnarPostOverrides RowToArrowColumnarExec(${child.getClass})")
        RowToArrowColumnarExec(child)
      }
    case ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeAdaptor) =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithColumnarPlan(child.child)))
    case plan: ColumnarToRowExec =>
      if (columnarConf.enableArrowColumnarToRow) {
        val child = replaceWithColumnarPlan(plan.child)
        logDebug(s"ColumnarPostOverrides ArrowColumnarToRowExec(${child.getClass})")
        try {
          new ArrowColumnarToRowExec(child)
        } catch {
          case _: Throwable =>
            logInfo("ArrowColumnarToRowExec: Falling back to ColumnarToRow...")
            ColumnarToRowExec(child)
        }
      } else {
        val children = plan.children.map(replaceWithColumnarPlan)
        plan.withNewChildren(children)
      }
    case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
          c.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan.
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          if (columnarConf.enableArrowColumnarToRow) {
            try {
              val child = replaceWithColumnarPlan(c.child)
              new ArrowColumnarToRowExec(child)
            } catch {
              case _: Throwable =>
                logInfo("ArrowColumnarToRow : Falling back to ColumnarToRow...")
                c.withNewChildren(c.children.map(replaceWithColumnarPlan))
            }
          } else {
            c.withNewChildren(c.children.map(replaceWithColumnarPlan))
          }
        case other =>
          replaceWithColumnarPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled =
    session.sqlContext.getConf("org.apache.spark.example.columnar.enabled", "true").trim.toBoolean
  def conf = session.sparkContext.getConf

  // Do not create rules in class initialization as we should access SQLConf while creating the rules. At this time
  // SQLConf may not be there yet.
  def rowGuardOverrides = ColumnarGuardRule()
  def preOverrides = ColumnarPreOverrides()
  def postOverrides = ColumnarPostOverrides()

  val columnarWholeStageEnabled = conf.getBoolean("spark.oap.sql.columnar.wholestagecodegen", defaultValue = true)
  def collapseOverrides = ColumnarCollapseCodegenStages(columnarWholeStageEnabled)

  var isSupportAdaptive: Boolean = true

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    // TODO migrate dynamic-partition-pruning onto adaptive execution.
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case e: Exchange => true
      case other => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled && (sanityCheck(plan) &&
    !plan.logicalLink.exists(_.isStreaming) &&
    !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
    plan.children.forall(supportAdaptive)))
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      isSupportAdaptive = supportAdaptive(plan)
      val rule = preOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      rule(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      val rule = postOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      val tmpPlan = rule(plan)
      collapseOverrides(tmpPlan)
    } else {
      plan
    }
  }
}

object ColumnarOverrides extends GazelleSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(ColumnarOverrideRules)
  }
}
