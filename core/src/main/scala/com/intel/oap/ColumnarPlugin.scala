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

package com.intel.oap

import com.intel.oap.execution._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

case class ColumnarPreOverrides(conf: SparkConf) extends Rule[SparkPlan] {
  val columnarConf = ColumnarPluginConfig.getConf(conf)
  var isSupportAdaptive: Boolean = true

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case RowGuard(child: CustomShuffleReaderExec) =>
      replaceWithColumnarPlan(child)
    case plan: RowGuard =>
      val actualPlan = plan.child match {
        case p: BroadcastHashJoinExec =>
          p.withNewChildren(p.children.map(child =>
            child match {
              case RowGuard(queryStage: BroadcastQueryStageExec) =>
                fallBackBroadcastQueryStage(queryStage)
              case queryStage: BroadcastQueryStageExec =>
                fallBackBroadcastQueryStage(queryStage)
              case other => other
            }))
        case other =>
          other
      }
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithColumnarPlan))
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarBatchScanExec(plan.output, plan.scan)
    case plan: ProjectExec =>
      val columnarPlan = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if (columnarPlan.isInstanceOf[ColumnarConditionProjectExec]) {
        val cur_plan = columnarPlan.asInstanceOf[ColumnarConditionProjectExec]
        ColumnarConditionProjectExec(cur_plan.condition, plan.projectList, cur_plan.child)
      } else {
        ColumnarConditionProjectExec(null, plan.projectList, columnarPlan)
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
    case plan: ExpandExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarExpandExec(plan.projections, plan.output, child)
    case plan: SortExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      child match {
        case CoalesceBatchesExec(fwdChild: SparkPlan) =>
          ColumnarSortExec(plan.sortOrder, plan.global, fwdChild, plan.testSpillFrequency)
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
            child,
            plan.canChangeNumPartitions)
        } else {
          CoalesceBatchesExec(
            ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child,
              plan.canChangeNumPartitions))
        }
      } else {
        plan.withNewChildren(Seq(child))
      }
    case plan: ShuffledHashJoinExec =>
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarShuffledHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
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
          right)
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

    case plan: CustomShuffleReaderExec if columnarConf.enableColumnarShuffle =>
      plan.child match {
        case shuffle: ColumnarShuffleExchangeAdaptor =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs, plan.description))
        case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs, plan.description))
        case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
          reused match {
            case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              CoalesceBatchesExec(
                ColumnarCustomShuffleReaderExec(
                  plan.child,
                  plan.partitionSpecs,
                  plan.description))
            case _ =>
              plan
          }
        case _ =>
          plan
      }

    case plan: WindowExec =>
      if (columnarConf.enableColumnarWindow) {
        val child = plan.child match {
          case sort: SortExec => // remove ordering requirements
            replaceWithColumnarPlan(sort.child)
          case _ =>
            replaceWithColumnarPlan(plan.child)
        }
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        try {
          return new ColumnarWindowExec(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            child)
        } catch {
          case _: Throwable =>
            logInfo("Columnar Window: Falling back to regular Window...")
        }
      }
      logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
      val children = plan.children.map(replaceWithColumnarPlan)
      plan.withNewChildren(children)

    case p =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${p.getClass} is currently not supported.")
      p.withNewChildren(children.map(fallBackBroadcastExchangeOrNot))
  }

  def fallBackBroadcastQueryStage(curPlan: BroadcastQueryStageExec): BroadcastQueryStageExec = {
    curPlan.plan match {
      case originalBroadcastPlan: ColumnarBroadcastExchangeAdaptor =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            DataToArrowColumnarExec(originalBroadcastPlan, 1)))
      case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeAdaptor) =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            DataToArrowColumnarExec(curPlan.plan, 1)))
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

case class ColumnarPostOverrides(conf: SparkConf) extends Rule[SparkPlan] {
  val columnarConf = ColumnarPluginConfig.getConf(conf)
  var isSupportAdaptive: Boolean = true

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"ColumnarPostOverrides RowToArrowColumnarExec(${child.getClass})")
      RowToArrowColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeAdaptor) =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithColumnarPlan(child.child)))
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
  val rowGuardOverrides = ColumnarGuardRule(conf)
  val preOverrides = ColumnarPreOverrides(conf)
  val postOverrides = ColumnarPostOverrides(conf)
  val collapseOverrides = ColumnarCollapseCodegenStages(conf)
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
      preOverrides.setAdaptiveSupport(isSupportAdaptive)
      preOverrides(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      postOverrides.setAdaptiveSupport(isSupportAdaptive)
      val tmpPlan = postOverrides(plan)
      collapseOverrides(tmpPlan)
    } else {
      plan
    }
  }

}

/**
 * Extension point to enable columnar processing.
 *
 * To run with columnar set spark.sql.extensions to com.intel.oap.ColumnarPlugin
 */
class ColumnarPlugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logDebug(
      "Installing extensions to enable columnar CPU support." +
        " To disable this set `org.apache.spark.example.columnar.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}
