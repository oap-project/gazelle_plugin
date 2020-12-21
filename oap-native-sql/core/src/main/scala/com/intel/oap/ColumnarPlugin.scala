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
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithColumnarPlan))
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      new ColumnarBatchScanExec(plan.output, plan.scan)
    case plan: ProjectExec =>
      val columnarPlan = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      if (columnarPlan.isInstanceOf[ColumnarConditionProjectExec]) {
        val cur_plan = columnarPlan.asInstanceOf[ColumnarConditionProjectExec]
        new ColumnarConditionProjectExec(cur_plan.condition, plan.projectList, cur_plan.child)
      } else {
        new ColumnarConditionProjectExec(null, plan.projectList, columnarPlan)
      }
    case plan: FilterExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      new ColumnarConditionProjectExec(plan.condition, null, child)
    case plan: HashAggregateExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      new ColumnarHashAggregateExec(
        plan.requiredChildDistributionExpressions,
        plan.groupingExpressions,
        plan.aggregateExpressions,
        plan.aggregateAttributes,
        plan.initialInputBufferOffset,
        plan.resultExpressions,
        child)
    case plan: UnionExec =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      new ColumnarUnionExec(children)
    case plan: ExpandExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      new ColumnarExpandExec(plan.projections, plan.output, child)
    case plan: SortExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      child match {
        case CoalesceBatchesExec(fwdChild: SparkPlan) =>
          new ColumnarSortExec(plan.sortOrder, plan.global, fwdChild, plan.testSpillFrequency)
        case _ =>
          new ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      }
    case plan: ShuffleExchangeExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      if ((child.supportsColumnar || columnarConf.enablePreferColumnar) && columnarConf.enableColumnarShuffle) {
        if (SQLConf.get.adaptiveExecutionEnabled) {
          new ColumnarShuffleExchangeExec(
            plan.outputPartitioning,
            child,
            plan.canChangeNumPartitions)
        } else {
          CoalesceBatchesExec(
            new ColumnarShuffleExchangeExec(
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
    case plan: BroadcastHashJoinExec =>
      if (columnarConf.enableColumnarBroadcastJoin) {
        val originalLeft = plan.left
        val originalRight = plan.right
        val left = if (originalLeft.isInstanceOf[BroadcastExchangeExec]) {
          val child = originalLeft.asInstanceOf[BroadcastExchangeExec]
          new ColumnarBroadcastExchangeExec(child.mode, replaceWithColumnarPlan(child.child))
        } else {
          replaceWithColumnarPlan(originalLeft)
        }
        val right = if (originalRight.isInstanceOf[BroadcastExchangeExec]) {
          val child = originalRight.asInstanceOf[BroadcastExchangeExec]
          new ColumnarBroadcastExchangeExec(child.mode, replaceWithColumnarPlan(child.child))
        } else {
          replaceWithColumnarPlan(originalRight)
        }
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

        new ColumnarSortMergeJoinExec(
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

    case plan: BroadcastQueryStageExec =>
      plan

    case plan: ShuffleQueryStageExec =>
      if (columnarConf.enableColumnarShuffle) {
        // To catch the case when AQE enabled and there's no wrapped CustomShuffleReaderExec,
        // and don't call replaceWithColumnarPlan because ShuffleQueryStageExec is a leaf node
        CoalesceBatchesExec(plan)
      } else {
        plan
      }

    case plan: CustomShuffleReaderExec if columnarConf.enableColumnarShuffle =>
      plan.child match {
        case shuffle: ColumnarShuffleExchangeExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs, plan.description))
        case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeExec) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs, plan.description))
        case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
          reused match {
            case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeExec) =>
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
      p.withNewChildren(children)
  }

  def fallBackBroadcastQueryStage(curPlan: BroadcastQueryStageExec): BroadcastQueryStageExec = {
    curPlan.plan match {
      case originalBroadcastPlan: ColumnarBroadcastExchangeExec =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            DataToArrowColumnarExec(originalBroadcastPlan, 1)))
      case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeExec) =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            DataToArrowColumnarExec(curPlan.plan, 1)))
      case _ =>
        curPlan
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

}

case class ColumnarPostOverrides(conf: SparkConf) extends Rule[SparkPlan] {
  val columnarConf = ColumnarPluginConfig.getConf(conf)

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      RowToArrowColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeExec)
        if SQLConf.get.adaptiveExecutionEnabled && columnarConf.enableColumnarShuffle =>
      // When AQE enabled, we need to discard ColumnarToRowExec to avoid extra transactions
      // if ColumnarShuffleExchangeExec is the last plan of the query stage.
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithColumnarPlan(child.child)))
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }

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

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      val tmpPlan = rowGuardOverrides(plan)
      preOverrides(tmpPlan)
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
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
    logWarning(
      "Installing extensions to enable columnar CPU support." +
        " To disable this set `org.apache.spark.example.columnar.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}
