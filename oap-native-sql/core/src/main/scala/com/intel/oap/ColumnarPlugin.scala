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

import java.util.Locale

import java.io.{File, BufferedReader, InputStreamReader};
import java.nio.file.Files;
import com.intel.oap.execution._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.adaptive.{
  BroadcastQueryStageExec,
  ColumnarCustomShuffleReaderExec,
  CustomShuffleReaderExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{
  BroadcastExchangeExec,
  ReusedExchangeExec,
  ShuffleExchangeExec
}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import java.io.IOException

case class ColumnarPreOverrides(conf: SparkConf) extends Rule[SparkPlan] {
  val columnarConf = ColumnarPluginConfig.getConf(conf)

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarBatchScanExec(plan.output, plan.scan)
    case plan: ProjectExec =>
      //new ColumnarProjectExec(plan.projectList, replaceWithColumnarPlan(plan.child))
      val columnarPlan = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      var newPlan: SparkPlan = null
      try {
        // If some expression is not supported, we will use RowBased HashAggr here.
        val newColumnarPlan = if (!columnarPlan.isInstanceOf[ColumnarConditionProjectExec]) {
          new ColumnarConditionProjectExec(null, plan.projectList, columnarPlan)
        } else {
          val cur_plan = columnarPlan.asInstanceOf[ColumnarConditionProjectExec]
          new ColumnarConditionProjectExec(cur_plan.condition, plan.projectList, cur_plan.child)
        }
        newPlan = newColumnarPlan
      } catch {
        case e: UnsupportedOperationException =>
          System.out.println(s"Fall back to use RowBased Filter and Project Exec")
      }
      if (newPlan == null) {
        if (columnarPlan.isInstanceOf[ColumnarConditionProjectExec]) {
          val planBeforeFilter = columnarPlan.children.map(replaceWithColumnarPlan)
          plan.child.withNewChildren(planBeforeFilter)
        } else {
          plan.withNewChildren(List(columnarPlan))
        }
      } else {
        newPlan
      }
    case plan: FilterExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarConditionProjectExec(plan.condition, null, child)
    case plan: HashAggregateExec =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      // If some expression is not supported, we will use RowBased HashAggr here.
      var newPlan: SparkPlan = plan.withNewChildren(children)
      try {
        val columnarPlan = new ColumnarHashAggregateExec(
          plan.requiredChildDistributionExpressions,
          plan.groupingExpressions,
          plan.aggregateExpressions,
          plan.aggregateAttributes,
          plan.initialInputBufferOffset,
          plan.resultExpressions,
          children(0))
        newPlan = columnarPlan
      } catch {
        case e: UnsupportedOperationException =>
          System.out.println(s"Fall back to use HashAggregateExec, error is ${e.getMessage()}")
      }
      newPlan
    case plan: SortExec =>
      if (columnarConf.enableColumnarSort) {
        val child = replaceWithColumnarPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      } else {
        val children = applyChildrenWithStrategy(plan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }
    case plan: ShuffleExchangeExec =>
      if (columnarConf.enableColumnarShuffle) {
        val child = replaceWithColumnarPlan(plan.child)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        if (SQLConf.get.adaptiveExecutionEnabled) {
          val exchange =
            new ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child,
              plan.canChangeNumPartitions)
          if (ColumnarShuffleExchangeExec.exchanges.contains(plan)) {
            logWarning(s"Found not reused ColumnarShuffleExchange " + exchange.treeString)
          }
          ColumnarShuffleExchangeExec.exchanges.update(plan, exchange)
          exchange
        } else {
          CoalesceBatchesExec(
            new ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child,
              plan.canChangeNumPartitions))
        }
      } else {
        val children = applyChildrenWithStrategy(plan)
        //val children = plan.children.map(replaceWithColumnarPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }
    case plan: ShuffledHashJoinExec =>
      if (!columnarConf.enablePreferColumnar) {
        val (doConvert, child) = optimizeJoin(0, plan)
        if (doConvert) {
          return child
        }
      }
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      // If some expression is not supported, we will use RowBased HashAggr here.
      var newPlan: SparkPlan = plan.withNewChildren(List(left, right))
      try {
        val columnarPlan = new ColumnarShuffledHashJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.buildSide,
          plan.condition,
          left,
          right)
        newPlan = columnarPlan
      } catch {
        case e: UnsupportedOperationException =>
          System.out.println(
            s"ColumnarShuffledHashJoinExec Fall back to use ShuffledHashJoinExec, error is ${e
              .getMessage()}")
      }
      newPlan
    case plan: BroadcastHashJoinExec =>
      if (!columnarConf.enablePreferColumnar) {
        val (doConvert, child) = optimizeJoin(0, plan)
        if (doConvert) {
          return child
        }
      }
      if (columnarConf.enableColumnarBroadcastJoin) {
        var (buildPlan, streamedPlan) = getJoinPlan(plan)
        val originalLeft = plan.left
        val originalRight = plan.right
        buildPlan = buildPlan match {
          case curPlan: BroadcastQueryStageExec =>
            fallBackBroadcastQueryStage(curPlan)
          case _ =>
            replaceWithColumnarPlan(buildPlan)
        }
        var newPlan = plan.buildSide match {
          case BuildLeft =>
            plan.withNewChildren(List(buildPlan, replaceWithColumnarPlan(streamedPlan)))
          case BuildRight =>
            plan.withNewChildren(List(replaceWithColumnarPlan(streamedPlan), buildPlan))
        }

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
        try {
          val columnarPlan = new ColumnarBroadcastHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right)
          newPlan = columnarPlan
        } catch {
          case e: UnsupportedOperationException =>
            System.out.println(
              s"ColumnarBroadcastHashJoinExec Fall back to use ShuffledHashJoinExec, error is ${e.getMessage()}")
        }
        newPlan
      } else {
        val children = applyChildrenWithStrategy(plan)
        logDebug(s"Columnar Processing for ${plan.getClass} is not currently supported.")
        plan.withNewChildren(children)
      }

    case plan: SortMergeJoinExec =>
      if (!columnarConf.enablePreferColumnar) {
        val (doConvert, child) = optimizeJoin(0, plan)
        if (doConvert) {
          return child
        }
      }
      if (columnarConf.enableColumnarSortMergeJoin && plan.condition == None) {
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val res = new ColumnarSortMergeJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
        res
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
      // To catch the case when AQE enabled and there's a wrapped CustomShuffleReaderExec,
      // and don't call replaceWithColumnarPlan on it's child because
      // the child must be the instance of ShuffleQueryStageExec, which is a leaf node
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      CoalesceBatchesExec(
        ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs, plan.description))

    case p =>
      val children = applyChildrenWithStrategy(p)
      logDebug(s"Columnar Processing for ${p.getClass} is currently not supported.")
      p.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

  def optimizeJoin(
      level: Int,
      plan: SparkPlan,
      mustDoConvert: Boolean = false): (Boolean, SparkPlan) = {
    plan match {
      case join: ShuffledHashJoinExec =>
        val (buildPlan, streamPlan) = getJoinPlan(join)
        val (doConvert, child) = optimizeJoin(level + 1, streamPlan)
        val newPlan = if (doConvert) {
          if (columnarConf.enableJoinOptimizationReplace) {
            val left = join.buildSide match {
              case BuildLeft =>
                SortExec(
                  join.leftKeys.map(expr => SortOrder(expr, Ascending)),
                  false,
                  replaceWithColumnarPlan(join.left))
              case BuildRight =>
                SortExec(join.leftKeys.map(expr => SortOrder(expr, Ascending)), false, child)
            }
            val right = join.buildSide match {
              case BuildLeft =>
                SortExec(join.rightKeys.map(expr => SortOrder(expr, Ascending)), false, child)
              case BuildRight =>
                SortExec(
                  join.rightKeys.map(expr => SortOrder(expr, Ascending)),
                  false,
                  replaceWithColumnarPlan(join.right))
            }
            SortMergeJoinExec(
              join.leftKeys,
              join.rightKeys,
              join.joinType,
              join.condition,
              left,
              right)
          } else {
            join.buildSide match {
              case BuildLeft =>
                join.withNewChildren(List(replaceWithColumnarPlan(buildPlan), child))
              case BuildRight =>
                join.withNewChildren(List(child, replaceWithColumnarPlan(buildPlan)))
            }
          }
        } else {
          join
        }
        (doConvert, newPlan)

      case join: BroadcastHashJoinExec =>
        var (buildPlan, streamPlan) = getJoinPlan(join)
        val (doConvert, child) = optimizeJoin(level + 1, streamPlan)
        val newPlan = if (doConvert) {
          buildPlan = buildPlan match {
            case curPlan: BroadcastQueryStageExec =>
              fallBackBroadcastQueryStage(curPlan)
            case _ =>
              replaceWithColumnarPlan(buildPlan)
          }
          join.buildSide match {
            case BuildLeft =>
              join.withNewChildren(List(buildPlan, child))
            case BuildRight =>
              join.withNewChildren(List(child, buildPlan))
          }
        } else {
          join
        }
        (doConvert, newPlan)

      case join: SortMergeJoinExec =>
        var (leftdoConvert, left) = optimizeJoin(level + 1, join.left)
        var (rightdoConvert, right) = optimizeJoin(level + 1, join.right, leftdoConvert)
        if (leftdoConvert != rightdoConvert) {
          val res = optimizeJoin(level + 1, join.left, rightdoConvert)
          left = res._2
        }
        val newPlan = if (rightdoConvert) {
          join.withNewChildren(List(left, right))
        } else {
          join
        }
        (rightdoConvert, newPlan)

      case project: ProjectExec =>
        val (doConvert, child) = optimizeJoin(level + 1, project.child)
        val newPlan = if (doConvert) {
          project.withNewChildren(List(child))
        } else {
          project
        }
        (doConvert, newPlan)

      case _ =>
        if (mustDoConvert || level >= columnarConf.joinOptimizationThrottle) {
          (true, replaceWithColumnarPlan(plan))
        } else {
          (false, plan)
        }
    }
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

  def getJoinPlan(join: HashJoin): (SparkPlan, SparkPlan) = {
    join.buildSide match {
      case BuildLeft =>
        (join.left, join.right)
      case BuildRight =>
        (join.right, join.left)
    }
  }

  def getJoinKeys(join: HashJoin): (Seq[Expression], Seq[Expression]) = {
    join.buildSide match {
      case BuildLeft =>
        (join.leftKeys, join.rightKeys)
      case BuildRight =>
        (join.rightKeys, join.leftKeys)
    }
  }

  def applyChildrenWithStrategy(p: SparkPlan): Seq[SparkPlan] = {
    if (columnarConf.enablePreferColumnar) {
      p.children.map(replaceWithColumnarPlan)
    } else {
      p.children.map(child =>
        child match {
          case project: ProjectExec =>
            val newChild = replaceWithColumnarPlan(project.child)
            if (newChild.supportsColumnar) {
              replaceWithColumnarPlan(child)
            } else {
              val newProject = project.withNewChildren(List(newChild))
              newProject
            }
          case filter: FilterExec =>
            val newChild = replaceWithColumnarPlan(filter.child)
            if (newChild.supportsColumnar) {
              replaceWithColumnarPlan(child)
            } else {
              val newFilter = filter.withNewChildren(List(newChild))
              newFilter
            }
          case aggr: HashAggregateExec =>
            val newChild = replaceWithColumnarPlan(aggr.child)
            if (newChild.supportsColumnar) {
              replaceWithColumnarPlan(child)
            } else {
              val newAggr = aggr.withNewChildren(List(newChild))
              newAggr
            }
          case _ =>
            replaceWithColumnarPlan(child)
        })
    }
  }
}

case class ColumnarPostOverrides(conf: SparkConf) extends Rule[SparkPlan] {
  val columnarConf = ColumnarPluginConfig.getConf(conf)

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      RowToArrowColumnarExec(child)
    case ReusedExchangeExec(id, s: ShuffleExchangeExec)
        if SQLConf.get.adaptiveExecutionEnabled && columnarConf.enableColumnarShuffle =>
      val exchange = ColumnarShuffleExchangeExec.exchanges.get(s) match {
        case Some(e) => e
        case None => throw new IllegalStateException("Reused exchange operator not found.")
      }
      ReusedExchangeExec(id, exchange)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeExec)
        if SQLConf.get.adaptiveExecutionEnabled && columnarConf.enableColumnarShuffle =>
      // When AQE enabled, we need to discard ColumnarToRowExec to avoid extra transactions
      // if ColumnarShuffleExchangeExec is the last plan of the query stage.
      replaceWithColumnarPlan(child)
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
  val preOverrides = ColumnarPreOverrides(conf)
  val postOverrides = ColumnarPostOverrides(conf)

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      preOverrides(plan)
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      postOverrides(plan)
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
