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
import org.apache.spark.sql.{AnalysisException, SparkSessionExtensions, Strategy, execution}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType.{Python, SQL}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, PythonUDF, Rank, SortOrder, WindowFunction, WindowFunctionType}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalWindow}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins


object JoinSelectionOverrides extends Strategy with JoinSelectionHelper with SQLConfHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // targeting equi-joins only
    case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond, left, right, hint) =>
      if (getBroadcastBuildSide(left, right, joinType, hint, hintOnly = false, conf).isDefined) {
        return Nil
      }

      if (GazellePluginConfig.getSessionConf.forceShuffledHashJoin) {
        // Force use of ShuffledHashJoin in preference to SortMergeJoin. With no respect to
        // conf setting "spark.sql.join.preferSortMergeJoin".
        val leftBuildable = canBuildShuffledHashJoinLeft(joinType)
        val rightBuildable = canBuildShuffledHashJoinRight(joinType)
        if (!leftBuildable && !rightBuildable) {
          return Nil
        }
        val buildSide = if (!leftBuildable) {
          BuildRight
        } else if (!rightBuildable) {
          BuildLeft
        } else {
          getSmallerSide(left, right)
        }
        return Option(buildSide).map {
          buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }.getOrElse(Nil)
      }

      Nil
    case _ => Nil
  }
}

object LocalRankWindow extends Strategy with SQLConfHelper {
  // todo do this on window-filter pattern only
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case RankWindow(windowExprs, partitionSpec, orderSpec, child) =>
      execution.window.WindowExec(
        windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

    case _ => Nil
  }
}

object RankWindow {
  // windowFunctionType, windowExpression, partitionSpec, orderSpec, child
  private type ReturnType =
    (Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case expr @ logical.Window(windowExpressions, partitionSpec, orderSpec, child) =>
      if (windowExpressions.size != 1) {
        None
      } else {
        windowExpressions.head.collectFirst {
          case _: Rank =>
            Some((windowExpressions, partitionSpec, orderSpec, child))
          case _ =>
            None
        }.flatten
      }

    case _ => None
  }
}

object StrategyOverrides extends GazelleSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => JoinSelectionOverrides)
  }
}
