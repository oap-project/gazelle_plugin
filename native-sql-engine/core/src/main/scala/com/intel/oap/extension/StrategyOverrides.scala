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
import com.intel.oap.execution.LocalPhysicalWindow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSessionExtensions, Strategy, execution}
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, JoinedRow, NamedExpression, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow, WindowFunctionType}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalWindow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan, joins}
import org.apache.spark.sql.execution.window.WindowExecBase


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

case class LocalWindowExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
    extends WindowExecBase {

  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    super.requiredChildDistribution
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = {
    // todo implement this to fall back
    throw new UnsupportedOperationException()
  }

  protected def withNewChildInternal(newChild: SparkPlan):
  LocalWindowExec =
    copy(child = newChild)
}

object LocalWindowApply extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LocalPhysicalWindow(
    WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
      LocalWindowExec(
        windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

    case LocalPhysicalWindow(
    WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
      Nil // python window not supported

    case _ => Nil
  }
}

object StrategyOverrides extends GazelleSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => JoinSelectionOverrides)
    extensions.injectPlannerStrategy(_ => LocalWindowApply)
  }
}
