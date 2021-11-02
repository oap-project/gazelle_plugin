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

package org.apache.spark.sql.catalyst.analysis

import com.intel.oap.extension.LocalWindow
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, CollectMetrics, LogicalPlan, ObjectConsumer, ObjectProducer, Project, Window}
import org.apache.spark.sql.catalyst.rules.Rule

class GazelleAnalyzer {
  /**
   * Gazelle version of CleanupAliases.
   */
  object CleanupAliases extends Rule[LogicalPlan] {
    def trimAliases(e: Expression): Expression = {
      e.transformDown {
        case Alias(child, _) => child
        case MultiAlias(child, _) => child
      }
    }

    def trimNonTopLevelAliases(e: Expression): Expression = e match {
      case a: Alias =>
        a.copy(child = trimAliases(a.child))(
          exprId = a.exprId,
          qualifier = a.qualifier,
          explicitMetadata = Some(a.metadata),
          nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
      case a: MultiAlias =>
        a.copy(child = trimAliases(a.child))
      case other => trimAliases(other)
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Project(projectList, child) =>
        val cleanedProjectList =
          projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
        Project(cleanedProjectList, child)

      case Aggregate(grouping, aggs, child) =>
        val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
        Aggregate(grouping.map(trimAliases), cleanedAggs, child)

      case Window(windowExprs, partitionSpec, orderSpec, child) =>
        val cleanedWindowExprs =
          windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
        Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
          orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

      case LocalWindow(windowExprs, partitionSpec, orderSpec, child) =>
        val cleanedWindowExprs =
          windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
        LocalWindow(cleanedWindowExprs, partitionSpec.map(trimAliases),
          orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

      case CollectMetrics(name, metrics, child) =>
        val cleanedMetrics = metrics.map {
          e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression]
        }
        CollectMetrics(name, cleanedMetrics, child)

      // Operators that operate on objects should only have expressions from encoders, which should
      // never have extra aliases.
      case o: ObjectConsumer => o
      case o: ObjectProducer => o
      case a: AppendColumns => a

      case other =>
        other transformExpressionsDown {
          case Alias(child, _) => child
        }
    }
  }
}
