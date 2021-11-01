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

import java.util.Objects

import com.intel.oap.GazelleSparkExtensionsInjector

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.catalyst.rules.Rule


object LocalRankWindow extends Rule[LogicalPlan] with SQLConfHelper {
  // rank->filter to rank(local)->filter->rank-filter
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      // up
      case p @ RankFilterPattern(filterCond, matchedWindow) =>
        p.transformDown {
          // down
          case w @ Window(windowExprs, partitionSpec, orderSpec, windowChild) =>
            if (w eq matchedWindow) {
              Window(windowExprs, partitionSpec, orderSpec,
                Filter(filterCond,
                  LocalizedWindow(windowExprs, partitionSpec, orderSpec, windowChild)))
            } else {
              w
            }
          case plan => plan
        }
      case other @ _ => other
    }
  }
}

case class LocalizedWindow(
    windowExpressions: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] =
    child.output ++ windowExpressions.map(_.toAttribute)

  def windowOutputSet: AttributeSet = AttributeSet(windowExpressions.map(_.toAttribute))
}

object RankFilterPattern {
  // filterExpression, window relation
  private type ReturnType = (Expression, Window)

  def getRankColumns(plan: LogicalPlan): (Seq[Option[String]], Option[Window]) = {
    plan match {
      case p @ Project(expressions, child) =>
        val nameMapping = new java.util.HashMap[String, String]()
        expressions.foreach {
          case ar @ AttributeReference(n, _, _, _) =>
            nameMapping.put(n, n)
          // todo alias
          case _ =>
        }
        val tuple = getRankColumns(child)
        (tuple._1.map(c => Some(nameMapping.get(c))), tuple._2)
      case s @ SubqueryAlias(identifier, child) =>
        getRankColumns(child)
      case w @ Window(windowExpressions, partitionSpec, orderSpec, child) =>
        if (w.windowExpressions.size != 1) {
          (Nil, None)
        } else {
          w.windowExpressions.head.collectFirst {
            case a @ Alias(WindowExpression(Rank(children), _), aliasName) =>
              (Seq(Some(aliasName)), Some(w))
          }.getOrElse((Nil, None))
        }
      case _ => (Nil, None)
    }
  }

  def isColumnReference(expr: Expression, col: String): Boolean = {
    expr match {
      case attr: AttributeReference =>
        Objects.equals(attr.name, col)
      case _ =>
        false
    }
  }

  def isLiteral(expr: Expression): Boolean = {
    expr match {
      case lit: Literal => true
      case _ => false
    }
  }

  def unapply(a: Any): Option[ReturnType] = a match {
    case f @ Filter(cond, child) =>
      val (rankColumns, window) = getRankColumns(f.child)
      if (rankColumns.flatten.exists { col =>
        cond match {
          case lt @ LessThan(l, r) =>
            isColumnReference(l, col) && isLiteral(r)
          case lte @ LessThanOrEqual(l, r) =>
            isColumnReference(l, col) && isLiteral(r)
          case gt @ GreaterThan(l, r) =>
            isColumnReference(r, col) && isLiteral(l)
          case gt @ GreaterThanOrEqual(l, r) =>
            isColumnReference(r, col) && isLiteral(l)
          case _ =>
            false
        }
      }) {
        Some(cond, window.get)
      } else {
        None
      }
    case _ =>
      None
  }
}

object OptimizerOverrides extends GazelleSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => LocalRankWindow)
    // not in use for now
  }
}
