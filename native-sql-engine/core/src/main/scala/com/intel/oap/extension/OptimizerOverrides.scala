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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, Rank, SortOrder, WindowExpression, WindowFunctionType}
import org.apache.spark.sql.catalyst.planning.PhysicalWindow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

import java.util

/**
 * This contains workarounds to vanilla spark's restrictions extending
 * optimizer rule sets. An ideal implementation may start from SQL parser
 * to separate window using similar way as local/global limit were created.
 * Also, the rules around specific pattern matching of logical.Window may
 * need to be tuned as it's possible that we are adding LocalWindow, GlobalWindow
 * operators.
 */
object LocalRankWindow extends Rule[LogicalPlan] with SQLConfHelper {
  val LOCAL_WINDOW_COLUMN_SUFFIX = "<>local"
  val GLOBAL_WINDOW_COLUMN_SUFFIX = "<>global"

  // rank->filter to rank(local)->filter->rank-filter
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      // up
      case p @ RankFilterPattern(filterCond, matchedWindow, filteredRankColumns) =>
        p.transformDown {
          // down
          case w @ PhysicalWindow(WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec,
          windowChild) =>
            if (w eq matchedWindow) {
              val innerWindow = Window(windowExprs.map(expr => expr.transformDown {
                case alias: Alias =>
                  if (filteredRankColumns.contains(alias.name)) {
                    Alias(alias.child, toLocalWindowColumnName(alias.name))()
                  } else {
                    alias
                  }
                case other => other
              }.asInstanceOf[NamedExpression]), partitionSpec, orderSpec, windowChild)

              val innerFilter = Filter(filterCond.transformDown {
                case attr: AttributeReference =>
                  if (filteredRankColumns.contains(attr.name)) {
                    val windowOutAttr = innerWindow.output
                        .find(windowAttr => windowAttr.name ==
                            toLocalWindowColumnName(attr.name)).get
                    windowOutAttr.toAttribute
                  } else {
                    attr
                  }
                case other => other
              }, innerWindow)

              val originalOutputAttributes = new util.HashMap[String, Alias]()

              val outerWindow = Window(windowExprs.map {
                expr => expr.transformDown {
                  case alias: Alias =>
                    if (filteredRankColumns.contains(alias.name)) {
                      val globalName = toGlobalWindowColumnName(alias.name)
                      val globalAlias = Alias(alias.child, globalName)()
                      originalOutputAttributes.put(globalName, alias)
                      globalAlias
                    } else {
                      alias
                    }
                  case other => other
                }.asInstanceOf[NamedExpression]
              }, partitionSpec, orderSpec,
                Project(innerFilter.output.flatMap {
                  attr: Attribute =>
                    if (isLocalWindowColumnName(attr.name)) {
                      None
                    } else {
                      Some(attr)
                    }
                }, innerFilter))
              Project(outerWindow.output.map {
                attr: Attribute =>
                  if (isGlobalWindowColumnName(attr.name)) {
                    val restoredAlias = originalOutputAttributes.get(attr.name)
                    Alias(attr, restoredAlias.name)(restoredAlias.exprId,
                      restoredAlias.qualifier, restoredAlias.explicitMetadata,
                      restoredAlias.nonInheritableMetadataKeys)
                  } else {
                    attr
                  }
              }, outerWindow)
            } else {
              w
            }
          case plan => plan
        }
      case other @ _ => other
    }
  }

  def toLocalWindowColumnName(col: String): String = {
    col + LOCAL_WINDOW_COLUMN_SUFFIX
  }

  def isLocalWindowColumnName(col: String): Boolean = {
    col.endsWith(LOCAL_WINDOW_COLUMN_SUFFIX)
  }

  def toGlobalWindowColumnName(col: String): String = {
    col + GLOBAL_WINDOW_COLUMN_SUFFIX
  }

  def isGlobalWindowColumnName(col: String): Boolean = {
    col.endsWith(GLOBAL_WINDOW_COLUMN_SUFFIX)
  }
}

object RankFilterPattern {
  // filterExpression, window relation, filtered rank column name
  private type ReturnType = (Expression, Window, Seq[String])

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
              if (LocalRankWindow.isLocalWindowColumnName(a.name) ||
                  LocalRankWindow.isGlobalWindowColumnName(a.name)) {
                // already optimized
                (Nil, None)
              } else {
                (Seq(Some(aliasName)), Some(w))
              }
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
      val filteredRankColumns: Seq[String] = rankColumns.flatten.flatMap { col =>
        val isDesiredPattern = cond match {
          // todo rk < 100 && xxx ?
          case lt@LessThan(l, r) =>
            isColumnReference(l, col) && isLiteral(r)
          case lte@LessThanOrEqual(l, r) =>
            isColumnReference(l, col) && isLiteral(r)
          case gt@GreaterThan(l, r) =>
            isColumnReference(r, col) && isLiteral(l)
          case gt@GreaterThanOrEqual(l, r) =>
            isColumnReference(r, col) && isLiteral(l)
          case _ =>
            false
        }
        if (isDesiredPattern) Some(col) else None
      }

      if (filteredRankColumns.nonEmpty) {
        Some(cond, window.get, filteredRankColumns)
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
