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
package com.intel.oap.execution

import com.intel.oap.extension.LocalRankWindow
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, SortOrder, WindowFunctionType}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}

object LocalPhysicalWindow {
  // windowFunctionType, windowExpression, partitionSpec, orderSpec, child
  private type ReturnType =
    (WindowFunctionType, Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case expr @ Window(windowExpressions, partitionSpec, orderSpec, child) =>

      // The window expression should not be empty here, otherwise it's a bug.
      if (windowExpressions.isEmpty) {
        throw new IllegalArgumentException(s"Window expression is empty in $expr")
      }

      if (!windowExpressions.exists(expr => {
        expr.isInstanceOf[Alias] &&
            LocalRankWindow.isLocalWindowColumnName(expr.asInstanceOf[Alias].name)
      })) {
        return None
      }

      val windowFunctionType = windowExpressions.map(WindowFunctionType.functionType)
          .reduceLeft { (t1: WindowFunctionType, t2: WindowFunctionType) =>
            if (t1 != t2) {
              // We shouldn't have different window function type here, otherwise it's a bug.
              throw new IllegalArgumentException(
                s"Found different window function type in $windowExpressions")
            } else {
              t1
            }
          }

      Some((windowFunctionType, windowExpressions, partitionSpec, orderSpec, child))

    case _ => None
  }
}

object Patterns {

}
