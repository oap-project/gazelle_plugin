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

package com.intel.oap.expression

import com.google.common.collect.Lists
import org.apache.arrow.gandiva.expression.{TreeBuilder, TreeNode}
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, RegExpReplace}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.ArrowUtils


class ColumnarRegExpReplace(child: Expression, regexp: Expression, rep: Expression,
                            pos: Expression, original: Expression) extends RegExpReplace(
  child: Expression, regexp: Expression, rep: Expression, pos: Expression) with ColumnarExpression
    with Logging {
  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(
      StringType
    )
    if (supportedTypes.indexOf(child.dataType) == -1) {
      throw new UnsupportedOperationException(
        s"${child.dataType} is not supported in ColumnarRegExpReplace.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (regexp_node, regexpType): (TreeNode, ArrowType) =
      regexp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (rep_node, repType): (TreeNode, ArrowType) =
      rep.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (pos_node, posType): (TreeNode, ArrowType) =
      pos.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = ArrowUtils.toArrowType(StringType,
      SparkSchemaUtils.getLocalTimezoneID())

    val funcNode =
      TreeBuilder.makeFunction("regexp_replace", Lists.newArrayList(child_node,
        regexp_node, rep_node, pos_node), resultType)
    (funcNode, resultType)
  }
}

object ColumnarQuaternaryOperator {

  def create(value: Expression,
             original: Expression): Expression = original match {
    case a: RegExpReplace =>
      new ColumnarRegExpReplace(value, a.regexp, a.rep, a.pos, original)

    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}