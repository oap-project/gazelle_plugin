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
import org.apache.spark.sql.catalyst.expressions.{Expression, StringTrim, StringTrimLeft, StringTrimRight}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.ArrowUtils

// StringTrim
class ColumnarStringTrim(srcStr: Expression, trimStr: Option[Expression], original: Expression)
    extends StringTrim(srcStr: Expression, trimStr: Option[Expression])
        with ColumnarExpression
        with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(
      StringType
    )
    // It is not supported to specify trimStr. By default, space is trimmed.
    if (supportedTypes.indexOf(srcStr.dataType) == -1 || !trimStr.isEmpty) {
      throw new UnsupportedOperationException(
        s"${srcStr.dataType} is not supported in ColumnarStringTrim.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (srcStr_node, srcStrType): (TreeNode, ArrowType) =
      srcStr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = ArrowUtils.toArrowType(StringType,
      SparkSchemaUtils.getLocalTimezoneID())
    val funcNode =
      TreeBuilder.makeFunction("btrim", Lists.newArrayList(srcStr_node), resultType)
    (funcNode, resultType)
  }
}

// StringTrimLeft
class ColumnarStringTrimLeft(srcStr: Expression, trimStr: Option[Expression], original: Expression)
    extends StringTrimLeft(srcStr: Expression, trimStr: Option[Expression])
        with ColumnarExpression
        with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(
      StringType
    )
    if (supportedTypes.indexOf(srcStr.dataType) == -1 || !trimStr.isEmpty) {
      throw new UnsupportedOperationException(
        s"${srcStr.dataType} is not supported in ColumnarStringTrimLeft.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (srcStr_node, srcStrType): (TreeNode, ArrowType) =
      srcStr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = ArrowUtils.toArrowType(StringType,
      SparkSchemaUtils.getLocalTimezoneID())
    val funcNode =
      TreeBuilder.makeFunction("ltrim", Lists.newArrayList(srcStr_node), resultType)
    (funcNode, resultType)
  }
}

// StringTrimRight
class ColumnarStringTrimRight(child: Expression, trimStr: Option[Expression], original: Expression)
    extends StringTrimRight(child: Expression, trimStr: Option[Expression])
        with ColumnarExpression
        with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(
      StringType
    )
    if (supportedTypes.indexOf(child.dataType) == -1 || !trimStr.isEmpty) {
      throw new UnsupportedOperationException(
        s"${child.dataType} is not supported in ColumnarStringTrimRight.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = ArrowUtils.toArrowType(StringType,
      SparkSchemaUtils.getLocalTimezoneID())
    val funcNode =
      TreeBuilder.makeFunction("rtrim", Lists.newArrayList(child_node), resultType)
    (funcNode, resultType)
  }
}

object ColumnarString2TrimOperator {

  def create(value: Seq[Expression],
             original: Expression): Expression = original match {
    case a: StringTrim =>
      new ColumnarStringTrim(value(0), Some(value(1)), a)
    case a: StringTrimLeft =>
      new ColumnarStringTrimLeft(value(0), Some(value(1)), a)
    case a: StringTrimRight =>
      new ColumnarStringTrimRight(value(0), Some(value(1)), a)

    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}