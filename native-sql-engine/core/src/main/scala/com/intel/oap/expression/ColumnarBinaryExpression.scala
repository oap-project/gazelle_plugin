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
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

import com.intel.oap.expression.ColumnarDateTimeExpressions.ColumnarDateDiff
import com.intel.oap.expression.ColumnarDateTimeExpressions.ColumnarDateSub
import com.intel.oap.expression.ColumnarDateTimeExpressions.ColumnarUnixTimestamp
import com.intel.oap.expression.ColumnarDateTimeExpressions.ColumnarFromUnixTime
import com.intel.oap.expression.ColumnarDateTimeExpressions.ColumnarGetTimestamp

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarDateAddInterval(start: Expression, interval: Expression, original: DateAddInterval)
    extends DateAddInterval(start, interval, original.timeZoneId, original.ansiEnabled)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = left_type
    if (right_type.getTypeID != ArrowTypeID.Interval) {
      throw new IllegalArgumentException(
        "ColumnarDateAddInterval requires for Interval type on second parameter")
    }
    right_type.asInstanceOf[ArrowType.Interval].getUnit match {
      case IntervalUnit.DAY_TIME =>
        (
          TreeBuilder.makeFunction("add", Lists.newArrayList(left_node, right_node), resultType),
          resultType)
      case IntervalUnit.YEAR_MONTH =>
        (
          TreeBuilder.makeFunction(
            "dateAddYearMonthInterval",
            Lists.newArrayList(left_node, right_node),
            resultType),
          resultType)
      case _ =>
        throw new IllegalArgumentException(
          "ColumnarDateAddInterval only support interval type as DATE_TIME or YEAR_MONTH")
    }

  }
}

class ColumnarGetJsonObject(left: Expression, right: Expression, original: GetJsonObject)
    extends GetJsonObject(original.json, original.path)
    with ColumnarExpression
    with Logging {

  buildCheck

  // Only literal json path is supported and wildcard is not supported.
  def buildCheck: Unit = {
    right match {
      case literal: ColumnarLiteral =>
        val jsonPath = literal.value.toString
        if (jsonPath.contains("*")) {
          throw new UnsupportedOperationException("Wildcard is NOT supported" +
            " in json path for get_json_object.")
        }
      case _ =>
        throw new UnsupportedOperationException("Only literal json path is supported!")
    }
  }

  // TODO: currently we have a codegen implementation, but needs to be optimized.
  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(dataType)
    val funcNode = TreeBuilder.makeFunction("get_json_object",
      Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarStringInstr(left: Expression, right: Expression, original: StringInstr)
    extends StringInstr(original.str, original.substr) with ColumnarExpression with Logging {

  val gName = "locate"

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    codegenFuncList.contains(gName) && 
    left.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args) &&
    right.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (left_node, _): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, _): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val resultType = CodeGeneration.getResultType(dataType)
    // Be careful about the argument order.
    val funcNode = TreeBuilder.makeFunction("locate",
      Lists.newArrayList(right_node, left_node,
        TreeBuilder.makeLiteral(1.asInstanceOf[java.lang.Integer])), resultType)
    (funcNode, resultType)
  }
}

class ColumnarPow(left: Expression, right: Expression, original: Pow) extends Pow(left, right)
  with ColumnarExpression with Logging {

  override def supportColumnarCodegen(args: Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (leftNode, _): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (rightNode, _): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val resultType = CodeGeneration.getResultType(dataType)
    val funcNode =
      TreeBuilder.makeFunction("pow", Lists.newArrayList(leftNode, rightNode), resultType)
    (funcNode, resultType)
  }
}

class ColumnarFindInSet(left: Expression, right: Expression, original: Expression)
  extends FindInSet(left: Expression, right: Expression) with ColumnarExpression with Logging {

  override def supportColumnarCodegen(args: Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (leftNode, _): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (rightNode, _): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Int(32, true)
    val funcNode = TreeBuilder.makeFunction("find_in_set",
      Lists.newArrayList(leftNode, rightNode), resultType)
    (funcNode, resultType)
  }
}

class ColumnarSha2(left: Expression, right: Expression) extends Sha2(left, right)
  with ColumnarExpression with Logging {

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (leftNode, _): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (rightNode, _): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val resultType = new ArrowType.Utf8()
    val funcNode = TreeBuilder.makeFunction("sha2",
      Lists.newArrayList(leftNode, rightNode), resultType)
    (funcNode, resultType)
  }
}

object ColumnarBinaryExpression {

  def create(left: Expression, right: Expression, original: Expression): Expression =
    original match {
      case s: DateAddInterval =>
        new ColumnarDateAddInterval(left, right, s)
      case s: DateDiff =>
        new ColumnarDateDiff(left, right)
      case a: UnixTimestamp =>
        new ColumnarUnixTimestamp(left, right, a.timeZoneId, a.failOnError)
      // To match GetTimestamp (a private class).
      case _ if (original.isInstanceOf[ToTimestamp] && original.dataType == TimestampType) =>
        // Convert a string to Timestamp. Default timezone is used.
        new ColumnarGetTimestamp(left, right, None)
      case a: FromUnixTime =>
        new ColumnarFromUnixTime(left, right, a.timeZoneId)
      case d: DateSub =>
        new ColumnarDateSub(left, right)
      case g: GetJsonObject =>
         new ColumnarGetJsonObject(left, right, g)
      case instr: StringInstr =>
        new ColumnarStringInstr(left, right, instr)
      case pow: Pow =>
        new ColumnarPow(left, right, pow)
      case f: FindInSet =>
        new ColumnarFindInSet(left, right, f)
      case _: Sha2 =>
        new ColumnarSha2(left, right)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
