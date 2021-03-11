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
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarAdd(left: Expression, right: Expression, original: Expression)
    extends Add(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
        val resultType = DecimalTypeUtil.getResultTypeForOperation(
          DecimalTypeUtil.OperationType.ADD, l, r)
        val addNode = TreeBuilder.makeFunction(
          "add", Lists.newArrayList(left_node, right_node), resultType)
        (addNode, resultType)
      case _ =>
        val resultType = CodeGeneration.getResultType(left_type, right_type)
        if (!left_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType)
        }
        if (!right_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType)
        }
        //logInfo(s"(TreeBuilder.makeFunction(add, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
        val funcNode = TreeBuilder.makeFunction(
          "add", Lists.newArrayList(left_node, right_node), resultType)
        (funcNode, resultType)
    }
  }
}

class ColumnarSubtract(left: Expression, right: Expression, original: Expression)
    extends Subtract(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
        val resultType = DecimalTypeUtil.getResultTypeForOperation(
          DecimalTypeUtil.OperationType.SUBTRACT, l, r)
        val subNode = TreeBuilder.makeFunction(
          "subtract", Lists.newArrayList(left_node, right_node), resultType)
        (subNode, resultType)
      case _ =>
        val resultType = CodeGeneration.getResultType(left_type, right_type)
        if (!left_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType)
        }
        if (!right_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType)
        }
        //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
        val funcNode = TreeBuilder.makeFunction(
          "subtract", Lists.newArrayList(left_node, right_node), resultType)
        (funcNode, resultType)
    }
  }
}

class ColumnarMultiply(left: Expression, right: Expression, original: Expression)
    extends Multiply(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
        val resultType = DecimalTypeUtil.getResultTypeForOperation(
          DecimalTypeUtil.OperationType.MULTIPLY, l, r)
        val mulNode = TreeBuilder.makeFunction(
          "multiply", Lists.newArrayList(left_node, right_node), resultType)
        (mulNode, resultType)
      case _ =>
        val resultType = CodeGeneration.getResultType(left_type, right_type)
        if (!left_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
        }
        if (!right_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
        }
        //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
        val funcNode = TreeBuilder.makeFunction(
          "multiply", Lists.newArrayList(left_node, right_node), resultType)
        (funcNode, resultType)
    }
  }
}

class ColumnarDivide(left: Expression, right: Expression, original: Expression)
    extends Divide(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
        val resultType = DecimalTypeUtil.getResultTypeForOperation(
          DecimalTypeUtil.OperationType.DIVIDE, l, r)
        val divNode = TreeBuilder.makeFunction(
          "divide", Lists.newArrayList(left_node, right_node), resultType)
        (divNode, resultType)
      case _ =>
        val resultType = CodeGeneration.getResultType(left_type, right_type)
        if (!left_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), resultType),
        }
        if (!right_type.equals(resultType)) {
          val func_name = CodeGeneration.getCastFuncName(resultType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), resultType),
        }
        //logInfo(s"(TreeBuilder.makeFunction(multiply, Lists.newArrayList($left_node, $right_node), $resultType), $resultType)")
        val funcNode = TreeBuilder.makeFunction(
          "divide", Lists.newArrayList(left_node, right_node), resultType)
        (funcNode, resultType)
    }
  }
}

class ColumnarBitwiseAnd(left: Expression, right: Expression, original: Expression)
    extends BitwiseAnd(left: Expression, right: Expression)
        with ColumnarExpression
        with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)

    val funcNode = TreeBuilder.makeFunction(
      "bitwise_and",
      Lists.newArrayList(left_node, right_node),
      unifiedType)
    (funcNode, unifiedType)
  }
}


class ColumnarBitwiseOr(left: Expression, right: Expression, original: Expression)
    extends BitwiseOr(left: Expression, right: Expression)
        with ColumnarExpression
        with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    val funcNode = TreeBuilder.makeFunction(
      "bitwise_or",
      Lists.newArrayList(left_node, right_node),
      unifiedType)
    (funcNode, unifiedType)
  }
}

class ColumnarBitwiseXor(left: Expression, right: Expression, original: Expression)
    extends BitwiseXor(left: Expression, right: Expression)
        with ColumnarExpression
        with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)

    val funcNode = TreeBuilder.makeFunction(
      "bitwise_xor",
      Lists.newArrayList(left_node, right_node),
      unifiedType)
    (funcNode, unifiedType)
  }
}

object ColumnarBinaryArithmetic {

  def create(left: Expression, right: Expression, original: Expression): Expression = {
      buildCheck(left, right)
      original match {
      case a: Add =>
        new ColumnarAdd(left, right, a)
      case s: Subtract =>
        new ColumnarSubtract(left, right, s)
      case m: Multiply =>
        new ColumnarMultiply(left, right, m)
      case d: Divide =>
        new ColumnarDivide(left, right, d)
      case a: BitwiseAnd =>
        new ColumnarBitwiseAnd(left, right, a)
      case o: BitwiseOr =>
        new ColumnarBitwiseOr(left, right, o)
      case x: BitwiseXor =>
        new ColumnarBitwiseXor(left, right, x)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  def buildCheck(left: Expression, right: Expression): Unit = {
    try {
      ConverterUtils.checkIfTypeSupported(left.dataType)
      ConverterUtils.checkIfTypeSupported(right.dataType)
    } catch {
      case e : UnsupportedOperationException =>
        throw new UnsupportedOperationException(
          s"${left.dataType} or ${right.dataType} is not supported in ColumnarBinaryArithmetic")
    }
  }
}
