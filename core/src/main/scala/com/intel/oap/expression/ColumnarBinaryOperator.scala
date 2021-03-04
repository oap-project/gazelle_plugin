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
import com.intel.oap.ColumnarPluginConfig
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarAnd(left: Expression, right: Expression, original: Expression)
    extends And(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode = TreeBuilder.makeAnd(Lists.newArrayList(left_node, right_node))
    (funcNode, resultType)
  }
}

class ColumnarOr(left: Expression, right: Expression, original: Expression)
    extends Or(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode = TreeBuilder.makeOr(Lists.newArrayList(left_node, right_node))
    (funcNode, resultType)
  }
}

class ColumnarEndsWith(left: Expression, right: Expression, original: Expression)
    extends EndsWith(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("ends_with", Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarStartsWith(left: Expression, right: Expression, original: Expression)
    extends StartsWith(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("starts_with", Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarLike(left: Expression, right: Expression, original: Expression)
    extends Like(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("like", Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarContains(left: Expression, right: Expression, original: Expression)
    extends Contains(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (right_node, right_type): (TreeNode, ArrowType) =
      (TreeBuilder.makeStringLiteral(right.toString()), new ArrowType.Utf8())

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction("is_substr", Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarEqualTo(left: Expression, right: Expression, original: Expression)
    extends EqualTo(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "equal"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "equal_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction(function, Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarEqualNull(left: Expression, right: Expression, original: Expression)
    extends EqualNullSafe(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "equal"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "equal_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction(function, Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarLessThan(left: Expression, right: Expression, original: Expression)
    extends LessThan(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "less_than"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "less_than_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction(function, Lists.newArrayList(left_node, right_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarLessThanOrEqual(left: Expression, right: Expression, original: Expression)
    extends LessThanOrEqual(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "less_than_or_equal_to"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "less_than_or_equal_to_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode = TreeBuilder.makeFunction(
      function,
      Lists.newArrayList(left_node, right_node),
      resultType)
    (funcNode, resultType)
  }
}

class ColumnarGreaterThan(left: Expression, right: Expression, original: Expression)
    extends GreaterThan(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "greater_than"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "greater_than_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode = TreeBuilder.makeFunction(
      function,
      Lists.newArrayList(left_node, right_node),
      resultType)
    (funcNode, resultType)
  }
}

class ColumnarGreaterThanOrEqual(left: Expression, right: Expression, original: Expression)
    extends GreaterThanOrEqual(left: Expression, right: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val unifiedType = CodeGeneration.getResultType(left_type, right_type)
    (left_type, right_type) match {
      case (l: ArrowType.Decimal, r: ArrowType.Decimal) =>
      case _ =>
        if (!left_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          left_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(left_node), unifiedType)
        }
        if (!right_type.equals(unifiedType)) {
          val func_name = CodeGeneration.getCastFuncName(unifiedType)
          right_node =
            TreeBuilder.makeFunction(func_name, Lists.newArrayList(right_node), unifiedType)
        }
    }

    var function = "greater_than_or_equal_to"
    val nanCheck = ColumnarPluginConfig.getConf.enableColumnarNaNCheck
    if (nanCheck) {
      unifiedType match {
        case t: ArrowType.FloatingPoint =>
          function = "greater_than_or_equal_to_with_nan"
        case _ =>
      }
    }
    val resultType = new ArrowType.Bool()
    val funcNode = TreeBuilder.makeFunction(
      function,
      Lists.newArrayList(left_node, right_node),
      resultType)
    (funcNode, resultType)
  }
}

class ColumnarShiftLeft(left: Expression, right: Expression, original: Expression)
    extends ShiftLeft(left: Expression, right: Expression)
        with ColumnarExpression
        with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    if (right_type.getTypeID != ArrowTypeID.Int) {
      throw new IllegalArgumentException("shiftleft requires for int type on second parameter")
    }
    val resultType = left_type
    val funcNode = TreeBuilder.makeFunction(
      "shift_left",
      Lists.newArrayList(left_node, right_node),
      resultType)
    (funcNode, resultType)
  }
}

class ColumnarShiftRight(left: Expression, right: Expression, original: Expression)
    extends ShiftRight(left: Expression, right: Expression)
        with ColumnarExpression
        with Logging {
  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    var (left_node, left_type): (TreeNode, ArrowType) =
      left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    var (right_node, right_type): (TreeNode, ArrowType) =
      right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    if (right_type.getTypeID != ArrowTypeID.Int) {
      throw new IllegalArgumentException("shiftright requires for int type on second parameter")
    }
    val resultType = left_type
    val funcNode = TreeBuilder.makeFunction(
      "shift_right",
      Lists.newArrayList(left_node, right_node),
      resultType)
    (funcNode, resultType)
  }
}

object ColumnarBinaryOperator {

  def create(left: Expression, right: Expression, original: Expression): Expression = {
    buildCheck(left, right)
    original match {
      case a: And =>
        new ColumnarAnd(left, right, a)
      case o: Or =>
        new ColumnarOr(left, right, o)
      case e: EqualTo =>
        new ColumnarEqualTo(left, right, e)
      case e: EqualNullSafe =>
        new ColumnarEqualNull(left, right, e)
      case l: LessThan =>
        new ColumnarLessThan(left, right, l)
      case l: LessThanOrEqual =>
        new ColumnarLessThanOrEqual(left, right, l)
      case g: GreaterThan =>
        new ColumnarGreaterThan(left, right, g)
      case g: GreaterThanOrEqual =>
        new ColumnarGreaterThanOrEqual(left, right, g)
      case e: EndsWith =>
        new ColumnarEndsWith(left, right, e)
      case s: StartsWith =>
        new ColumnarStartsWith(left, right, s)
      case c: Contains =>
        new ColumnarContains(left, right, c)
      case l: Like =>
        new ColumnarLike(left, right, l)
      case s: ShiftLeft =>
        new ColumnarShiftLeft(left, right, s)
      case s: ShiftRight =>
        new ColumnarShiftRight(left, right, s)
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
        if (!left.dataType.isInstanceOf[DecimalType] ||
            !right.dataType.isInstanceOf[DecimalType]) {
          throw new UnsupportedOperationException(
            s"${left.dataType} or ${right.dataType} is not supported in ColumnarBinaryOperator")
        }
    }
  }
}
