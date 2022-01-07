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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * A version of substring that supports columnar processing for utf8.
 */
class ColumnarSubString(str: Expression, pos: Expression, len: Expression, original: Expression)
    extends Substring(str: Expression, pos: Expression, len: Expression)
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    if (str.dataType != StringType) {
      throw new UnsupportedOperationException(
        s"${str.dataType} is not supported in ColumnarSubString")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (str_node, strType): (TreeNode, ArrowType) =
      str.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (pos_node, posType): (TreeNode, ArrowType) =
      pos.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (len_node, lenType): (TreeNode, ArrowType) =
      len.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    //FIXME(): gandiva only support pos and len with int64 type
    val lit_pos :ColumnarLiteral= pos.asInstanceOf[ColumnarLiteral]
    val lit_pos_val = lit_pos.value
    val long_pos_node = TreeBuilder.makeLiteral(lit_pos_val.asInstanceOf[Integer].longValue() :java.lang.Long)

    val lit_len :ColumnarLiteral= len.asInstanceOf[ColumnarLiteral]
    val lit_len_val = lit_len.value
    val long_len_node = TreeBuilder.makeLiteral(lit_len_val.asInstanceOf[Integer].longValue() :java.lang.Long)

    val resultType = new ArrowType.Utf8()
    val funcNode =
      TreeBuilder.makeFunction("substr", Lists.newArrayList(str_node, long_pos_node, long_len_node), resultType)
    (funcNode, resultType)
  }
}

// StringSplit, not functionality ready, need array type support.
class ColumnarStringSplit(child: Expression, regex: Expression,
                          limit: Expression, original: Expression)
    extends StringSplit(child: Expression,
      regex: Expression, limit: Expression)
        with ColumnarExpression
        with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(
      StringType
    )
    if (supportedTypes.indexOf(child.dataType) == -1) {
      throw new UnsupportedOperationException(
        s"${child.dataType} is not supported in ColumnarStringSplit.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object)
  : (TreeNode, ArrowType) = {
    val (child_node, childType): (TreeNode, ArrowType) =
      child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (regex_node, regexType): (TreeNode, ArrowType) =
      regex.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (limit_node, limitType): (TreeNode, ArrowType) =
      limit.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    val funcNode =
      TreeBuilder.makeFunction(
        "split_part", Lists.newArrayList(child_node, regex_node,
          limit_node), resultType)
    (funcNode, resultType)
  }
}

class ColumnarStringTranslate(src: Expression, matchingExpr: Expression,
                              replaceExpr: Expression, original: Expression)
    extends StringTranslate(src, matchingExpr, replaceExpr) with ColumnarExpression{
  buildCheck

  def buildCheck: Unit = {
    val supportedTypes = List(StringType)
    if (supportedTypes.indexOf(src.dataType) == -1) {
      throw new UnsupportedOperationException(s"${src.dataType}" +
          s" is not supported in ColumnarStringTranslate!")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object) : (TreeNode, ArrowType) = {
    val (str_node, _): (TreeNode, ArrowType) =
      src.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (matchingExpr_node, _): (TreeNode, ArrowType) =
      matchingExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (replaceExpr_node, _): (TreeNode, ArrowType) =
      replaceExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val resultType = new ArrowType.Utf8()
    (TreeBuilder.makeFunction("translate",
      Lists.newArrayList(str_node, matchingExpr_node, replaceExpr_node), resultType), resultType)
  }
}

object ColumnarTernaryOperator {

  def create(src: Expression, arg1: Expression, arg2: Expression,
             original: Expression): Expression = original match {
    case ss: Substring =>
      new ColumnarSubString(src, arg1, arg2, ss)
      // Currently not supported.
//    case a: StringSplit =>
//      new ColumnarStringSplit(str, a.regex, a.limit, a)
    case st: StringTranslate =>
      new ColumnarStringTranslate(src, arg1, arg2, st)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
