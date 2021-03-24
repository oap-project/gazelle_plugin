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

import java.lang

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

class ColumnarInSet(value: Expression, hset: Set[Any], original: Expression)
    extends InSet(value: Expression, hset: Set[Any])
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(StringType, IntegerType, LongType)
    if (supportedTypes.indexOf(value.dataType) == -1) {
      throw new UnsupportedOperationException(
        s"${value.dataType} is not supported in ColumnarInSet.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (value_node, valueType): (TreeNode, ArrowType) =
      value.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    var inNode: TreeNode = null
    var has_null = false

    if (value.dataType == StringType) {
      var newlist: List[String] = List()
      val hList = hset.toList
      for (i <- hList.indices) {
        if (hList(i) == null) {
          has_null = true
        } else {
          hList(i) match {
            case expr@(str: UTF8String) =>
              newlist = newlist :+ expr.toString
            case literal: Literal =>
              newlist = newlist :+ value.toString
          }
        }
      }
      val tlist = Lists.newArrayList(newlist:_*);
      inNode = TreeBuilder.makeInExpressionString(value_node, Sets.newHashSet(tlist))
    } else if (value.dataType == IntegerType) {
      var newlist: List[Integer] = List()
      val hList = hset.toList
      for (i <- hList.indices) {
        if (hList(i) == null) {
          has_null = true
        } else {
          hList(i) match {
            case integer: Integer =>
              newlist = newlist :+ integer
            case literal: Literal =>
              newlist = newlist :+ value.asInstanceOf[Integer]
          }
        }
      }
      val tlist = Lists.newArrayList(newlist:_*);
      inNode = TreeBuilder.makeInExpressionInt32(value_node: TreeNode, Sets.newHashSet(tlist))
    } else if (value.dataType == LongType) {
      var newlist: List[lang.Long] = List()
      val hList = hset.toList
      for (i <- hList.indices) {
        if (hList(i) == null) {
          has_null = true
        } else {
          hList(i) match {
            case long: lang.Long =>
              newlist = newlist :+ long
            case literal: Literal =>
              newlist = newlist :+ value.asInstanceOf[lang.Long]
          }
        }
      }
      val tlist = Lists.newArrayList(newlist:_*);
      inNode = TreeBuilder.makeInExpressionBigInt(value_node, Sets.newHashSet(tlist))
    } else {
      throw new UnsupportedOperationException(s"not currently supported: ${value.dataType}.")
    }

    /** Null should be specially handled:
    TRUE is returned when the non-NULL value in question is found in the list
    FALSE is returned when the non-NULL value is not found in the list and the list does not contain NULL values
    NULL is returned when the value is NULL, or the non-NULL value is not found in the list and the list contains at least one NULL value
     */
    val isnotnullNode = TreeBuilder.makeFunction(
      "isnotnull", Lists.newArrayList(value_node), resultType)
    val trueNode =
      TreeBuilder.makeLiteral(true.asInstanceOf[java.lang.Boolean])
    val falseNode =
      TreeBuilder.makeLiteral(false.asInstanceOf[java.lang.Boolean])
    val nullNode =
      TreeBuilder.makeNull(resultType)
    val hasNullNode =
      TreeBuilder.makeIf(
        TreeBuilder.makeLiteral(has_null.asInstanceOf[java.lang.Boolean]),
        trueNode, falseNode, resultType)

    val notInNode = TreeBuilder.makeIf(
      hasNullNode, nullNode, falseNode, resultType)
    val isNotNullBranch =
      TreeBuilder.makeIf(inNode, trueNode, notInNode, resultType)
    val funcNode = TreeBuilder.makeIf(
      isnotnullNode, isNotNullBranch, nullNode, resultType)
    (funcNode, resultType)
  }
}

object ColumnarInSetOperator {

  def create(value: Expression, hset: Set[Any], original: Expression): Expression = original match {
    case i: InSet =>
      new ColumnarInSet(value, hset, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
