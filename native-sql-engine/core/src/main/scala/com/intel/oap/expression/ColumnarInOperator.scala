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
import com.google.common.collect.Sets

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
class ColumnarIn(value: Expression, list: Seq[Expression], original: Expression)
    extends In(value: Expression, list: Seq[Expression])
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(StringType, IntegerType, LongType, DateType)
    if (supportedTypes.indexOf(value.dataType) == -1) {
      throw new UnsupportedOperationException(
        s"${value.dataType} is not supported in ColumnarIn.")
    }
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (value_node, valueType): (TreeNode, ArrowType) =
      value.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()

    if (value.dataType == StringType) {
      val newlist :List[String]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.toString
      });
      val tlist = Lists.newArrayList(newlist:_*);

      val funcNode = TreeBuilder.makeInExpressionString(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == IntegerType) {
      var has_null = false
      var newlist: List[Integer] = List()
      list.toList.foreach (expr => {
        val value = expr.asInstanceOf[Literal].value
        if (value != null) {
          newlist = newlist :+ value.asInstanceOf[Integer]
        } else {
          has_null = true
        }
      })
      val tlist = Lists.newArrayList(newlist:_*);
      val isnotnullNode = TreeBuilder.makeFunction(
        "isnotnull", Lists.newArrayList(value_node), resultType)
      val hasnull = TreeBuilder.makeLiteral(has_null.asInstanceOf[java.lang.Boolean])
      val trueNode = TreeBuilder.makeLiteral(true.asInstanceOf[java.lang.Boolean])
      val falseNode = TreeBuilder.makeLiteral(false.asInstanceOf[java.lang.Boolean])
      val nullNode = TreeBuilder.makeNull(resultType)

      val hasNullNode = TreeBuilder.makeIf(hasnull, trueNode, falseNode, resultType)
      val inNode = TreeBuilder.makeInExpressionInt32(value_node, Sets.newHashSet(tlist))
      val notInNode = TreeBuilder.makeIf(hasNullNode, nullNode, falseNode, resultType)

      val isNotNullBranch = TreeBuilder.makeIf(inNode, trueNode, notInNode, resultType)

      val funcNode = TreeBuilder.makeIf(isnotnullNode, isNotNullBranch, nullNode, resultType)
      (funcNode, resultType)
    } else if (value.dataType == LongType) {
      val newlist :List[java.lang.Long]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.asInstanceOf[java.lang.Long]
      });
      val tlist = Lists.newArrayList(newlist:_*);

      val funcNode = TreeBuilder.makeInExpressionBigInt(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == DateType) {
      val newlist :List[Integer]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.asInstanceOf[Integer]
      });
      val tlist = Lists.newArrayList(newlist:_*);
      val cast_func = TreeBuilder.makeFunction("castINT", Lists.newArrayList(value_node), new ArrowType.Int(32, true))

      val funcNode = TreeBuilder.makeInExpressionInt32(cast_func, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else {
      throw new UnsupportedOperationException(s"not currently supported: ${value.dataType}.")
    }
  }
}

object ColumnarInOperator {

  def create(value: Expression, list: Seq[Expression], original: Expression): Expression = original match {
    case i: In =>
      new ColumnarIn(value, list, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
