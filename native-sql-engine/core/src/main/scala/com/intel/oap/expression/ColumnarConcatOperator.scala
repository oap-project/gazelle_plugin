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

class ColumnarConcatWs(exps: Seq[Expression], original: Expression)
    extends ConcatWs(exps: Seq[Expression])
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    exps.foreach(expr =>
      if (expr.dataType != StringType) {
        throw new UnsupportedOperationException(
          s"${expr.dataType} is not supported in ColumnarConcatWS")
      })
  }

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    if (exps.size == 2) {
      // return 2nd param, ignore spliter
      val (exp_node, expType): (TreeNode, ArrowType) =
        exps.last.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      return (exp_node, expType)
    } else if (exps.size == 1) {
      // Corner case, only spliter provided
      return (TreeBuilder.makeNull(ArrowType.Utf8.INSTANCE), ArrowType.Utf8.INSTANCE)
    }

    val iter: Iterator[Expression] = exps.iterator
    val exp = iter.next() // spliter
    val exp1 = iter.next()
    val iterFaster: Iterator[Expression] = exps.iterator
    iterFaster.next()
    iterFaster.next()
    iterFaster.next()

    val (split_node, expType): (TreeNode, ArrowType) =
      exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (exp1_node, exp1Type): (TreeNode, ArrowType) =
      exp1.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Utf8()
    val funcNode = TreeBuilder.makeFunction("concat",
      Lists.newArrayList(exp1_node, split_node, rightNode(args, exps, split_node, iter, iterFaster)), resultType)
    (funcNode, expType)
  }

  def rightNode(args: java.lang.Object, exps: Seq[Expression], split_node: TreeNode,
                iter: Iterator[Expression], iterFaster: Iterator[Expression]): TreeNode = {
    if (!iterFaster.hasNext) {
      // When iter reaches the last but one expression
      val (exp_node, expType): (TreeNode, ArrowType) =
        exps.last.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      exp_node
    } else {
      val exp = iter.next()
      iterFaster.next()
      val (exp_node, expType): (TreeNode, ArrowType) =
        exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val resultType = new ArrowType.Utf8()
      val funcNode = TreeBuilder.makeFunction("concat",
        Lists.newArrayList(exp_node, split_node, rightNode(args, exps, split_node, iter, iterFaster)), resultType)
      funcNode
    }
  }
}

class ColumnarConcat(exps: Seq[Expression], original: Expression)
    extends Concat(exps: Seq[Expression])
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    exps.foreach(expr =>
      if (expr.dataType != StringType) {
        throw new UnsupportedOperationException(
          s"${expr.dataType} is not supported in ColumnarConcat")
      })
  }

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val iter: Iterator[Expression] = exps.iterator
    val exp = iter.next()
    val iterFaster: Iterator[Expression] = exps.iterator
    iterFaster.next()
    iterFaster.next()

    val (exp_node, expType): (TreeNode, ArrowType) =
      exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Utf8()
    val funcNode = TreeBuilder.makeFunction("concatOperator",
      Lists.newArrayList(exp_node, rightNode(args, exps, iter, iterFaster)), resultType)
    (funcNode, expType)
  }

  def rightNode(args: java.lang.Object, exps: Seq[Expression],
                iter: Iterator[Expression], iterFaster: Iterator[Expression]): TreeNode = {
    if (!iterFaster.hasNext) {
      // When iter reaches the last but one expression
      val (exp_node, expType): (TreeNode, ArrowType) =
        exps.last.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      exp_node
    } else {
      val exp = iter.next()
      iterFaster.next()
      val (exp_node, expType): (TreeNode, ArrowType) =
        exp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val resultType = new ArrowType.Utf8()
      val funcNode = TreeBuilder.makeFunction("concatOperator",
        Lists.newArrayList(exp_node, rightNode(args, exps, iter, iterFaster)), resultType)
      funcNode
    }
  }
}

object ColumnarConcatOperator {

  def create(exps: Seq[Expression], original: Expression): Expression = original match {
    case c: Concat =>
      new ColumnarConcat(exps, original)
    case cws: ConcatWs =>
      new ColumnarConcatWs(exps, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
