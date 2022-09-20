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
class ColumnarCaseWhen(
  branches: Seq[(Expression, Expression)], 
  elseValue: Option[Expression], 
  original: Expression, 
  rename: Boolean)
    extends CaseWhen(branches: Seq[(Expression, Expression)] ,elseValue: Option[Expression])
    with ColumnarExpression
    with Logging {

  buildCheck()

  def buildCheck(): Unit = {
    val exprs = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue
    exprs.foreach(expr => try {
        ConverterUtils.checkIfTypeSupported(expr.dataType)
      } catch {
        case e : UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${dataType} is not supported in ColumnarCaseWhen")
      })
  }

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    val exprs = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue
    val exprList = { exprs.filter(expr => !expr.isInstanceOf[Literal]) }
    !exprList.map(expr => expr.asInstanceOf[ColumnarExpression].supportColumnarCodegen(Lists.newArrayList())).exists(_ == false)
  }
  
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val i = 0
    val exprs = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue
    val exprList = { exprs.filter(expr => !expr.isInstanceOf[Literal]) }
    val inputAttributes = exprList.toList.map(expr => ConverterUtils.getResultAttrFromExpr(expr))

    val colCondExpr = branches(i)._1
    val (cond_node, condType): (TreeNode, ArrowType) =
      colCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    var colRetExpr = branches(i)._2
    if (rename && colRetExpr.isInstanceOf[AttributeReference]) {
      colRetExpr = new ColumnarBoundReference(inputAttributes.indexOf(colRetExpr),
                                              colRetExpr.dataType, colRetExpr.nullable)
    }
    val (ret_node, retType): (TreeNode, ArrowType) =
      colRetExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val funcNode = TreeBuilder.makeIf(cond_node, ret_node,
        elseNode(args, i + 1, inputAttributes, retType), retType)
    (funcNode, retType)
  }

  def elseNode(args: java.lang.Object, idx: Int,
                inputAttributes: List[AttributeReference], retType: ArrowType): TreeNode = {
    if (idx == branches.size) {
      val elseValueExpr = elseValue.orNull
      val (else_node, elseType): (TreeNode, ArrowType) = if (elseValueExpr != null) {
        var colElseValueExpr = ColumnarExpressionConverter.replaceWithColumnarExpression(elseValueExpr)
        if (rename && colElseValueExpr.isInstanceOf[AttributeReference]) {
          colElseValueExpr = new ColumnarBoundReference(inputAttributes.indexOf(colElseValueExpr),
            colElseValueExpr.dataType, colElseValueExpr.nullable)
        }
        colElseValueExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      } else {
        (TreeBuilder.makeNull(retType), retType)
      }
      return else_node
    }
    val colCondExpr = branches(idx)._1
    val (cond_node, condType): (TreeNode, ArrowType) =
      colCondExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    var colRetExpr = branches(idx)._2
    if (rename && colRetExpr.isInstanceOf[AttributeReference]) {
      colRetExpr = new ColumnarBoundReference(inputAttributes.indexOf(colRetExpr),
        colRetExpr.dataType, colRetExpr.nullable)
    }
    val (ret_node, ret_type): (TreeNode, ArrowType) =
      colRetExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val funcNode = TreeBuilder.makeIf(cond_node, ret_node,
        elseNode(args, idx + 1, inputAttributes, retType), retType)
    funcNode
  }
}

object ColumnarCaseWhenOperator {

  def create(branches: Seq[(Expression, Expression)], elseValue: Option[Expression],
             original: Expression, rename: Boolean = true): Expression = original match {
    case i: CaseWhen =>
      new ColumnarCaseWhen(branches, elseValue, i, rename)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
