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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType


case class ColumnarURLDecoder(input: Expression) extends Expression with ColumnarExpression {
  def nullable: Boolean = {
    true
  }

  def children: Seq[Expression] = {
    Seq(input)
  }

  def dataType: DataType = {
    StringType
  }

  def eval(input: InternalRow): Any = {
  }

  def child: Expression = {
    input
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException("Should not trigger code gen")
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ColumnarURLDecoder = {
    copy(input = newChildren.head)
  }

  buildCheck

  def buildCheck: Unit = {
    val supportedTypes = List(StringType)
    if (!supportedTypes.contains(input.dataType)) {
      throw new UnsupportedOperationException("Only StringType input is supported!");
    }
  }

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val (inputNode, _): (TreeNode, ArrowType) =
      input.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val resultType = new ArrowType.Utf8()
    val funcNode =
      TreeBuilder.makeFunction(
        "url_decoder",
        Lists.newArrayList(inputNode),
        resultType)
    (funcNode, resultType)
  }
}

object UDF {
  val supportList = {"testUDF"}

  def isSupportedUDF(name: String): Boolean = {
    return supportList.contains(name)
  }

  def create(children: Seq[Expression], original: Expression): Expression = {
    original.prettyName match {
      case "testUDF" =>
        ColumnarURLDecoder(children.head)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
