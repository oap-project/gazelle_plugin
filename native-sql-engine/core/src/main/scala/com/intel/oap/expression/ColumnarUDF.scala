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
import com.intel.oap.GazellePluginConfig
import org.apache.arrow.gandiva.expression.{TreeBuilder, TreeNode}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType


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
    throw new UnsupportedOperationException("Should not trigger eval!")
  }

  def child: Expression = {
    input
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException("Should not trigger code gen!")
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ColumnarURLDecoder = {
    copy(input = newChildren.head)
  }

  buildCheck

  def buildCheck: Unit = {
    val supportedTypes = List(StringType)
    if (!supportedTypes.contains(input.dataType)) {
      throw new UnsupportedOperationException("Only StringType input is supported!")
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

object ColumnarUDF {
  // Keep the supported UDF name. The name is specified in registering the
  // row based function in spark, e.g.,
  // CREATE TEMPORARY FUNCTION UrlDecoder AS 'com.intel.test.URLDecoderNew';
  val supportList = List("urldecoder")
  val conf = GazellePluginConfig.getSessionConf

  def isSupportedUDF(name: String): Boolean = {
    if (!conf.enableUDF) {
      return false
    }
    if (name == null) {
      return false
    }
    return supportList.map(s => s.equalsIgnoreCase(name)).exists(_ == true)
  }

  def create(children: Seq[Expression], original: Expression): Expression = {
    original.prettyName.toLowerCase() match {
      // Hive UDF.
      case "urldecoder" =>
        ColumnarURLDecoder(children.head)
      // Scala UDF.
      case "scalaudf" =>
        original.asInstanceOf[ScalaUDF].udfName.get.toLowerCase() match {
          case "urldecoder" =>
            ColumnarURLDecoder(children.head)
          case other =>
            throw new UnsupportedOperationException(s"not currently supported: $other.")
        }
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }
}
