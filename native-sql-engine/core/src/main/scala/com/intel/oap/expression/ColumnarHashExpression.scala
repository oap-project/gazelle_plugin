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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Murmur3Hash
import org.apache.spark.sql.types._


class ColumnarMurmur3Hash(children: Seq[Expression], seed: Int)
  extends Murmur3Hash(children: Seq[Expression], seed: Int) with ColumnarExpression {

  buildCheck

  def buildCheck(): Unit = {
    val supportedTypes =
      List(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType)
    if (children.map(child => supportedTypes.contains(child.dataType)).exists(_ == false)) {
      throw new UnsupportedOperationException("Fall back hash expression because" +
        " there is at least one unsupported input type!")
    }
  }

  override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
    false
  }

  override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
    val resultType = new ArrowType.Int(32, true)
    var hashNode = TreeBuilder.makeLiteral(new Integer(seed))
    var funcNode: TreeNode = null
    for (child: Expression <- children) {
      val (childNode, _): (TreeNode, ArrowType) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val gandivaFuncName = child.dataType match {
        case LongType => "hash64_spark"
        case DoubleType => "hash64_spark"
        case StringType => "hashbuf_spark"
        case _ => "hash32_spark"
      }
      funcNode =
        TreeBuilder.makeFunction(
          gandivaFuncName,
          Lists.newArrayList(childNode, hashNode),
          resultType)
      hashNode = funcNode
    }
    (funcNode, resultType)
  }
}

object ColumnarHashExpression {

  def create(children: Seq[Expression], seed: Int, original: Expression): Expression = {
    original match {
      case _: Murmur3Hash =>
        new ColumnarMurmur3Hash(children, seed)
    }
  }
}
