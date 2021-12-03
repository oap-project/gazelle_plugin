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
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarRegExpReplace(subject: Expression, regexp: Expression, rep: Expression, pos: Expression)
  extends RegExpReplace(subject: Expression, regexp: Expression, rep: Expression, pos: Expression)
    with ColumnarExpression
    with Logging {

  def this(subject: Expression, regexp: Expression, rep: Expression) =
    this(subject, regexp, rep, Literal(1))

  buildCheck()

  def buildCheck(): Unit = {
    val supportedTypes = List(StringType)
    if (supportedTypes.indexOf(subject.dataType) == -1) {
      throw new UnsupportedOperationException(
        s"${subject.dataType} is not supported in ColumnarRegexpReplace")
    }
  }


  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (subject_node, subjectType): (TreeNode, ArrowType) =
      subject.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (regexp_node, regexpType): (TreeNode, ArrowType) =
      regexp.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
    val (rep_node, repType): (TreeNode, ArrowType) =
      rep.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = CodeGeneration.getResultType(dataType)
    val funcNode = TreeBuilder.makeFunction("regexp_replace",
      Lists.newArrayList(subject_node, regexp_node, rep_node), resultType)
    (funcNode, resultType)
  }
    
}

object ColumnarRegExpReplaceOperator {

  def create(subject: Expression, regexp: Expression, rep: Expression, pos: Expression, original: Expression): Expression = original match {
    case r: RegExpReplace =>
      new ColumnarRegExpReplace(subject, regexp, rep, pos)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}

