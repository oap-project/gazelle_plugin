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
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.BaseSubqueryExec
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarScalarSubquery(
  query: ScalarSubquery)
  extends Expression with ColumnarExpression {

  override def dataType: DataType = query.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = query.toString
  override def eval(input: InternalRow): Any = query.eval(input)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = query.doGenCode(ctx, ev)
  override def canEqual(that: Any): Boolean = query.canEqual(that)
  override def productArity: Int = query.productArity
  override def productElement(n: Int): Any = query.productElement(n)
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val value = query.eval(null)
    val resultType = CodeGeneration.getResultType(query.dataType)
    query.dataType match {
      case t: StringType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeStringLiteral(value.toString().asInstanceOf[String]), resultType)
        }
      case t: IntegerType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeLiteral(value.asInstanceOf[Integer]), resultType)
        }
      case t: LongType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Long]), resultType)
        }
      case t: DoubleType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Double]), resultType)
        }
      case d: DecimalType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            val v = value.asInstanceOf[Decimal]
            (TreeBuilder.makeDecimalLiteral(v.toString, v.precision, v.scale), resultType)
        }
      case d: DateType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            val origIntNode = TreeBuilder.makeLiteral(value.asInstanceOf[Integer])
            val dateNode = TreeBuilder.makeFunction("castDATE", Lists.newArrayList(origIntNode), new ArrowType.Date(DateUnit.DAY))
            (dateNode, new ArrowType.Date(DateUnit.DAY))
        }
      case b: BooleanType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Boolean]), resultType)
        }
    }
  }
}
