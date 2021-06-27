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
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.types.CalendarIntervalType

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarLiteral(lit: Literal)
    extends Literal(lit.value, lit.dataType)
    with ColumnarExpression {

  val resultType: ArrowType = buildCheck()

  def buildCheck(): ArrowType = {
    val supportedTypes =
      List(StringType, IntegerType, LongType, DoubleType, DateType,
           BooleanType, CalendarIntervalType, BinaryType, TimestampType)
    if (supportedTypes.indexOf(dataType) == -1 && !dataType.isInstanceOf[DecimalType]) {
      // Decimal is supported in ColumnarLiteral
      throw new UnsupportedOperationException(
        s"${dataType} is not supported in ColumnarLiteral")
    }
    if (dataType == CalendarIntervalType) {
      value match {
        case null =>
        case interval: CalendarInterval =>
          if (interval.days != 0 && interval.months != 0) {
            throw new UnsupportedOperationException(
              "can't support Calendar Interval with both months and days.")
          }
        case _ =>
          throw new UnsupportedOperationException(
            "can't support Literal datatype is CalendarIntervalType while real value is not.")
      }
    }
    val resultType: ArrowType = dataType match {
      case CalendarIntervalType =>
        val interval = value.asInstanceOf[CalendarInterval]
        if (interval.microseconds == 0) {
          if (interval.days == 0) {
            new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
          } else {
            new ArrowType.Interval(IntervalUnit.DAY_TIME)
          }
        } else {
          throw new UnsupportedOperationException(
            s"can't support CalendarIntervalType with microseconds yet")
        }
      case _ =>
        CodeGeneration.getResultType(dataType)
    }
    resultType
  }

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    dataType match {
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
      case t : TimestampType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Long]), resultType)
        }
      case c: CalendarIntervalType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case interval: CalendarInterval =>
            if (interval.days == 0) {
              (TreeBuilder.makeLiteral(interval.months.asInstanceOf[Integer]), resultType)
            } else {
              if (interval.months != 0) {
                throw new UnsupportedOperationException(s"can't support Calendar Interval with both months and days.")
              }
              (TreeBuilder.makeLiteral(interval.days.asInstanceOf[Integer]), resultType)
            }
          case _ =>
            throw new UnsupportedOperationException("can't support Literal datatype is CalendarIntervalType while real value is not.")
        }
      case b: BinaryType =>
        value match {
          case null =>
            (TreeBuilder.makeNull(resultType), resultType)
          case _ =>
            (TreeBuilder.makeBinaryLiteral(value.asInstanceOf[Array[Byte]]), resultType)
        }
    }
  }
}
