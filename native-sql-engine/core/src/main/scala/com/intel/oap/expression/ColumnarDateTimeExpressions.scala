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

import java.util.Collections

import com.google.common.collect.Lists
import com.intel.oap.expression.ColumnarDateTimeExpressions.castDateFromTimestamp
import com.intel.oap.expression.ColumnarDateTimeExpressions.unimplemented
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.gandiva.expression.TreeNode
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.catalyst.expressions.CheckOverflow
import org.apache.spark.sql.catalyst.expressions.CurrentDate
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp
import org.apache.spark.sql.catalyst.expressions.DateDiff
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.catalyst.expressions.DayOfWeek
import org.apache.spark.sql.catalyst.expressions.DayOfYear
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Hour
import org.apache.spark.sql.catalyst.expressions.MakeDate
import org.apache.spark.sql.catalyst.expressions.MakeTimestamp
import org.apache.spark.sql.catalyst.expressions.MicrosToTimestamp
import org.apache.spark.sql.catalyst.expressions.MillisToTimestamp
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.catalyst.expressions.Now
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.sql.catalyst.expressions.SecondsToTimestamp
import org.apache.spark.sql.catalyst.expressions.UnixDate
import org.apache.spark.sql.catalyst.expressions.UnixMicros
import org.apache.spark.sql.catalyst.expressions.UnixMillis
import org.apache.spark.sql.catalyst.expressions.UnixSeconds
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils

object ColumnarDateTimeExpressions {
  class ColumnarCurrentTimestamp() extends CurrentTimestamp with ColumnarExpression {
    unimplemented()
    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "current_timestamp", Collections.emptyList(), outType)
      (funcNode, outType)
    }
  }

  class ColumnarCurrentDate(timeZoneId: Option[String] = None) extends CurrentDate(timeZoneId)
      with ColumnarExpression {
    unimplemented()
    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      castDateFromTimestamp(new ColumnarCurrentTimestamp(),
        timeZoneId)
          .doColumnarCodeGen(args)
    }
  }

  class ColumnarNow() extends Now()
      with ColumnarExpression {
    unimplemented()
    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      new ColumnarCurrentTimestamp().doColumnarCodeGen(args)
    }
  }

  class ColumnarHour(child: Expression,
      timeZoneId: Option[String] = None) extends Hour(child, timeZoneId) with ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      if (child.dataType != TimestampType) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarHour")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc,
        timeZoneId)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractHour", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarMinute(child: Expression,
      timeZoneId: Option[String] = None) extends Minute(child, timeZoneId) with ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      if (child.dataType != TimestampType) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarMinute")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc,
        timeZoneId)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractMinute", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarSecond(child: Expression,
      timeZoneId: Option[String] = None) extends Second(child, timeZoneId) with ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      if (child.dataType != TimestampType) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarSecond")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc,
        timeZoneId)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractSecond", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarDayOfMonth(child: Expression) extends DayOfMonth(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType, TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarDayOfMonth")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractDay", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarDayOfYear(child: Expression) extends DayOfYear(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType, TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarDayOfYear")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractDoy", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarDayOfWeek(child: Expression) extends DayOfWeek(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType, TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarDayOfWeek")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractDow", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarMonth(child: Expression) extends Month(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType, TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarMonth")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractMonth", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarYear(child: Expression) extends Year(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType, TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarYear")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaTimestamp(childNodeUtc, childTypeUtc)
      val outType = ArrowUtils.toArrowType(LongType, null)
      val funcNode = TreeBuilder.makeFunction(
        "extractYear", Lists.newArrayList(childNode), outType)
      ConverterUtils.toInt32(funcNode, outType)
    }
  }

  class ColumnarUnixDate(child: Expression) extends UnixDate(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarUnixDate")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNode, childType) = child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "unix_date", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarUnixSeconds(child: Expression) extends UnixSeconds(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarUnixSeconds")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaMicroUTCTimestamp(childNodeUtc,
        childTypeUtc)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "unix_seconds", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarUnixMillis(child: Expression) extends UnixMillis(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarUnixMillis")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaMicroUTCTimestamp(childNodeUtc,
        childTypeUtc)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "unix_millis", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarUnixMicros(child: Expression) extends UnixMicros(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(TimestampType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarUnixMicros")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNodeUtc, childTypeUtc) =
        child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (childNode, childType) = ConverterUtils.toGandivaMicroUTCTimestamp(childNodeUtc,
        childTypeUtc)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "unix_micros", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarSecondsToTimestamp(child: Expression) extends SecondsToTimestamp(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(IntegerType, LongType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarSecondsToTimestamp")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNode, childType) = child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "seconds_to_timestamp", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarMillisToTimestamp(child: Expression) extends MillisToTimestamp(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(IntegerType, LongType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarMillisToTimestamp")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNode, childType) = child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "millis_to_timestamp", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarMicrosToTimestamp(child: Expression) extends MicrosToTimestamp(child) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(IntegerType, LongType)
      if (supportedTypes.indexOf(child.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${child.dataType} is not supported in ColumnarMicrosToTimestamp")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (childNode, childType) = child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "micros_to_timestamp", Lists.newArrayList(childNode), outType)
      (funcNode, outType)
    }
  }

  /**
   * Converts time string with given pattern to Unix time stamp (in seconds), returns null if fail.
   */
  class ColumnarUnixTimestamp(left: Expression, right: Expression)
      extends UnixTimestamp(left, right) with
      ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(TimestampType, StringType, DateType)
      if (supportedTypes.indexOf(left.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} is not supported in ColumnarUnixTimestamp.")
      }
      if (left.dataType == StringType) {
        right match {
          case literal: ColumnarLiteral =>
            val format = literal.value.toString
            if (format.length > 10) {
              throw new UnsupportedOperationException(
                s"$format is not supported in ColumnarUnixTimestamp.")
            }
          case _ =>
        }
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (leftNode, leftType) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (rightNode, rightType) = right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val milliType = new ArrowType.Date(DateUnit.MILLISECOND)
      val dateNode = if (left.dataType == TimestampType) {
        val milliNode = ConverterUtils.convertTimestampToMicro(leftNode, leftType)._1
        TreeBuilder.makeFunction(
          "unix_seconds", Lists.newArrayList(milliNode), CodeGeneration.getResultType(dataType))
      } else if (left.dataType == StringType) {
        // Convert from UTF8 to Date[Millis].
        val dateNode = TreeBuilder.makeFunction(
          "castDATE_nullsafe", Lists.newArrayList(leftNode), milliType)
        val intNode = TreeBuilder.makeFunction("castBIGINT",
          Lists.newArrayList(dateNode), outType)
        // Convert from milliseconds to seconds.
        TreeBuilder.makeFunction("divide", Lists.newArrayList(intNode,
          TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))), outType)
      } else {
        // Convert from Date[Day] to seconds.
        TreeBuilder.makeFunction(
          "unix_date_seconds", Lists.newArrayList(leftNode), outType)
      }
      (dateNode, outType)
    }
  }

  class ColumnarDateDiff(left: Expression, right: Expression)
      extends DateDiff(left, right) with ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(DateType)
      if (supportedTypes.indexOf(left.dataType) == -1 ||
          supportedTypes.indexOf(right.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} is not supported in ColumnarDateDiff.")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (leftNode, leftType) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (rightNode, rightType) = right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "date_diff", Lists.newArrayList(leftNode, rightNode), outType)
      (funcNode, outType)
    }
  }

  class ColumnarMakeDate(
      year: Expression,
      month: Expression,
      day: Expression,
      failOnError: Boolean = SQLConf.get.ansiEnabled)
      extends MakeDate(year, month, day, failOnError) with ColumnarExpression {
    unimplemented()
    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (yearNode, yearType) = year.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (monthNode, monthType) = month.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (dayNode, dayType) = day.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "make_date", Lists.newArrayList(yearNode, monthNode, dayNode), outType)
      (funcNode, outType)
    }
  }
  class ColumnarMakeTimestamp(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Option[Expression] = None,
      timeZoneId: Option[String] = None,
      failOnError: Boolean = SQLConf.get.ansiEnabled)
      extends MakeTimestamp(year, month, day, hour, min, sec, timezone, timeZoneId, failOnError)
          with ColumnarExpression {
    unimplemented()
    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (yearNode, yearType) = year.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (monthNode, monthType) = month.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (dayNode, dayType) = day.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (hourNode, hourType) = hour.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (minNode, minType) = min.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (secNode, secType) = sec.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "make_timestamp", Lists.newArrayList(yearNode, monthNode, dayNode, hourNode,
          minNode, secNode), outType)
      (funcNode, outType)
    }
  }

  def castTimestampFromDate(child: Expression,
      timeZoneId: Option[String] = None): ColumnarExpression = {
    new ColumnarCast(child, DateType, timeZoneId, null)
  }

  def castDateFromTimestamp(child: Expression,
      timeZoneId: Option[String] = None): ColumnarExpression = {
    new ColumnarCast(child, TimestampType, timeZoneId, null)
  }

  def unimplemented(): Unit = {
    throw new UnsupportedOperationException()
  }
}
