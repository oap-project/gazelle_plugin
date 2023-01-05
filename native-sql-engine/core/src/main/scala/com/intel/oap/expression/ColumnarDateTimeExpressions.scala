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
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.gandiva.expression.TreeNode
import org.apache.arrow.vector.types.{DateUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.catalyst.expressions.CurrentDate
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp
import org.apache.spark.sql.catalyst.expressions.DateDiff
import org.apache.spark.sql.catalyst.expressions.DateSub
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.catalyst.expressions.DayOfWeek
import org.apache.spark.sql.catalyst.expressions.DayOfYear
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.FromUnixTime
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
import org.apache.spark.sql.catalyst.expressions.TimeZoneAwareExpression
import org.apache.spark.sql.catalyst.expressions.ToTimestamp
import org.apache.spark.sql.catalyst.expressions.UnixDate
import org.apache.spark.sql.catalyst.expressions.UnixMicros
import org.apache.spark.sql.catalyst.expressions.UnixMillis
import org.apache.spark.sql.catalyst.expressions.UnixSeconds
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DataType, DateType, IntegerType, LongType, ShortType, StringType, TimestampType}
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
    val gName = "extractYear"

    override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
      codegenFuncList.contains(gName) && 
      child.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
    }

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
    val gName = "micros_to_timestamp"

    override def supportColumnarCodegen(args: java.lang.Object): Boolean = {
      codegenFuncList.contains(gName) && 
      child.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
    }

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
    * Converts time string with given pattern to Unix timestamp (in seconds), returns null if fail.
    * The input is the date/time for local timezone (can be configured in spark) and the result is
    * the timestamp for UTC. So we need consider timezone difference.
    * */
  class ColumnarUnixTimestamp(
      left: Expression,
      right: Expression,
      timeZoneId: Option[String] = None,
      failOnError: Boolean = SQLConf.get.ansiEnabled)
      extends UnixTimestamp(left, right, timeZoneId, failOnError) with
      ColumnarExpression {

    val gName = "unix_seconds"

    val yearMonthDayFormat = "yyyy-MM-dd"
    val yearMonthDayTimeFormat = "yyyy-MM-dd HH:mm:ss"
    val yearMonthDayTimeNoSepFormat = "yyyyMMddHHmmss"
    val yearMonthDayNoSepFormat = "yyyyMMdd"
    var formatLiteral: String = null

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(TimestampType, StringType, DateType)
      if (supportedTypes.indexOf(left.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} is not supported in ColumnarUnixTimestamp.")
      }
      // The format is only applicable for StringType left input.
      if (left.dataType == StringType) {
        right match {
          case literal: ColumnarLiteral =>
            this.formatLiteral = literal.value.toString.trim
            // Only support yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
            if (!this.formatLiteral.equals(yearMonthDayFormat) &&
              !this.formatLiteral.equals(yearMonthDayTimeFormat) &&
              !this.formatLiteral.equals(yearMonthDayTimeNoSepFormat) &&
              !this.formatLiteral.equals(yearMonthDayNoSepFormat)) {
              throw new UnsupportedOperationException(
                s"$formatLiteral is not supported in ColumnarUnixTimestamp.")
            }
          case _ =>
            throw new UnsupportedOperationException("Only literal format is" +
              " supported for ColumnarUnixTimestamp!")
        }
      }
    }

    override def supportColumnarCodegen(args: Object): Boolean = {
      codegenFuncList.contains(gName) &&
        left.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args) &&
        right.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (leftNode, leftType) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val milliType = new ArrowType.Date(DateUnit.MILLISECOND)
      val dateNode = if (left.dataType == TimestampType) {
        val milliNode = ConverterUtils.convertTimestampToMicro(leftNode, leftType)._1
        TreeBuilder.makeFunction(
          "unix_seconds", Lists.newArrayList(milliNode), CodeGeneration.getResultType(dataType))
      } else if (left.dataType == StringType) {
        if (this.formatLiteral.equals(yearMonthDayFormat)) {
          // Convert from UTF8 to Date[Millis].
          val dateNode = TreeBuilder.makeFunction(
            "castDATE_nullsafe", Lists.newArrayList(leftNode), milliType)
          val intNode = TreeBuilder.makeFunction("castBIGINT",
            Lists.newArrayList(dateNode), outType)
          // Convert from milliseconds to seconds.
          TreeBuilder.makeFunction("divide", Lists.newArrayList(
            ConverterUtils.subtractTimestampOffset(intNode),
            TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))), outType)
        } else if (this.formatLiteral.equals(yearMonthDayTimeFormat)) {
          val timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
          val timestampNode = TreeBuilder.makeFunction("castTIMESTAMP_withCarrying",
            Lists.newArrayList(leftNode), timestampType)
          val castNode = TreeBuilder.makeFunction("castBIGINT",
            Lists.newArrayList(timestampNode), outType)
          TreeBuilder.makeFunction("divide", Lists.newArrayList(
            ConverterUtils.subtractTimestampOffset(castNode),
            TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))), outType)
        } else if (this.formatLiteral.equals(yearMonthDayTimeNoSepFormat) ||
          this.formatLiteral.equals(yearMonthDayNoSepFormat)) {
          val timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
          val timestampNode = TreeBuilder.makeFunction("castTIMESTAMP_withCarrying_withoutSep",
            Lists.newArrayList(leftNode), timestampType)
          // The result is in milliseconds.
          val castNode = TreeBuilder.makeFunction("castBIGINT",
            Lists.newArrayList(timestampNode), outType)
          // Convert to the timestamp in seconds.
          TreeBuilder.makeFunction("divide", Lists.newArrayList(
            ConverterUtils.subtractTimestampOffset(castNode),
            TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))), outType)
        } else {
          throw new RuntimeException("Unexpected format for ColumnarUnixTimestamp!")
        }
      } else {
        // Convert from Date[Day] to seconds.
        TreeBuilder.makeFunction(
          "unix_date_seconds", Lists.newArrayList(leftNode), outType)
      }
      (dateNode, outType)
    }
  }

  // The datatype is TimestampType.
  // Internally, a timestamp is stored as the number of microseconds from unix epoch.
  case class ColumnarGetTimestamp(leftChild: Expression,
                             rightChild: Expression,
                             timeZoneId: Option[String] = None)
      extends ToTimestamp with ColumnarExpression {

    override def left: Expression = leftChild
    override def right : Expression = rightChild
    override def canEqual(that: Any): Boolean = true
    // The below functions are consistent with spark GetTimestamp.
    override val downScaleFactor = 1
    override def dataType: DataType = TimestampType
    override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
      copy(timeZoneId = Option(timeZoneId))
    override def failOnError: Boolean = SQLConf.get.ansiEnabled

    buildCheck()

    def buildCheck(): Unit = {
      val parserPolicy = SQLConf.get.getConf(SQLConf.LEGACY_TIME_PARSER_POLICY);
      // TODO: support "exception" time parser policy.
      if (!parserPolicy.equalsIgnoreCase("corrected")) {
        throw new UnsupportedOperationException(
          s"$parserPolicy is NOT a supported time parser policy");
      }
      
      val supportedTypes = List(StringType)
      if (supportedTypes.indexOf(left.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} is not supported in ColumnarUnixTimestamp.")
      }
      if (left.dataType == StringType) {
        right match {
          case literal: ColumnarLiteral =>
            val format = literal.value.toString.trim
            // TODO: support other format.
            if (!format.equals("yyyy-MM-dd")) {
              throw new UnsupportedOperationException(
                s"$format is not supported in ColumnarUnixTimestamp.")
            }
          case _ =>
        }
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      // Use default timeZoneId. Give specific timeZoneId if needed in the future.
      val outType = CodeGeneration.getResultType(TimestampType)
      val (leftNode, leftType) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      // convert to milli, then convert to micro
      val intermediateType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)

      right match {
        case literal: ColumnarLiteral =>
          val format = literal.value.toString.trim
          if (format.equals("yyyy-MM-dd")) {
            val funcNode = TreeBuilder.makeFunction("castTIMESTAMP_with_validation_check",
              Lists.newArrayList(leftNode), intermediateType)
            ConverterUtils.convertTimestampToMicro(funcNode, intermediateType)
          } else {
            // TODO: add other format support.
            throw new UnsupportedOperationException(
              s"$format is not supported in ColumnarUnixTimestamp.")
          }
      }
    }

    // For spark 3.2.
    protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
    ColumnarGetTimestamp =
      copy(leftChild = newLeft, rightChild = newRight)
  }

  /**
    * The result is the date/time for local timezone (can be configured in spark). The input is
    * the timestamp for UTC. So we need consider timezone difference.
    */
  class ColumnarFromUnixTime(left: Expression, right: Expression, timeZoneId: Option[String] = None)
      extends FromUnixTime(left, right, timeZoneId) with
      ColumnarExpression {
        
    override def supportColumnarCodegen(args: Object): Boolean = {
      false && left.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args) &&
        right.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
    }

    var formatLiteral: String = null
    val yearMonthDayFormat = "yyyy-MM-dd"
    val yearMonthDayNoSepFormat = "yyyyMMdd"
    val yearMonthDayTimeFormat = "yyyy-MM-dd HH:mm:ss"

    buildCheck()

    def buildCheck(): Unit = {
      val supportedTypes = List(LongType)
      if (supportedTypes.indexOf(left.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} is not supported in ColumnarFromUnixTime.")
      }
      if (left.dataType == LongType) {
        right match {
          case literal: ColumnarLiteral =>
            this.formatLiteral = literal.value.toString.trim
            if (!formatLiteral.equals(yearMonthDayFormat) &&
              !formatLiteral.equals(yearMonthDayNoSepFormat) &&
              !formatLiteral.equals(yearMonthDayTimeFormat)) {
              throw new UnsupportedOperationException(
                s"$format is not supported in ColumnarFromUnixTime.")
            }
          case _ =>
            throw new UnsupportedOperationException("Only literal format is supported!")
        }
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (leftNode, _) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      if (this.formatLiteral.equals(yearMonthDayFormat) ||
        this.formatLiteral.equals(yearMonthDayNoSepFormat)) {
        val date32LeftNode = if (left.dataType == LongType) {
          // cast unix seconds to date64()
          val milliNode = TreeBuilder.makeFunction("multiply", Lists.newArrayList(leftNode,
            TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))), new ArrowType.Int(8 * 8, true))
          val date64Node = TreeBuilder.makeFunction("castDATE",
            Lists.newArrayList(ConverterUtils.addTimestampOffset(milliNode)),
              new ArrowType.Date(DateUnit.MILLISECOND))
          TreeBuilder.makeFunction("castDATE", Lists.newArrayList(date64Node),
            new ArrowType.Date(DateUnit.DAY))
        } else {
          throw new UnsupportedOperationException(
            s"${left.dataType} is not supported in ColumnarFromUnixTime.")
        }
        var formatLength = 0L
        right match {
          case literal: ColumnarLiteral =>
            val format = literal.value.toString.trim
            if (format.equals(yearMonthDayFormat)) {
              formatLength = 10L
            } else if (format.equals(yearMonthDayNoSepFormat)) {
              formatLength = 8L
            }
        }
        val dateNode = TreeBuilder.makeFunction(
          "castVARCHAR", Lists.newArrayList(date32LeftNode,
            TreeBuilder.makeLiteral(java.lang.Long.valueOf(formatLength))), outType)
        (dateNode, outType)
      } else if (this.formatLiteral.equals(yearMonthDayTimeFormat)) {
        // Only millisecond based input is expected in following functions, but the raw input
        // is second based. So we make the below conversion.
        val tsInMilliSecNode = TreeBuilder.makeFunction("multiply", Lists.newArrayList(
          leftNode, TreeBuilder.makeLiteral(java.lang.Long.valueOf(1000L))),
          new ArrowType.Int(64, true))
        val timestampType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
        val timestampNode = TreeBuilder.makeFunction("castTIMESTAMP",
          Lists.newArrayList(ConverterUtils.addTimestampOffset(tsInMilliSecNode)), timestampType)
        // The largest length for yyyy-MM-dd HH:mm:ss.
        val lenNode = TreeBuilder.makeLiteral(java.lang.Long.valueOf(19L))
        val resultNode = TreeBuilder.makeFunction("castVARCHAR",
          Lists.newArrayList(timestampNode, lenNode), outType)
        (resultNode, outType)
      } else {
        throw new RuntimeException("Unexpected format is used!")
      }
    }
  }

  class ColumnarDateSub(left: Expression, right: Expression) extends
      DateSub(left, right) with ColumnarExpression {

    buildCheck()

    def buildCheck(): Unit = {
      val supportedLeftTypes = List(DateType)
      val supportedRightTypes = List(IntegerType, ShortType, ByteType)
      if (supportedLeftTypes.indexOf(left.dataType) == -1 ||
          supportedRightTypes.indexOf(right.dataType) == -1) {
        throw new UnsupportedOperationException(
          s"${left.dataType} or ${right.dataType} is not supported in ColumnarDateDiff.")
      }
    }

    override def doColumnarCodeGen(args: Object): (TreeNode, ArrowType) = {
      val (leftNode, leftType) = left.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (rightNode, rightType) = right.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val outType = CodeGeneration.getResultType(dataType)
      val funcNode = TreeBuilder.makeFunction(
        "date_sub", Lists.newArrayList(leftNode, rightNode), outType)
      (funcNode, outType)
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

    override def supportColumnarCodegen(args: Object): Boolean = {
      false && left.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args) &&
        right.asInstanceOf[ColumnarExpression].supportColumnarCodegen(args)
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
