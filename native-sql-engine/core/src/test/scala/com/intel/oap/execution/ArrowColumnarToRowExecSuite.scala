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

package com.intel.oap.execution

import java.time.ZoneId

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.sql.execution.RowToColumnConverter
import com.intel.oap.vectorized.{ArrowColumnarToRowInfo, ArrowColumnarToRowJniWrapper, ArrowWritableColumnVector}
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ArrowColumnarToRowExecSuite extends SharedSparkSession {

  test("ArrowColumnarToRowExec: Boolean type with array list") {
    val schema = StructType(Seq(StructField("boolean type with array", ArrayType(BooleanType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(true, false)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(true == array.getBoolean(0))
      assert(false == array.getBoolean(1))
    }
  }

  test("ArrowColumnarToRowExec: Byte type with array list") {
    val schema = StructType(Seq(StructField("boolean type with array", ArrayType(ByteType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1.toByte, 2.toByte)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1.toByte == array.getByte(0))
      assert(2.toByte == array.getByte(1))
    }
  }

  test("ArrowColumnarToRowExec: Short type with array list") {
    val schema = StructType(Seq(StructField("short type with array", ArrayType(ShortType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1.toShort, 2.toShort)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1.toShort == array.getShort(0))
      assert(2.toShort == array.getShort(1))
    }
  }

  test("ArrowColumnarToRowExec: Int type with array list") {
    val schema = StructType(Seq(StructField("Int type with array", ArrayType(IntegerType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1, 2)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1 == array.getInt(0))
      assert(2 == array.getInt(1))
    }
  }

  test("ArrowColumnarToRowExec: Long type with array list") {
    val schema = StructType(Seq(StructField("Long type with array", ArrayType(LongType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1.toLong, 2.toLong)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1.toLong == array.getLong(0))
      assert(2.toLong == array.getLong(1))
    }
  }

  test("ArrowColumnarToRowExec: Float type with array list") {
    val schema = StructType(Seq(StructField("Float type with array", ArrayType(FloatType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1.toFloat, 2.toFloat)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1.toFloat == array.getFloat(0))
      assert(2.toFloat == array.getFloat(1))
    }
  }

  test("ArrowColumnarToRowExec: Double type with array list") {
    val schema = StructType(Seq(StructField("Double type with array", ArrayType(DoubleType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(Seq(1.toDouble, 2.toDouble)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(1.toDouble == array.getDouble(0))
      assert(2.toDouble == array.getDouble(1))
    }
  }

  test("ArrowColumnarToRowExec: String type with array list") {
    val schema = StructType(Seq(StructField("String type with array", ArrayType(StringType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(
        Seq(UTF8String.fromString("abc"), UTF8String.fromString("def"))))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(UTF8String.fromString("abc") == array.getUTF8String(0))
      assert(UTF8String.fromString("def") == array.getUTF8String(1))
    }
  }

  test("ArrowColumnarToRowExec: Decimal type with array list precision <= 18") {
    val schema = StructType(
      Seq(StructField("Decimal type with array", ArrayType(DecimalType(10, 2)))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(
        Seq(new Decimal().set(BigDecimal("1.00")), new Decimal().set(BigDecimal("1.00")))))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(new Decimal().set(BigDecimal("1.00")) == array.getDecimal(0, 10, 2))
      assert(new Decimal().set(BigDecimal("1.00")) == array.getDecimal(0, 10, 2))
    }
  }

  test("ArrowColumnarToRowExec: Decimal type with array list precision > 18") {
    val schema = StructType(
      Seq(StructField("Decimal type with array", ArrayType(DecimalType(19, 2)))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(
        Seq(new Decimal().set(BigDecimal("1.00")), new Decimal().set(BigDecimal("1.00")))))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(new Decimal().set(BigDecimal("1.00")) == array.getDecimal(0, 19, 2))
      assert(new Decimal().set(BigDecimal("1.00")) == array.getDecimal(0, 19, 2))
    }
  }

  test("ArrowColumnarToRowExec: Timestamp type with array list ") {
    val defaultZoneId = ZoneId.systemDefault()
    val schema = StructType(
      Seq(StructField("Timestamp type with array", ArrayType(TimestampType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(
        Seq( DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get,  DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert( DateTimeUtils.stringToTimestamp(
        UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get == array.get(0, TimestampType).asInstanceOf[Long])
      assert( DateTimeUtils.stringToTimestamp(
        UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get == array.get(1, TimestampType).asInstanceOf[Long])
    }
  }

  test("ArrowColumnarToRowExec: Date32 type with array list ") {
    val defaultZoneId = ZoneId.systemDefault()
    val schema = StructType(
      Seq(StructField("Date type with array", ArrayType(DateType))))
    val rowIterator = (0 until 2).map { i =>
      InternalRow(new GenericArrayData(
        Seq(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get,
          DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get)))
    }.toIterator

    val cb = ArrowColumnarToRowExecSuite.createColumnarBatch(schema, rowIterator)
    val info = ArrowColumnarToRowExecSuite.nativeOp(cb)

    var rowId = 0
    val row = new UnsafeRow(cb.numCols())
    while (rowId < cb.numRows()) {
      val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
      row.pointTo(null, info.memoryAddress + offset, length.toInt)
      rowId += 1
      val array = row.getArray(0)
      assert(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get ==
        array.get(0, DateType).asInstanceOf[Int])
      assert(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get ==
        array.get(1, DateType).asInstanceOf[Int])
    }
  }
}

object ArrowColumnarToRowExecSuite {

  def serializeSchema(fields: Seq[Field]): Array[Byte] = {
    val schema = new Schema(fields.asJava)
    ConverterUtils.getSchemaBytesBuf(schema)
  }

  def nativeOp(batch: ColumnarBatch): ArrowColumnarToRowInfo = {
    val bufAddrs = new ListBuffer[Long]()
    val bufSizes = new ListBuffer[Long]()
    val fields = new ListBuffer[Field]()

    val recordBatch = ConverterUtils.createArrowRecordBatch(batch)
    recordBatch.getBuffers().asScala.foreach { buffer => bufAddrs += buffer.memoryAddress() }
    recordBatch.getBuffersLayout().asScala.foreach { bufLayout =>
      bufSizes += bufLayout.getSize()
    }
    ConverterUtils.releaseArrowRecordBatch(recordBatch)

    (0 until batch.numCols).foreach { idx =>
      val column = batch.column(idx).asInstanceOf[ArrowWritableColumnVector]
      fields += column.getValueVector.getField
    }
    val arrowSchema = serializeSchema(fields)
    val jniWrapper = new ArrowColumnarToRowJniWrapper()
    jniWrapper.nativeConvertColumnarToRow(
      arrowSchema, batch.numRows(), bufAddrs.toArray, bufSizes.toArray,
      SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
  }

  def createColumnarBatch(schema: StructType, rowIterator: Iterator[InternalRow]): ColumnarBatch = {
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)

    var rowCount = 0
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      converters.convert(row, vectors.toArray)
      rowCount += 1
    }

    vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].setValueCount(rowCount))
    new ColumnarBatch(vectors.toArray, rowCount)
  }
}

