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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.ArrowRowToColumnarJniWrapper
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.types.pojo.{Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkSchemaUtils}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

class ArrowRowToColumnarExecSuite extends SharedSparkSession {

  test("ArrowRowToColumnarExec: Boolean type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(BooleanType)))
    val schema = StructType(Seq(StructField("boolean type with array", ArrayType(BooleanType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(true, false))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(true == array.getBoolean(0))
      assert(false == array.getBoolean(1))
    }
  }

  test("ArrowRowToColumnarExec: Byte type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(ByteType)))
    val schema = StructType(Seq(StructField("boolean type with array", ArrayType(ByteType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(1.toByte, 2.toByte))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(1.toByte == array.getByte(0))
      assert(2.toByte == array.getByte(1))
    }
  }

  test("ArrowRowToColumnarExec: Short type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(ShortType)))
    val schema = StructType(Seq(StructField("short type with array", ArrayType(ShortType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(1.toShort, 2.toShort))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(1.toShort == array.getShort(0))
      assert(2.toShort == array.getShort(1))
    }
  }

  test("ArrowRowToColumnarExec: Int type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(IntegerType)))
    val schema = StructType(Seq(StructField("Int type with array", ArrayType(IntegerType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(-10, -20))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(-10 == array.getInt(0))
      assert(-20 == array.getInt(1))
    }
  }

  test("ArrowRowToColumnarExec: Long type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(LongType)))
    val schema = StructType(Seq(StructField("Long type with array", ArrayType(LongType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(1.toLong, 2.toLong))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(1.toLong == array.getLong(0))
      assert(2.toLong == array.getLong(1))
    }
  }

  test("ArrowRowToColumnarExec: Float type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(FloatType)))
    val schema = StructType(Seq(StructField("Float type with array", ArrayType(FloatType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(1.toFloat, 2.toFloat))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(1.toFloat == array.getFloat(0))
      assert(2.toFloat == array.getFloat(1))
    }
  }

  test("ArrowRowToColumnarExec: Double type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(DoubleType)))
    val schema = StructType(Seq(StructField("Double type with array", ArrayType(DoubleType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(Seq(1.toDouble, 2.toDouble))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(1.toDouble == array.getDouble(0))
      assert(2.toDouble == array.getDouble(1))
    }
  }

  test("ArrowRowToColumnarExec: String type with array list") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(StringType)))
    val schema = StructType(Seq(StructField("String type with array", ArrayType(StringType))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(
        Seq(UTF8String.fromString("abc"), UTF8String.fromString("def")))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(UTF8String.fromString("abc") == array.getUTF8String(0))
      assert(UTF8String.fromString("def") == array.getUTF8String(1))
    }
  }

  test("ArrowRowToColumnarExec: Decimal type with array list precision <= 18") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(DecimalType(10, 4))))
    val schema = StructType(
      Seq(StructField("Decimal type with array", ArrayType(DecimalType(10, 4)))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(
        Seq(new Decimal().set(BigDecimal("-1.5645")), new Decimal().set(BigDecimal("-1.8645"))))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(new Decimal().set(BigDecimal("-1.5645")) == array.getDecimal(0, 10, 4))
      assert(new Decimal().set(BigDecimal("-1.8645")) == array.getDecimal(1, 10, 4))
    }
  }

  test("ArrowRowToColumnarExec: Decimal type with array list precision > 18") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(DecimalType(19, 4))))
    val schema = StructType(
      Seq(StructField("Decimal type with array", ArrayType(DecimalType(19, 4)))))
    val rowIterator = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(
        Seq(new Decimal().set(BigDecimal("1.2457")), new Decimal().set(BigDecimal("1.2457"))))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(new Decimal().set(BigDecimal("1.2457")) == array.getDecimal(0, 19, 4))
      assert(new Decimal().set(BigDecimal("1.2457")) == array.getDecimal(1, 19, 4))
    }
  }

  test("ArrowRowToColumnarExec: Timestamp type with array list ") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(TimestampType)))
    val defaultZoneId = ZoneId.systemDefault()
    val schema = StructType(
      Seq(StructField("Timestamp type with array", ArrayType(TimestampType))))
    val rowIterator: Iterator[InternalRow] = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(
        Seq(DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get,
          DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get))))
    }.toIterator

    val cb = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(DateTimeUtils.stringToTimestamp(
        UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get ==
        array.get(0, TimestampType).asInstanceOf[Long])
      assert(DateTimeUtils.stringToTimestamp(
        UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get ==
        array.get(1, TimestampType).asInstanceOf[Long])
    }
  }

  test("ArrowRowToColumnarExec: Date32 type with array list ") {
    val converter: UnsafeProjection =
      UnsafeProjection.create(Array[DataType](ArrayType(DateType)))
    val defaultZoneId = ZoneId.systemDefault()
    val schema: StructType = StructType(
      Seq(StructField("Date type with array", ArrayType(DateType))))
    val rowIterator: Iterator[InternalRow] = (0 until 2).map { i =>
      converter.apply(InternalRow(new GenericArrayData(
        Seq(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get,
          DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get))))
    }.toIterator

    val cb: ColumnarBatch = ArrowRowToColumnarExecSuite.nativeOp(schema, rowIterator)
    val convert_rowIterator = cb.rowIterator

    var rowId = 0
    while (rowId < cb.numRows()) {
      val row = convert_rowIterator.next()
      rowId += 1
      val array = row.getArray(0)
      assert(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get ==
        array.get(0, DateType).asInstanceOf[Int])
      assert(DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1"), defaultZoneId).get ==
        array.get(1, DateType).asInstanceOf[Int])
    }
  }
}

object ArrowRowToColumnarExecSuite {

  def serializeSchema(fields: Seq[Field]): Array[Byte] = {
    val schema = new Schema(fields.asJava)
    ConverterUtils.getSchemaBytesBuf(schema)
  }

  def nativeOp(schema: StructType, rowIterator: Iterator[InternalRow]): ColumnarBatch = {
    val bufferSize = 1024  // 128M can estimator the buffer size based on the data type
    val allocator = SparkMemoryUtils.contextAllocator()
    val arrowBuf = allocator.buffer(bufferSize)

    val rowLength = new ListBuffer[Long]()
    var rowCount = 0
    var offset = 0
    while (rowIterator.hasNext) {
      val row = rowIterator.next() // UnsafeRow
      assert(row.isInstanceOf[UnsafeRow])
      val unsafeRow = row.asInstanceOf[UnsafeRow]
      val sizeInBytes = unsafeRow.getSizeInBytes
      Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
        null, arrowBuf.memoryAddress() + offset, sizeInBytes)
      offset += sizeInBytes
      rowLength += sizeInBytes.toLong
      rowCount += 1
    }
    val timeZoneId = SparkSchemaUtils.getLocalTimezoneID()
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val schemaBytes: Array[Byte] = ConverterUtils.getSchemaBytesBuf(arrowSchema)
    val jniWrapper = new ArrowRowToColumnarJniWrapper()
    val serializedRecordBatch = jniWrapper.nativeConvertRowToColumnar(
      schemaBytes, rowLength.toArray, arrowBuf.memoryAddress(),
      SparkMemoryUtils.contextMemoryPool().getNativeInstanceId)
    val rb = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator, serializedRecordBatch)
    val output = ConverterUtils.fromArrowRecordBatch(arrowSchema, rb)
    val outputNumRows = rb.getLength
    ConverterUtils.releaseArrowRecordBatch(rb)
    new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]).toArray, outputNumRows)
  }
}

