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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RowSuite extends SparkFunSuite with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      //.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "50m")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.columnar.codegen.hashAggregate", "false")
      .set("spark.oap.sql.columnar.wholestagecodegen", "false")
      .set("spark.sql.columnar.window", "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      //.set("spark.sql.columnar.tmp_dir", "/codegen/nativesql/")
      .set("spark.sql.columnar.sort.broadcastJoin", "true")
      .set("spark.oap.sql.columnar.preferColumnar", "true")

  test("create row") {
    val expected = new GenericInternalRow(4)
    expected.setInt(0, 2147483647)
    expected.update(1, UTF8String.fromString("this is a string"))
    expected.setBoolean(2, false)
    expected.setNullAt(3)

    val actual1 = Row(2147483647, "this is a string", false, null)
    assert(expected.numFields === actual1.size)
    assert(expected.getInt(0) === actual1.getInt(0))
    assert(expected.getString(1) === actual1.getString(1))
    assert(expected.getBoolean(2) === actual1.getBoolean(2))
    assert(expected.isNullAt(3) === actual1.isNullAt(3))

    val actual2 = Row.fromSeq(Seq(2147483647, "this is a string", false, null))
    assert(expected.numFields === actual2.size)
    assert(expected.getInt(0) === actual2.getInt(0))
    assert(expected.getString(1) === actual2.getString(1))
    assert(expected.getBoolean(2) === actual2.getBoolean(2))
    assert(expected.isNullAt(3) === actual2.isNullAt(3))
  }

  test("SpecificMutableRow.update with null") {
    val row = new SpecificInternalRow(Seq(IntegerType))
    row(0) = null
    assert(row.isNullAt(0))
  }

  test("get values by field name on Row created via .toDF") {
    val row = Seq((1, Seq(1))).toDF("a", "b").first()
    assert(row.getAs[Int]("a") === 1)
    assert(row.getAs[Seq[Int]]("b") === Seq(1))

    intercept[IllegalArgumentException]{
      row.getAs[Int]("c")
    }
  }

  test("float NaN == NaN") {
    val r1 = Row(Float.NaN)
    val r2 = Row(Float.NaN)
    assert(r1 === r2)
  }

  test("double NaN == NaN") {
    val r1 = Row(Double.NaN)
    val r2 = Row(Double.NaN)
    assert(r1 === r2)
  }

  test("equals and hashCode") {
    val r1 = Row("Hello")
    val r2 = Row("Hello")
    assert(r1 === r2)
    assert(r1.hashCode() === r2.hashCode())
    val r3 = Row("World")
    assert(r3.hashCode() != r1.hashCode())
  }

  test("toString") {
    val r1 = Row(2147483647, 21474.8364, (-5).toShort, "this is a string", true, null)
    assert(r1.toString == "[2147483647,21474.8364,-5,this is a string,true,null]")
    val r2 = Row(null, Int.MinValue, Double.NaN, Short.MaxValue, "", false)
    assert(r2.toString == "[null,-2147483648,NaN,32767,,false]")
    val tsString = "2019-05-01 17:30:12.0"
    val dtString = "2019-05-01"
    val r3 = Row(
      r1,
      Seq(1, 2, 3),
      Map(1 -> "a", 2 -> "b"),
      java.sql.Timestamp.valueOf(tsString),
      java.sql.Date.valueOf(dtString),
      BigDecimal("1234567890.1234567890"),
      (-1).toByte)
    assert(r3.toString == "[[2147483647,21474.8364,-5,this is a string,true,null],List(1, 2, 3)," +
      s"Map(1 -> a, 2 -> b),$tsString,$dtString,1234567890.1234567890,-1]")
    val empty = Row()
    assert(empty.toString == "[]")
  }
}
