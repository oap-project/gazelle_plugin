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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test cases for various [[JSONOptions]].
 */
class JsonParsingOptionsSuite extends QueryTest with SharedSparkSession {
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

  test("allowComments off") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowComments on") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val df = spark.read.option("allowComments", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowSingleQuotes off") {
    val str = """{'name': 'Reynold Xin'}"""
    val df = spark.read.option("allowSingleQuotes", "false").json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowSingleQuotes on") {
    val str = """{'name': 'Reynold Xin'}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowUnquotedFieldNames off") {
    val str = """{name: 'Reynold Xin'}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowUnquotedFieldNames on") {
    val str = """{name: 'Reynold Xin'}"""
    val df = spark.read.option("allowUnquotedFieldNames", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowUnquotedControlChars off") {
    val str = """{"name": "a\u0001b"}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowUnquotedControlChars on") {
    val str = """{"name": "a\u0001b"}"""
    val df = spark.read.option("allowUnquotedControlChars", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "a\u0001b")
  }

  test("allowNumericLeadingZeros off") {
    val str = """{"age": 0018}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowNumericLeadingZeros on") {
    val str = """{"age": 0018}"""
    val df = spark.read.option("allowNumericLeadingZeros", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "age")
    assert(df.first().getLong(0) == 18)
  }

  // The following two tests are not really working - need to look into Jackson's
  // JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.
  ignore("allowNonNumericNumbers off") {
    val str = """{"age": NaN}"""
    val df = spark.read.json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowNonNumericNumbers on") {
    val str = """{"age": NaN}"""
    val df = spark.read.option("allowNonNumericNumbers", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "age")
    assert(df.first().getDouble(0).isNaN)
  }

  test("allowBackslashEscapingAnyCharacter off") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "false").json(Seq(str).toDS())

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowBackslashEscapingAnyCharacter on") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "true").json(Seq(str).toDS())

    assert(df.schema.head.name == "name")
    assert(df.schema.last.name == "price")
    assert(df.first().getString(0) == "Cazen Lee")
    assert(df.first().getString(1) == "$10")
  }
}
