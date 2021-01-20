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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import collection.JavaConverters._
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class JsonFunctionsSuite extends QueryTest with SharedSparkSession {
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

  test("function get_json_object") {
    val df: DataFrame = Seq(("""{"name": "alice", "age": 5}""", "")).toDF("a", "b")
    checkAnswer(
      df.selectExpr("get_json_object(a, '$.name')", "get_json_object(a, '$.age')"),
      Row("alice", "5"))
  }

  test("function get_json_object - support single quotes") {
    val df: DataFrame = Seq(("""{'name': 'fang', 'age': 5}""")).toDF("a")
    checkAnswer(
      df.selectExpr("get_json_object(a, '$.name')", "get_json_object(a, '$.age')"),
      Row("fang", "5"))
  }

  val tuples: Seq[(String, String)] =
    ("1", """{"f1": "value1", "f2": "value2", "f3": 3, "f5": 5.23}""") ::
    ("2", """{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}""") ::
    ("3", """{"f1": "value13", "f4": "value44", "f3": "value33", "f2": 2, "f5": 5.01}""") ::
    ("4", null) ::
    ("5", """{"f1": "", "f5": null}""") ::
    ("6", "[invalid JSON string]") ::
    Nil

  test("function get_json_object - null") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expected =
      Row("1", "value1", "value2", "3", null, "5.23") ::
        Row("2", "value12", "2", "value3", "4.01", null) ::
        Row("3", "value13", "2", "value33", "value44", "5.01") ::
        Row("4", null, null, null, null, null) ::
        Row("5", "", null, null, null, null) ::
        Row("6", null, null, null, null, null) ::
        Nil

    checkAnswer(
      df.select($"key", functions.get_json_object($"jstring", "$.f1"),
        functions.get_json_object($"jstring", "$.f2"),
        functions.get_json_object($"jstring", "$.f3"),
        functions.get_json_object($"jstring", "$.f4"),
        functions.get_json_object($"jstring", "$.f5")),
      expected)
  }

  test("json_tuple select") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expected =
      Row("1", "value1", "value2", "3", null, "5.23") ::
      Row("2", "value12", "2", "value3", "4.01", null) ::
      Row("3", "value13", "2", "value33", "value44", "5.01") ::
      Row("4", null, null, null, null, null) ::
      Row("5", "", null, null, null, null) ::
      Row("6", null, null, null, null, null) ::
      Nil

    checkAnswer(
      df.select($"key", functions.json_tuple($"jstring", "f1", "f2", "f3", "f4", "f5")),
      expected)

    checkAnswer(
      df.selectExpr("key", "json_tuple(jstring, 'f1', 'f2', 'f3', 'f4', 'f5')"),
      expected)
  }

  test("json_tuple filter and group") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val expr = df
      .select(functions.json_tuple($"jstring", "f1", "f2"))
      .where($"c0".isNotNull)
      .groupBy($"c1")
      .count()

    val expected = Row(null, 1) ::
      Row("2", 2) ::
      Row("value2", 1) ::
      Nil

    checkAnswer(expr, expected)
  }

  test("from_json") {
    val df = Seq("""{"a": 1}""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Row(1)) :: Nil)
  }

  test("from_json with option") {
    val df = Seq("""{"time": "26/08/2015 18:00"}""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))
  }

  test("from_json missing columns") {
    val df = Seq("""{"a": 1}""").toDS()
    val schema = new StructType().add("b", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Row(null)) :: Nil)
  }

  test("from_json invalid json") {
    val df = Seq("""{"a" 1}""").toDS()
    val schema = new StructType().add("a", IntegerType)

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Row(null)) :: Nil)
  }

  test("from_json - json doesn't conform to the array type") {
    val df = Seq("""{"a" 1}""").toDS()
    val schema = ArrayType(StringType)

    checkAnswer(df.select(from_json($"value", schema)), Seq(Row(null)))
  }

  test("from_json array support") {
    val df = Seq("""[{"a": 1, "b": "a"}, {"a": 2}, { }]""").toDS()
    val schema = ArrayType(
      StructType(
        StructField("a", IntegerType) ::
        StructField("b", StringType) :: Nil))

    checkAnswer(
      df.select(from_json($"value", schema)),
      Row(Seq(Row(1, "a"), Row(2, null), Row(null, null))))
  }

  test("from_json uses DDL strings for defining a schema - java") {
    val df = Seq("""{"a": 1, "b": "haa"}""").toDS()
    checkAnswer(
      df.select(from_json($"value", "a INT, b STRING", new java.util.HashMap[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("from_json uses DDL strings for defining a schema - scala") {
    val df = Seq("""{"a": 1, "b": "haa"}""").toDS()
    checkAnswer(
      df.select(from_json($"value", "a INT, b STRING", Map[String, String]())),
      Row(Row(1, "haa")) :: Nil)
  }

  test("to_json - struct") {
    val df = Seq(Tuple1(Tuple1(1))).toDF("a")

    checkAnswer(
      df.select(to_json($"a")),
      Row("""{"_1":1}""") :: Nil)
  }

  test("to_json - array") {
    val df = Seq(Tuple1(Tuple1(1) :: Nil)).toDF("a")
    val df2 = Seq(Tuple1(Map("a" -> 1) :: Nil)).toDF("a")

    checkAnswer(
      df.select(to_json($"a")),
      Row("""[{"_1":1}]""") :: Nil)
    checkAnswer(
      df2.select(to_json($"a")),
      Row("""[{"a":1}]""") :: Nil)
  }

  test("to_json - map") {
    val df1 = Seq(Map("a" -> Tuple1(1))).toDF("a")
    val df2 = Seq(Map("a" -> 1)).toDF("a")

    checkAnswer(
      df1.select(to_json($"a")),
      Row("""{"a":{"_1":1}}""") :: Nil)
    checkAnswer(
      df2.select(to_json($"a")),
      Row("""{"a":1}""") :: Nil)
  }

  test("to_json with option") {
    val df = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))).toDF("a")
    val options = Map("timestampFormat" -> "dd/MM/yyyy HH:mm")

    checkAnswer(
      df.select(to_json($"a", options)),
      Row("""{"_1":"26/08/2015 18:00"}""") :: Nil)
  }

  test("to_json - interval support") {
    val baseDf = Seq(Tuple1(Tuple1("-3 month 7 hours"))).toDF("a")
    val df = baseDf.select(struct($"a._1".cast(CalendarIntervalType).as("a")).as("c"))
    checkAnswer(
      df.select(to_json($"c")),
      Row("""{"a":"-3 months 7 hours"}""") :: Nil)

    val df1 = baseDf
      .select(struct(map($"a._1".cast(CalendarIntervalType), lit("a")).as("col1")).as("c"))
    checkAnswer(
      df1.select(to_json($"c")),
      Row("""{"col1":{"-3 months 7 hours":"a"}}""") :: Nil)

    val df2 = baseDf
      .select(struct(map(lit("a"), $"a._1".cast(CalendarIntervalType)).as("col1")).as("c"))
    checkAnswer(
      df2.select(to_json($"c")),
      Row("""{"col1":{"a":"-3 months 7 hours"}}""") :: Nil)
  }

  test("roundtrip in to_json and from_json - struct") {
    val dfOne = Seq(Tuple1(Tuple1(1)), Tuple1(null)).toDF("struct")
    val schemaOne = dfOne.schema(0).dataType.asInstanceOf[StructType]
    val readBackOne = dfOne.select(to_json($"struct").as("json"))
      .select(from_json($"json", schemaOne).as("struct"))
    checkAnswer(dfOne, readBackOne)

    val dfTwo = Seq(Some("""{"a":1}"""), None).toDF("json")
    val schemaTwo = new StructType().add("a", IntegerType)
    val readBackTwo = dfTwo.select(from_json($"json", schemaTwo).as("struct"))
      .select(to_json($"struct").as("json"))
    checkAnswer(dfTwo, readBackTwo)
  }

  test("roundtrip in to_json and from_json - array") {
    val dfOne = Seq(Tuple1(Tuple1(1) :: Nil), Tuple1(null :: Nil)).toDF("array")
    val schemaOne = dfOne.schema(0).dataType
    val readBackOne = dfOne.select(to_json($"array").as("json"))
      .select(from_json($"json", schemaOne).as("array"))
    checkAnswer(dfOne, readBackOne)

    val dfTwo = Seq(Some("""[{"a":1}]"""), None).toDF("json")
    val schemaTwo = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val readBackTwo = dfTwo.select(from_json($"json", schemaTwo).as("array"))
      .select(to_json($"array").as("json"))
    checkAnswer(dfTwo, readBackTwo)
  }

  test("SPARK-19637 Support to_json in SQL") {
    val df1 = Seq(Tuple1(Tuple1(1))).toDF("a")
    checkAnswer(
      df1.selectExpr("to_json(a)"),
      Row("""{"_1":1}""") :: Nil)

    val df2 = Seq(Tuple1(Tuple1(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0")))).toDF("a")
    checkAnswer(
      df2.selectExpr("to_json(a, map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row("""{"_1":"26/08/2015 18:00"}""") :: Nil)

    val errMsg1 = intercept[AnalysisException] {
      df2.selectExpr("to_json(a, named_struct('a', 1))")
    }
    assert(errMsg1.getMessage.startsWith("Must use a map() function for options"))

    val errMsg2 = intercept[AnalysisException] {
      df2.selectExpr("to_json(a, map('a', 1))")
    }
    assert(errMsg2.getMessage.startsWith(
      "A type of keys and values in map() must be string, but got"))
  }

  test("SPARK-19967 Support from_json in SQL") {
    val df1 = Seq("""{"a": 1}""").toDS()
    checkAnswer(
      df1.selectExpr("from_json(value, 'a INT')"),
      Row(Row(1)) :: Nil)

    val df2 = Seq("""{"c0": "a", "c1": 1, "c2": {"c20": 3.8, "c21": 8}}""").toDS()
    checkAnswer(
      df2.selectExpr("from_json(value, 'c0 STRING, c1 INT, c2 STRUCT<c20: DOUBLE, c21: INT>')"),
      Row(Row("a", 1, Row(3.8, 8))) :: Nil)

    val df3 = Seq("""{"time": "26/08/2015 18:00"}""").toDS()
    checkAnswer(
      df3.selectExpr(
        "from_json(value, 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy HH:mm'))"),
      Row(Row(java.sql.Timestamp.valueOf("2015-08-26 18:00:00.0"))))

    val errMsg1 = intercept[AnalysisException] {
      df3.selectExpr("from_json(value, 1)")
    }
    assert(errMsg1.getMessage.startsWith("Schema should be specified in DDL format as a string"))
    val errMsg2 = intercept[AnalysisException] {
      df3.selectExpr("""from_json(value, 'time InvalidType')""")
    }
    assert(errMsg2.getMessage.contains("DataType invalidtype is not supported"))
    val errMsg3 = intercept[AnalysisException] {
      df3.selectExpr("from_json(value, 'time Timestamp', named_struct('a', 1))")
    }
    assert(errMsg3.getMessage.startsWith("Must use a map() function for options"))
    val errMsg4 = intercept[AnalysisException] {
      df3.selectExpr("from_json(value, 'time Timestamp', map('a', 1))")
    }
    assert(errMsg4.getMessage.startsWith(
      "A type of keys and values in map() must be string, but got"))
  }

  test("SPARK-24027: from_json - map<string, int>") {
    val in = Seq("""{"a": 1, "b": 2, "c": 3}""").toDS()
    val schema =
      """
        |{
        |  "type" : "map",
        |  "keyType" : "string",
        |  "valueType" : "integer",
        |  "valueContainsNull" : true
        |}
      """.stripMargin
    val out = in.select(from_json($"value", schema, Map[String, String]()))

    assert(out.columns.head == "entries")
    checkAnswer(out, Row(Map("a" -> 1, "b" -> 2, "c" -> 3)))
  }

  test("SPARK-24027: from_json - map<string, struct>") {
    val in = Seq("""{"a": {"b": 1}}""").toDS()
    val schema = MapType(StringType, new StructType().add("b", IntegerType), true)
    val out = in.select(from_json($"value", schema))

    checkAnswer(out, Row(Map("a" -> Row(1))))
  }

  test("SPARK-24027: from_json - map<string, map<string, int>>") {
    val in = Seq("""{"a": {"b": 1}}""").toDS()
    val schema = "map<string, map<string, int>>"
    val out = in.select(from_json($"value", schema, Map.empty[String, String]))

    checkAnswer(out, Row(Map("a" -> Map("b" -> 1))))
  }

  test("SPARK-24027: roundtrip - from_json -> to_json  - map<string, string>") {
    val json = """{"a":1,"b":2,"c":3}"""
    val schema = MapType(StringType, IntegerType, true)
    val out = Seq(json).toDS().select(to_json(from_json($"value", schema)))

    checkAnswer(out, Row(json))
  }

  test("SPARK-24027: roundtrip - to_json -> from_json  - map<string, string>") {
    val in = Seq(Map("a" -> 1)).toDF()
    val schema = MapType(StringType, IntegerType, true)
    val out = in.select(from_json(to_json($"value"), schema))

    checkAnswer(out, in)
  }

  test("SPARK-24027: from_json - wrong map<string, int>") {
    val in = Seq("""{"a" 1}""").toDS()
    val schema = MapType(StringType, IntegerType)
    val out = in.select(from_json($"value", schema, Map[String, String]()))

    checkAnswer(out, Row(null))
  }

  test("SPARK-24027: from_json of a map with unsupported key type") {
    val schema = MapType(StructType(StructField("f", IntegerType) :: Nil), StringType)

    checkAnswer(Seq("""{{"f": 1}: "a"}""").toDS().select(from_json($"value", schema)),
      Row(null))
    checkAnswer(Seq("""{"{"f": 1}": "a"}""").toDS().select(from_json($"value", schema)),
      Row(null))
  }

  test("SPARK-24709: infers schemas of json strings and pass them to from_json") {
    val in = Seq("""{"a": [1, 2, 3]}""").toDS()
    val out = in.select(from_json('value, schema_of_json("""{"a": [1]}""")) as "parsed")
    val expected = StructType(StructField(
      "parsed",
      StructType(StructField(
        "a",
        ArrayType(LongType, true), true) :: Nil),
      true) :: Nil)

    assert(out.schema == expected)
  }

  test("infers schemas using options") {
    val df = spark.range(1)
      .select(schema_of_json(lit("{a:1}"), Map("allowUnquotedFieldNames" -> "true").asJava))
    checkAnswer(df, Seq(Row("struct<a:bigint>")))
  }

  test("from_json - array of primitive types") {
    val df = Seq("[1, 2, 3]").toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(from_json($"a", schema)), Seq(Row(Array(1, 2, 3))))
  }

  test("from_json - array of primitive types - malformed row") {
    val df = Seq("[1, 2 3]").toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(from_json($"a", schema)), Seq(Row(null)))
  }

  test("from_json - array of arrays") {
    withTempView("jsonTable") {
      val jsonDF = Seq("[[1], [2, 3], [4, 5, 6]]").toDF("a")
      val schema = new ArrayType(ArrayType(IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("select json[0][0], json[1][1], json[2][2] from jsonTable"),
        Seq(Row(1, 3, 6)))
    }
  }

  test("from_json - array of arrays - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("[[1], [2, 3], 4, 5, 6]]").toDF("a")
      val schema = new ArrayType(ArrayType(IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("select json[0] from jsonTable"), Seq(Row(null)))
    }
  }

  test("from_json - array of structs") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"a":2}, {"a":3}]""").toDF("a")
      val schema = new ArrayType(new StructType().add("a", IntegerType), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("select json[0], json[1], json[2] from jsonTable"),
        Seq(Row(Row(1), Row(2), Row(3))))
    }
  }

  test("from_json - array of structs - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"a:2}, {"a":3}]""").toDF("a")
      val schema = new ArrayType(new StructType().add("a", IntegerType), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("select json[0], json[1]from jsonTable"), Seq(Row(null, null)))
    }
  }

  test("from_json - array of maps") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1}, {"b":2}]""").toDF("a")
      val schema = new ArrayType(MapType(StringType, IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("""select json[0], json[1] from jsonTable"""),
        Seq(Row(Map("a" -> 1), Map("b" -> 2))))
    }
  }

  test("from_json - array of maps - malformed row") {
    withTempView("jsonTable") {
      val jsonDF = Seq("""[{"a":1} "b":2}]""").toDF("a")
      val schema = new ArrayType(MapType(StringType, IntegerType, false), false)
      jsonDF.select(from_json($"a", schema) as "json").createOrReplaceTempView("jsonTable")

      checkAnswer(sql("""select json[0] from jsonTable"""), Seq(Row(null)))
    }
  }

  test("to_json - array of primitive types") {
    val df = Seq(Array(1, 2, 3)).toDF("a")
    checkAnswer(df.select(to_json($"a")), Seq(Row("[1,2,3]")))
  }

  test("roundtrip to_json -> from_json - array of primitive types") {
    val arr = Array(1, 2, 3)
    val df = Seq(arr).toDF("a")
    checkAnswer(df.select(from_json(to_json($"a"), ArrayType(IntegerType))), Row(arr))
  }

  test("roundtrip from_json -> to_json - array of primitive types") {
    val json = "[1,2,3]"
    val df = Seq(json).toDF("a")
    val schema = new ArrayType(IntegerType, false)

    checkAnswer(df.select(to_json(from_json($"a", schema))), Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of arrays") {
    val json = "[[1],[2,3],[4,5,6]]"
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(ArrayType(IntegerType, false), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of maps") {
    val json = """[{"a":1},{"b":2}]"""
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(MapType(StringType, IntegerType, false), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("roundtrip from_json -> to_json - array of structs") {
    val json = """[{"a":1},{"a":2},{"a":3}]"""
    val jsonDF = Seq(json).toDF("a")
    val schema = new ArrayType(new StructType().add("a", IntegerType), false)

    checkAnswer(
      jsonDF.select(to_json(from_json($"a", schema))),
      Seq(Row(json)))
  }

  test("pretty print - roundtrip from_json -> to_json") {
    val json = """[{"book":{"publisher":[{"country":"NL","year":[1981,1986,1999]}]}}]"""
    val jsonDF = Seq(json).toDF("root")
    val expected =
      """[ {
        |  "book" : {
        |    "publisher" : [ {
        |      "country" : "NL",
        |      "year" : [ 1981, 1986, 1999 ]
        |    } ]
        |  }
        |} ]""".stripMargin

    checkAnswer(
      jsonDF.select(
        to_json(
          from_json($"root", schema_of_json(lit(json))),
          Map("pretty" -> "true"))),
      Seq(Row(expected)))
  }

  test("from_json invalid json - check modes") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("_unparsed", StringType)
      val badRec = """{"a" 1, "b": 11}"""
      val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

      checkAnswer(
        df.select(from_json($"value", schema, Map("mode" -> "PERMISSIVE"))),
        Row(Row(null, null, badRec)) :: Row(Row(2, 12, null)) :: Nil)

      val exception1 = intercept[SparkException] {
        df.select(from_json($"value", schema, Map("mode" -> "FAILFAST"))).collect()
      }.getMessage
      assert(exception1.contains(
        "Malformed records are detected in record parsing. Parse Mode: FAILFAST."))

      val exception2 = intercept[SparkException] {
        df.select(from_json($"value", schema, Map("mode" -> "DROPMALFORMED")))
          .collect()
      }.getMessage
      assert(exception2.contains(
        "from_json() doesn't support the DROPMALFORMED mode. " +
          "Acceptable modes are PERMISSIVE and FAILFAST."))
    }
  }

  test("corrupt record column in the middle") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("_unparsed", StringType)
      .add("b", IntegerType)
    val badRec = """{"a" 1, "b": 11}"""
    val df = Seq(badRec, """{"a": 2, "b": 12}""").toDS()

    checkAnswer(
      df.select(from_json($"value", schema, Map("columnNameOfCorruptRecord" -> "_unparsed"))),
      Row(Row(null, badRec, null)) :: Row(Row(2, null, 12)) :: Nil)
  }

  test("parse timestamps with locale") {
    Seq("en-US", "ko-KR", "zh-CN", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val ts = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse("06/11/2018 18:00")
      val timestampFormat = "dd MMM yyyy HH:mm"
      val sdf = new SimpleDateFormat(timestampFormat, locale)
      val input = Seq(s"""{"time": "${sdf.format(ts)}"}""").toDS()
      val options = Map("timestampFormat" -> timestampFormat, "locale" -> langTag)
      val df = input.select(from_json($"value", "time timestamp", options))

      checkAnswer(df, Row(Row(java.sql.Timestamp.valueOf("2018-11-06 18:00:00.0"))))
    }
  }

  test("special timestamp values") {
    Seq("now", "today", "epoch", "tomorrow", "yesterday").foreach { specialValue =>
      val input = Seq(s"""{"t": "$specialValue"}""").toDS()
      val readback = input.select(from_json($"value", lit("t timestamp"),
        Map.empty[String, String].asJava)).collect()
      assert(readback(0).getAs[Row](0).getAs[Timestamp](0).getTime >= 0)
    }
  }

  test("special date values") {
    Seq("now", "today", "epoch", "tomorrow", "yesterday").foreach { specialValue =>
      val input = Seq(s"""{"d": "$specialValue"}""").toDS()
      val readback = input.select(from_json($"value", lit("d date"),
        Map.empty[String, String].asJava)).collect()
      assert(readback(0).getAs[Row](0).getAs[Date](0).getTime >= 0)
    }
  }

  test("from_json - timestamp in micros") {
    val df = Seq("""{"time": "1970-01-01T00:00:00.123456"}""").toDS()
    val schema = new StructType().add("time", TimestampType)
    val options = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    checkAnswer(
      df.select(from_json($"value", schema, options)),
      Row(Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.123456"))))
  }

  test("to_json - timestamp in micros") {
    val s = "2019-11-18 11:56:00.123456"
    val df = Seq(java.sql.Timestamp.valueOf(s)).toDF("t").select(
      to_json(struct($"t"), Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSSSS")))
    checkAnswer(df, Row(s"""{"t":"$s"}"""))
  }

  test("json_tuple - do not truncate results") {
    Seq(2000, 2800, 8000 - 1, 8000, 8000 + 1, 65535).foreach { len =>
      val str = Array.tabulate(len)(_ => "a").mkString
      val json_tuple_result = Seq(s"""{"test":"$str"}""").toDF("json")
        .withColumn("result", json_tuple('json, "test"))
        .select('result)
        .as[String].head.length
      assert(json_tuple_result === len)
    }
  }

  test("SPARK-31065: schema_of_json - null and empty strings as strings") {
    Seq("""{"id": null}""", """{"id": ""}""").foreach { input =>
      checkAnswer(
        spark.range(1).select(schema_of_json(input)),
        Seq(Row("struct<id:string>")))
    }
  }

  test("SPARK-31065: schema_of_json - 'dropFieldIfAllNull' option") {
    val options = Map("dropFieldIfAllNull" -> "true")
    // Structs
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""{"id": "a", "drop": {"drop": null}}"""),
          options.asJava)),
      Seq(Row("struct<id:string>")))

    // Array of structs
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""[{"id": "a", "drop": {"drop": null}}]"""),
          options.asJava)),
      Seq(Row("array<struct<id:string>>")))

    // Other types are not affected.
    checkAnswer(
      spark.range(1).select(
        schema_of_json(
          lit("""null"""),
          options.asJava)),
      Seq(Row("string")))
  }

  test("optional datetime parser does not affect json time formatting") {
    val s = "2015-08-26 12:34:46"
    def toDF(p: String): DataFrame = sql(
      s"""
         |SELECT
         | to_json(
         |   named_struct('time', timestamp'$s'), map('timestampFormat', "$p")
         | )
         | """.stripMargin)
    checkAnswer(toDF("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), toDF("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"))
  }
}
