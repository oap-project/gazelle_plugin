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

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameSelfJoinSuite extends QueryTest with SharedSparkSession {
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

  test("join - join using self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")

    // self join
    checkAnswer(
      df.join(df, "int"),
      Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") :: Nil)
  }

  test("join - self join") {
    val df1 = testData.select(testData("key")).as("df1")
    val df2 = testData.select(testData("key")).as("df2")

    checkAnswer(
      df1.join(df2, $"df1.key" === $"df2.key"),
      sql("SELECT a.key, b.key FROM testData a JOIN testData b ON a.key = b.key")
        .collect().toSeq)
  }

  test("join - self join auto resolve ambiguity with case insensitivity") {
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("Key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("Key")),
      Row(2, "2", 2, "2") :: Nil)
  }

  test("join - using aliases after self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    checkAnswer(
      df.as("x").join(df.as("y"), $"x.str" === $"y.str").groupBy("x.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)

    checkAnswer(
      df.as("x").join(df.as("y"), $"x.str" === $"y.str").groupBy("y.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("[SPARK-6231] join - self join auto resolve ambiguity") {
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("key")),
      Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df, df("key") === df("key") && df("value") === 1),
      Row(1, "1", 1, "1") :: Nil)

    val left = df.groupBy("key").agg(count("*"))
    val right = df.groupBy("key").agg(sum("key"))
    checkAnswer(
      left.join(right, left("key") === right("key")),
      Row(1, 1, 1, 1) :: Row(2, 1, 2, 2) :: Nil)
  }

  private def assertAmbiguousSelfJoin(df: => DataFrame): Unit = {
    val e = intercept[AnalysisException](df)
    assert(e.message.contains("ambiguous"))
  }

  test("SPARK-28344: fail ambiguous self join - column ref in join condition") {
    val df1 = spark.range(3)
    val df2 = df1.filter($"id" > 0)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      // `df1("id") > df2("id")` is always false.
      checkAnswer(df1.join(df2, df1("id") > df2("id")), Nil)

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDf1 = df1.alias("left")
      val aliasedDf2 = df2.as("right")
      checkAnswer(
        aliasedDf1.join(aliasedDf2, $"left.id" > $"right.id"),
        Seq(Row(2, 1)))
    }

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2, df1("id") > df2("id")))
    }
  }

  test("SPARK-28344: fail ambiguous self join - Dataset.colRegex as column ref") {
    val df1 = spark.range(3)
    val df2 = df1.filter($"id" > 0)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2, df1.colRegex("id") > df2.colRegex("id")))
    }
  }

  test("SPARK-28344: fail ambiguous self join - Dataset.col with nested field") {
    val df1 = spark.read.json(Seq("""{"a": {"b": 1, "c": 1}}""").toDS())
    val df2 = df1.filter($"a.b" > 0)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2, df1("a.b") > df2("a.c")))
    }
  }

  test("SPARK-28344: fail ambiguous self join - column ref in Project") {
    val df1 = spark.range(3)
    val df2 = df1.filter($"id" > 0)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      // `df2("id")` actually points to the column of `df1`.
      checkAnswer(df1.join(df2).select(df2("id")), Seq(0, 0, 1, 1, 2, 2).map(Row(_)))

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDf1 = df1.alias("left")
      val aliasedDf2 = df2.as("right")
      checkAnswer(
        aliasedDf1.join(aliasedDf2).select($"right.id"),
        Seq(1, 1, 1, 2, 2, 2).map(Row(_)))
    }

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2).select(df2("id")))
    }
  }

  test("SPARK-28344: fail ambiguous self join - join three tables") {
    val df1 = spark.range(3)
    val df2 = df1.filter($"id" > 0)
    val df3 = df1.filter($"id" <= 2)
    val df4 = spark.range(1)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      // `df2("id") < df3("id")` is always false
      checkAnswer(df1.join(df2).join(df3, df2("id") < df3("id")), Nil)
      // `df2("id")` actually points to the column of `df1`.
      checkAnswer(
        df1.join(df4).join(df2).select(df2("id")),
        Seq(0, 0, 1, 1, 2, 2).map(Row(_)))
      // `df4("id")` is not ambiguous.
      checkAnswer(
        df1.join(df4).join(df2).select(df4("id")),
        Seq(0, 0, 0, 0, 0, 0).map(Row(_)))

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDf1 = df1.alias("x")
      val aliasedDf2 = df2.as("y")
      val aliasedDf3 = df3.as("z")
      checkAnswer(
        aliasedDf1.join(aliasedDf2).join(aliasedDf3, $"y.id" < $"z.id"),
        Seq(Row(0, 1, 2), Row(1, 1, 2), Row(2, 1, 2)))
      checkAnswer(
        aliasedDf1.join(df4).join(aliasedDf2).select($"y.id"),
        Seq(1, 1, 1, 2, 2, 2).map(Row(_)))
    }

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2).join(df3, df2("id") < df3("id")))
      assertAmbiguousSelfJoin(df1.join(df4).join(df2).select(df2("id")))
    }
  }

  test("SPARK-28344: don't fail as ambiguous self join when there is no join") {
    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true") {
      val df = Seq(1, 1, 2, 2).toDF("a")
      val w = Window.partitionBy(df("a"))
      checkAnswer(
        df.select(df("a").alias("x"), sum(df("a")).over(w)),
        Seq((1, 2), (1, 2), (2, 4), (2, 4)).map(Row.fromTuple))
    }
  }
}
