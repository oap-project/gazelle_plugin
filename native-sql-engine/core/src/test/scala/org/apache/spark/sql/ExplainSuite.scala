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
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

trait ExplainSuiteHelper extends QueryTest with SharedSparkSession {

  protected def getNormalizedExplain(df: DataFrame, mode: ExplainMode): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain(mode.name)
    }
    output.toString.replaceAll("#\\d+", "#x")
  }

  /**
   * Get the explain from a DataFrame and run the specified action on it.
   */
  protected def withNormalizedExplain(df: DataFrame, mode: ExplainMode)(f: String => Unit) = {
    f(getNormalizedExplain(df, mode))
  }

  /**
   * Get the explain by running the sql. The explain mode should be part of the
   * sql text itself.
   */
  protected def withNormalizedExplain(queryText: String)(f: String => Unit) = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      sql(queryText).show(false)
    }
    val normalizedOutput = output.toString.replaceAll("#\\d+", "#x")
    f(normalizedOutput)
  }

  /**
   * Runs the plan and makes sure the plans contains all of the keywords.
   */
  protected def checkKeywordsExistsInExplain(
      df: DataFrame, mode: ExplainMode, keywords: String*): Unit = {
    withNormalizedExplain(df, mode) { normalizedOutput =>
      for (key <- keywords) {
        assert(normalizedOutput.contains(key))
      }
    }
  }

  protected def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    checkKeywordsExistsInExplain(df, ExtendedMode, keywords: _*)
  }
}

class ExplainSuite extends ExplainSuiteHelper with DisableAdaptiveExecutionSuite {
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
      .set("spark.oap.sql.columnar.testing", "true")

  test("SPARK-23034 show rdd names in RDD scan nodes (Dataset)") {
    val rddWithName = spark.sparkContext.parallelize(Row(1, "abc") :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName, StructType.fromDDL("c0 int, c1 string"))
    checkKeywordsExistsInExplain(df, keywords = "Scan ExistingRDD testRdd")
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (DataFrame)") {
    val rddWithName = spark.sparkContext.parallelize(ExplainSingleData(1) :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName)
    checkKeywordsExistsInExplain(df, keywords = "Scan testRdd")
  }

  test("SPARK-24850 InMemoryRelation string representation does not include cached plan") {
    val df = Seq(1).toDF("a").cache()
    checkKeywordsExistsInExplain(df,
      keywords = "InMemoryRelation", "StorageLevel(disk, memory, deserialized, 1 replicas)")
  }

  test("optimized plan should show the rewritten aggregate expression") {
    withTempView("test_agg") {
      sql(
        """
          |CREATE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
          |  (1, true), (1, false),
          |  (2, true),
          |  (3, false), (3, null),
          |  (4, null), (4, null),
          |  (5, null), (5, true), (5, false) AS test_agg(k, v)
        """.stripMargin)

      // simple explain of queries having every/some/any aggregates. Optimized
      // plan should show the rewritten aggregate expression.
      val df = sql("SELECT k, every(v), some(v), any(v) FROM test_agg GROUP BY k")
      checkKeywordsExistsInExplain(df,
        "Aggregate [k#x], [k#x, every(v#x) AS every(v)#x, some(v#x) AS some(v)#x, " +
          "any(v#x) AS any(v)#x]")
    }
  }

  test("explain inline tables cross-joins") {
    val df = sql(
      """
        |SELECT * FROM VALUES ('one', 1), ('three', null)
        |  CROSS JOIN VALUES ('one', 1), ('three', null)
      """.stripMargin)
    checkKeywordsExistsInExplain(df,
      "Join Cross",
      ":- LocalRelation [col1#x, col2#x]",
      "+- LocalRelation [col1#x, col2#x]")
  }

  test("explain table valued functions") {
    checkKeywordsExistsInExplain(sql("select * from RaNgE(2)"), "Range (0, 2, step=1, splits=None)")
    checkKeywordsExistsInExplain(sql("SELECT * FROM range(3) CROSS JOIN range(3)"),
      "Join Cross",
      ":- Range (0, 3, step=1, splits=None)",
      "+- Range (0, 3, step=1, splits=None)")
  }

  test("explain string functions") {
    // Check if catalyst combine nested `Concat`s
    val df1 = sql(
      """
        |SELECT (col1 || col2 || col3 || col4) col
        |  FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10))
      """.stripMargin)
    checkKeywordsExistsInExplain(df1,
      "Project [concat(cast(id#xL as string), cast(id#xL as string), cast(id#xL as string)" +
        ", cast(id#xL as string)) AS col#x]")

    // Check if catalyst combine nested `Concat`s if concatBinaryAsString=false
    withSQLConf(SQLConf.CONCAT_BINARY_AS_STRING.key -> "false") {
      val df2 = sql(
        """
          |SELECT ((col1 || col2) || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    string(id + 1) col2,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df2,
        "Project [concat(cast(id#xL as string), cast((id#xL + 1) as string), " +
          "cast(encode(cast((id#xL + 2) as string), utf-8) as string), " +
          "cast(encode(cast((id#xL + 3) as string), utf-8) as string)) AS col#x]")

      val df3 = sql(
        """
          |SELECT (col1 || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df3,
        "Project [concat(cast(id#xL as string), " +
          "cast(encode(cast((id#xL + 2) as string), utf-8) as string), " +
          "cast(encode(cast((id#xL + 3) as string), utf-8) as string)) AS col#x]")
    }
  }

  test("check operator precedence") {
    // We follow Oracle operator precedence in the table below that lists the levels
    // of precedence among SQL operators from high to low:
    // ---------------------------------------------------------------------------------------
    // Operator                                          Operation
    // ---------------------------------------------------------------------------------------
    // +, -                                              identity, negation
    // *, /                                              multiplication, division
    // +, -, ||                                          addition, subtraction, concatenation
    // =, !=, <, >, <=, >=, IS NULL, LIKE, BETWEEN, IN   comparison
    // NOT                                               exponentiation, logical negation
    // AND                                               conjunction
    // OR                                                disjunction
    // ---------------------------------------------------------------------------------------
    checkKeywordsExistsInExplain(sql("select 'a' || 1 + 2"),
      "Project [null AS (CAST(concat(a, CAST(1 AS STRING)) AS DOUBLE) + CAST(2 AS DOUBLE))#x]")
    checkKeywordsExistsInExplain(sql("select 1 - 2 || 'b'"),
      "Project [-1b AS concat(CAST((1 - 2) AS STRING), b)#x]")
    checkKeywordsExistsInExplain(sql("select 2 * 4  + 3 || 'b'"),
      "Project [11b AS concat(CAST(((2 * 4) + 3) AS STRING), b)#x]")
    checkKeywordsExistsInExplain(sql("select 3 + 1 || 'a' || 4 / 2"),
      "Project [4a2.0 AS concat(concat(CAST((3 + 1) AS STRING), a), " +
        "CAST((CAST(4 AS DOUBLE) / CAST(2 AS DOUBLE)) AS STRING))#x]")
    checkKeywordsExistsInExplain(sql("select 1 == 1 OR 'a' || 'b' ==  'ab'"),
      "Project [true AS ((1 = 1) OR (concat(a, b) = ab))#x]")
    checkKeywordsExistsInExplain(sql("select 'a' || 'c' == 'ac' AND 2 == 3"),
      "Project [false AS ((concat(a, c) = ac) AND (2 = 3))#x]")
  }

  test("explain for these functions; use range to avoid constant folding") {
    val df = sql("select ifnull(id, 'x'), nullif(id, 'x'), nvl(id, 'x'), nvl2(id, 'x', 'y') " +
      "from range(2)")
    checkKeywordsExistsInExplain(df,
      "Project [coalesce(cast(id#xL as string), x) AS ifnull(`id`, 'x')#x, " +
        "id#xL AS nullif(`id`, 'x')#xL, coalesce(cast(id#xL as string), x) AS nvl(`id`, 'x')#x, " +
        "x AS nvl2(`id`, 'x', 'y')#x]")
  }

  test("SPARK-26659: explain of DataWritingCommandExec should not contain duplicate cmd.nodeName") {
    withTable("temptable") {
      val df = sql("create table temptable using parquet as select * from range(2)")
      withNormalizedExplain(df, SimpleMode) { normalizedOutput =>
        assert("Create\\w*?TableAsSelectCommand".r.findAllMatchIn(normalizedOutput).length == 1)
      }
    }
  }

  test("explain formatted - check presence of subquery in case of DPP") {
    withTable("df1", "df2") {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        withTable("df1", "df2") {
          spark.range(1000).select(col("id"), col("id").as("k"))
            .write
            .partitionBy("k")
            .format("parquet")
            .mode("overwrite")
            .saveAsTable("df1")

          spark.range(100)
            .select(col("id"), col("id").as("k"))
            .write
            .partitionBy("k")
            .format("parquet")
            .mode("overwrite")
            .saveAsTable("df2")

          val sqlText =
            """
              |EXPLAIN FORMATTED SELECT df1.id, df2.k
              |FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2
              |""".stripMargin

          val expected_pattern1 =
            "Subquery:1 Hosting operator id = 1 Hosting Expression = k#xL IN subquery#x"
          val expected_pattern2 =
            "PartitionFilters: \\[isnotnull\\(k#xL\\), dynamicpruningexpression\\(k#xL " +
              "IN subquery#x\\)\\]"
          val expected_pattern3 =
            "Location: InMemoryFileIndex \\[.*org.apache.spark.sql.ExplainSuite" +
              "/df2/.*, ... 99 entries\\]"
          val expected_pattern4 =
            "Location: InMemoryFileIndex \\[.*org.apache.spark.sql.ExplainSuite" +
              "/df1/.*, ... 999 entries\\]"
          withNormalizedExplain(sqlText) { normalizedOutput =>
            assert(expected_pattern1.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern2.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern3.r.findAllMatchIn(normalizedOutput).length == 2)
            assert(expected_pattern4.r.findAllMatchIn(normalizedOutput).length == 1)
          }
        }
      }
    }
  }

  ignore("Support ExplainMode in Dataset.explain") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))

    val simpleExplainOutput = getNormalizedExplain(testDf, SimpleMode)
    assert(simpleExplainOutput.startsWith("== Physical Plan =="))
    Seq("== Parsed Logical Plan ==",
        "== Analyzed Logical Plan ==",
        "== Optimized Logical Plan ==").foreach { planType =>
      assert(!simpleExplainOutput.contains(planType))
    }
    checkKeywordsExistsInExplain(
      testDf,
      ExtendedMode,
      "== Parsed Logical Plan ==" ::
        "== Analyzed Logical Plan ==" ::
        "== Optimized Logical Plan ==" ::
        "== Physical Plan ==" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      CostMode,
      "Statistics(sizeInBytes=" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      CodegenMode,
      "WholeStageCodegen subtrees" ::
        "Generated code:" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      "* LocalTableScan (1)" ::
        "(1) LocalTableScan [codegen id :" ::
        Nil: _*)
  }

  test("Dataset.toExplainString has mode as string") {
    val df = spark.range(10).toDF
    def assertExplainOutput(mode: ExplainMode): Unit = {
      assert(df.queryExecution.explainString(mode).replaceAll("#\\d+", "#x").trim ===
        getNormalizedExplain(df, mode).trim)
    }
    assertExplainOutput(SimpleMode)
    assertExplainOutput(ExtendedMode)
    assertExplainOutput(CodegenMode)
    assertExplainOutput(CostMode)
    assertExplainOutput(FormattedMode)

    val errMsg = intercept[IllegalArgumentException] {
      ExplainMode.fromString("unknown")
    }.getMessage
    assert(errMsg.contains("Unknown explain mode: unknown"))
  }

  ignore("SPARK-31504: Output fields in formatted Explain should have determined order") {
    withTempPath { path =>
      spark.range(10).selectExpr("id as a", "id as b", "id as c", "id as d", "id as e")
        .write.mode("overwrite").parquet(path.getAbsolutePath)
      val df1 = spark.read.parquet(path.getAbsolutePath)
      val df2 = spark.read.parquet(path.getAbsolutePath)
      assert(getNormalizedExplain(df1, FormattedMode) === getNormalizedExplain(df2, FormattedMode))
    }
  }
}

class ExplainSuiteAE extends ExplainSuiteHelper with EnableAdaptiveExecutionSuite {
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
      .set("spark.oap.sql.columnar.testing", "true")

  ignore("Explain formatted") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))
    // trigger the final plan for AQE
    testDf.collect()
    //   == Physical Plan ==
    //   AdaptiveSparkPlan (14)
    //   +- * HashAggregate (13)
    //      +- CustomShuffleReader (12)
    //         +- ShuffleQueryStage (11)
    //            +- Exchange (10)
    //               +- * HashAggregate (9)
    //                  +- * Project (8)
    //                     +- * BroadcastHashJoin Inner BuildRight (7)
    //                        :- * Project (2)
    //                        :  +- * LocalTableScan (1)
    //                        +- BroadcastQueryStage (6)
    //                           +- BroadcastExchange (5)
    //                              +- * Project (4)
    //                                 +- * LocalTableScan (3)
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      s"""
         |(6) BroadcastQueryStage
         |Output [2]: [k#x, v2#x]
         |Arguments: 0
         |""".stripMargin,
      s"""
         |(11) ShuffleQueryStage
         |Output [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
         |Arguments: 1
         |""".stripMargin,
      s"""
         |(12) CustomShuffleReader
         |Input [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
         |Arguments: coalesced
         |""".stripMargin,
      s"""
         |(14) AdaptiveSparkPlan
         |Output [4]: [k#x, count(v1)#xL, sum(v1)#xL, avg(v2)#x]
         |Arguments: isFinalPlan=true
         |""".stripMargin
    )
  }
}

case class ExplainSingleData(id: Int)
