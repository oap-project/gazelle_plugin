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

package org.apache.spark.sql.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeAndComment, CodeGenerator}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// Disable AQE because the WholeStageCodegenExec is added when running QueryStageExec
class WholeStageCodegenSuite extends QueryTest with SharedSparkSession
  with DisableAdaptiveExecutionSuite {

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

  test("range/filter should be combined") {
    val df = spark.range(10).filter("id = 1").selectExpr("id + 1")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined)
    assert(df.collect() === Array(Row(2)))
  }

  ignore("Aggregate should be included in WholeStageCodegen") {
    val df = spark.range(10).groupBy().agg(max(col("id")), avg(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
    assert(df.collect() === Array(Row(9, 4.5)))
  }

  ignore("Aggregate with grouping keys should be included in WholeStageCodegen") {
    val df = spark.range(3).groupBy(col("id") * 2).count().orderBy(col("id") * 2)
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]).isDefined)
    assert(df.collect() === Array(Row(0, 1), Row(2, 1), Row(4, 1)))
  }

  test("BroadcastHashJoin should be included in WholeStageCodegen") {
    val rdd = spark.sparkContext.makeRDD(Seq(Row(1, "1"), Row(1, "1"), Row(2, "2")))
    val schema = new StructType().add("k", IntegerType).add("v", StringType)
    val smallDF = spark.createDataFrame(rdd, schema)
    val df = spark.range(10).join(broadcast(smallDF), col("k") === col("id"))
    // Rui: ignored plan check
    /*
    assert(df.queryExecution.executedPlan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[BroadcastHashJoinExec]).isDefined)
    */
    assert(df.collect() === Array(Row(1, 1, "1"), Row(1, 1, "1"), Row(2, 2, "2")))
  }

  test("Sort should be included in WholeStageCodegen") {
    val df = spark.range(3, 0, -1).toDF().sort(col("id"))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SortExec]).isDefined)
    assert(df.collect() === Array(Row(1), Row(2), Row(3)))
  }

  test("MapElements should be included in WholeStageCodegen") {
    import testImplicits._

    val ds = spark.range(10).map(_.toString)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SerializeFromObjectExec]).isDefined)
    assert(ds.collect() === 0.until(10).map(_.toString).toArray)
  }

  test("typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]).isDefined)
    assert(ds.collect() === Array(0, 2, 4, 6, 8))
  }

  test("back-to-back typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0).filter(_ % 3 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.find(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]).isDefined)
    assert(ds.collect() === Array(0, 6))
  }

  test("cache for primitive type should be in WholeStageCodegen with InMemoryTableScanExec") {
    import testImplicits._

    val dsInt = spark.range(3).cache()
    dsInt.count()
    val dsIntFilter = dsInt.filter(_ > 0)
    val planInt = dsIntFilter.queryExecution.executedPlan
    assert(planInt.collect {
      case WholeStageCodegenExec(FilterExec(_,
          ColumnarToRowExec(InputAdapter(_: InMemoryTableScanExec)))) => ()
    }.length == 1)
    assert(dsIntFilter.collect() === Array(1, 2))

    // cache for string type is not supported for InMemoryTableScanExec
    val dsString = spark.range(3).map(_.toString).cache()
    dsString.count()
    val dsStringFilter = dsString.filter(_ == "1")
    val planString = dsStringFilter.queryExecution.executedPlan
    assert(planString.collect {
      case _: ColumnarToRowExec => ()
    }.isEmpty)
    assert(dsStringFilter.collect() === Array("1"))
  }

  ignore("SPARK-19512 codegen for comparing structs is incorrect") {
    // this would raise CompileException before the fix
    spark.range(10)
      .selectExpr("named_struct('a', id) as col1", "named_struct('a', id+2) as col2")
      .filter("col1 = col2").count()
    // this would raise java.lang.IndexOutOfBoundsException before the fix
    spark.range(10)
      .selectExpr("named_struct('a', id, 'b', id) as col1",
        "named_struct('a',id+2, 'b',id+2) as col2")
      .filter("col1 = col2").count()
  }

  test("SPARK-21441 SortMergeJoin codegen with CodegenFallback expressions should be disabled") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
      import testImplicits._

      val df1 = Seq((1, 1), (2, 2), (3, 3)).toDF("key", "int")
      val df2 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "str")

      val df = df1.join(df2, df1("key") === df2("key"))
        .filter("int = 2 or reflect('java.lang.Integer', 'valueOf', str) = 1")
        .select("int")

      val plan = df.queryExecution.executedPlan
      assert(plan.find(p =>
        p.isInstanceOf[WholeStageCodegenExec] &&
          p.asInstanceOf[WholeStageCodegenExec].child.children(0)
            .isInstanceOf[SortMergeJoinExec]).isEmpty)
      assert(df.collect() === Array(Row(1), Row(2)))
    }
  }

  def genGroupByCode(caseNum: Int): CodeAndComment = {
    val caseExp = (1 to caseNum).map { i =>
      s"case when id > $i and id <= ${i + 1} then 1 else 0 end as v$i"
    }.toList
    val keyExp = List(
      "id",
      "(id & 1023) as k1",
      "cast(id & 1023 as double) as k2",
      "cast(id & 1023 as int) as k3")

    val ds = spark.range(10)
      .selectExpr(keyExp:::caseExp: _*)
      .groupBy("k1", "k2", "k3")
      .sum()
    val plan = ds.queryExecution.executedPlan

    val wholeStageCodeGenExec = plan.find(p => p match {
      case wp: WholeStageCodegenExec => wp.child match {
        case hp: HashAggregateExec if (hp.child.isInstanceOf[ProjectExec]) => true
        case _ => false
      }
      case _ => false
    })

    assert(wholeStageCodeGenExec.isDefined)
    wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
  }

  def genCode(ds: Dataset[_]): Seq[CodeAndComment] = {
    val plan = ds.queryExecution.executedPlan
    val wholeStageCodeGenExecs = plan.collect { case p: WholeStageCodegenExec => p }
    assert(wholeStageCodeGenExecs.nonEmpty, "WholeStageCodegenExec is expected")
    wholeStageCodeGenExecs.map(_.doCodeGen()._2)
  }

  ignore("SPARK-21871 check if we can get large code size when compiling too long functions") {
    val codeWithShortFunctions = genGroupByCode(3)
    val (_, ByteCodeStats(maxCodeSize1, _, _)) = CodeGenerator.compile(codeWithShortFunctions)
    assert(maxCodeSize1 < SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.defaultValue.get)
    val codeWithLongFunctions = genGroupByCode(50)
    val (_, ByteCodeStats(maxCodeSize2, _, _)) = CodeGenerator.compile(codeWithLongFunctions)
    assert(maxCodeSize2 > SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.defaultValue.get)
  }

  ignore("bytecode of batch file scan exceeds the limit of WHOLESTAGE_HUGE_METHOD_LIMIT") {
    import testImplicits._
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(10).select(Seq.tabulate(201) {i => ('id + i).as(s"c$i")} : _*)
      df.write.mode(SaveMode.Overwrite).parquet(path)

      withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "202",
        SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "2000") {
        // wide table batch scan causes the byte code of codegen exceeds the limit of
        // WHOLESTAGE_HUGE_METHOD_LIMIT
        val df2 = spark.read.parquet(path)
        val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
        assert(fileScan2.asInstanceOf[FileSourceScanExec].supportsColumnar)
        checkAnswer(df2, df)
      }
    }
  }

  ignore("Control splitting consume function by operators with config") {
    import testImplicits._
    val df = spark.range(10).select(Seq.tabulate(2) {i => ('id + i).as(s"c$i")} : _*)

    Seq(true, false).foreach { config =>
      withSQLConf(SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> s"$config") {
        val plan = df.queryExecution.executedPlan
        val wholeStageCodeGenExec = plan.find(p => p match {
          case wp: WholeStageCodegenExec => true
          case _ => false
        })
        assert(wholeStageCodeGenExec.isDefined)
        val code = wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
        assert(code.body.contains("project_doConsume") == config)
      }
    }
  }

  ignore("Skip splitting consume function when parameter number exceeds JVM limit") {
    // since every field is nullable we have 2 params for each input column (one for the value
    // and one for the isNull variable)
    Seq((128, false), (127, true)).foreach { case (columnNum, hasSplit) =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).select(Seq.tabulate(columnNum) {i => lit(i).as(s"c$i")} : _*)
          .write.mode(SaveMode.Overwrite).parquet(path)

        withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "255",
            SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> "true") {
          val projection = Seq.tabulate(columnNum)(i => s"c$i + c$i as newC$i")
          val df = spark.read.parquet(path).selectExpr(projection: _*)

          val plan = df.queryExecution.executedPlan
          val wholeStageCodeGenExec = plan.find {
            case _: WholeStageCodegenExec => true
            case _ => false
          }
          assert(wholeStageCodeGenExec.isDefined)
          val code = wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
          assert(code.body.contains("project_doConsume") == hasSplit)
        }
      }
    }
  }

  test("codegen stage IDs should be preserved in transformations after CollapseCodegenStages") {
    // test case adapted from DataFrameSuite to trigger ReuseExchange
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100)
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      assert(plan.find(p =>
        p.isInstanceOf[WholeStageCodegenExec] &&
          p.asInstanceOf[WholeStageCodegenExec].codegenStageId == 0).isEmpty,
        "codegen stage IDs should be preserved through ReuseExchange")
      checkAnswer(join, df.toDF)
    }
  }

  test("including codegen stage ID in generated class name should not regress codegen caching") {
    import testImplicits._

    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME.key -> "true") {
      // the same query run twice should produce identical code, which would imply a hit in
      // the generated code cache.
      val ds1 = spark.range(3).select('id + 2)
      val code1 = genCode(ds1)
      val ds2 = spark.range(3).select('id + 2)
      val code2 = genCode(ds2) // same query shape as above, deliberately
      assert(code1 == code2, "Should produce same code")
    }
  }

  test("SPARK-23598: Codegen working for lots of aggregation operations without runtime errors") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      var df = Seq((8, "bat"), (15, "mouse"), (5, "horse")).toDF("age", "name")
      for (i <- 0 until 70) {
        df = df.groupBy("name").agg(avg("age").alias("age"))
      }
      assert(df.limit(1).collect() === Array(Row("bat", 8.0)))
    }
  }

  ignore("SPARK-25767: Lazy evaluated stream of expressions handled correctly") {
    val a = Seq(1).toDF("key")
    val b = Seq((1, "a")).toDF("key", "value")
    val c = Seq(1).toDF("key")

    val ab = a.join(b, Stream("key"), "left")
    val abc = ab.join(c, Seq("key"), "left")

    checkAnswer(abc, Row(1, "a"))
  }

  test("SPARK-26680: Stream in groupBy does not cause StackOverflowError") {
    val groupByCols = Stream(col("key"))
    val df = Seq((1, 2), (2, 3), (1, 3)).toDF("key", "value")
      .groupBy(groupByCols: _*)
      .max("value")

    checkAnswer(df, Seq(Row(1, 3), Row(2, 3)))
  }

  test("SPARK-26572: evaluate non-deterministic expressions for aggregate results") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val baseTable = Seq(1, 1).toDF("idx")

      // BroadcastHashJoinExec with a HashAggregateExec child containing no aggregate expressions
      val distinctWithId = baseTable.distinct().withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
//      assert(distinctWithId.queryExecution.executedPlan.collectFirst {
//        case WholeStageCodegenExec(
//        // Rui: different plan
//          ProjectExec(_, BroadcastHashJoinExec(_, _, _, _, _, _: HashAggregateExec, _))) => true
//      }.isDefined)
      checkAnswer(distinctWithId, Seq(Row(1, 0), Row(1, 0)))

      // BroadcastHashJoinExec with a HashAggregateExec child containing a Final mode aggregate
      // expression
      val groupByWithId =
        baseTable.groupBy("idx").sum().withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
//      assert(groupByWithId.queryExecution.executedPlan.collectFirst {
//        case WholeStageCodegenExec(
//          ProjectExec(_, BroadcastHashJoinExec(_, _, _, _, _, _: HashAggregateExec, _))) => true
//      }.isDefined)
      checkAnswer(groupByWithId, Seq(Row(1, 2, 0), Row(1, 2, 0)))
    }
  }

  ignore("SPARK-28520: WholeStageCodegen does not work properly for LocalTableScanExec") {
    // Case1: LocalTableScanExec is the root of a query plan tree.
    // In this case, WholeStageCodegenExec should not be inserted
    // as the direct parent of LocalTableScanExec.
    val df = Seq(1, 2, 3).toDF
    val rootOfExecutedPlan = df.queryExecution.executedPlan

    // Ensure WholeStageCodegenExec is not inserted and
    // LocalTableScanExec is still the root.
    assert(rootOfExecutedPlan.isInstanceOf[LocalTableScanExec],
      "LocalTableScanExec should be still the root.")

    // Case2: The parent of a LocalTableScanExec supports WholeStageCodegen.
    // In this case, the LocalTableScanExec should be within a WholeStageCodegen domain
    // and no more InputAdapter is inserted as the direct parent of the LocalTableScanExec.
    val aggedDF = Seq(1, 2, 3).toDF.groupBy("value").sum()
    val executedPlan = aggedDF.queryExecution.executedPlan

    // HashAggregateExec supports WholeStageCodegen and it's the parent of
    // LocalTableScanExec so LocalTableScanExec should be within a WholeStageCodegen domain.
    assert(
      executedPlan.find {
        case WholeStageCodegenExec(
          HashAggregateExec(_, _, _, _, _, _, _: LocalTableScanExec)) => false
        case _ => false
      }.isDefined,
      "LocalTableScanExec should be within a WholeStageCodegen domain.")
  }

  ignore("Give up splitting aggregate code if a parameter length goes over the limit") {
    withSQLConf(
        SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "true",
        SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
        "spark.sql.CodeGenerator.validParamLength" -> "0") {
      withTable("t") {
        val expectedErrMsg = "Failed to split aggregate code into small functions"
        Seq(
          // Test case without keys
          "SELECT AVG(v) FROM VALUES(1) t(v)",
          // Tet case with keys
          "SELECT k, AVG(v) FROM VALUES((1, 1)) t(k, v) GROUP BY k").foreach { query =>
          val errMsg = intercept[IllegalStateException] {
            sql(query).collect
          }.getMessage
          assert(errMsg.contains(expectedErrMsg))
        }
      }
    }
  }

  ignore("Give up splitting subexpression code if a parameter length goes over the limit") {
    withSQLConf(
        SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "false",
        SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
        "spark.sql.CodeGenerator.validParamLength" -> "0") {
      withTable("t") {
        val expectedErrMsg = "Failed to split subexpression code into small functions"
        Seq(
          // Test case without keys
          "SELECT AVG(a + b), SUM(a + b + c) FROM VALUES((1, 1, 1)) t(a, b, c)",
          // Tet case with keys
          "SELECT k, AVG(a + b), SUM(a + b + c) FROM VALUES((1, 1, 1, 1)) t(k, a, b, c) " +
            "GROUP BY k").foreach { query =>
          val e = intercept[Exception] {
            sql(query).collect
          }.getCause
          assert(e.isInstanceOf[IllegalStateException])
          assert(e.getMessage.contains(expectedErrMsg))
        }
      }
    }
  }
}
