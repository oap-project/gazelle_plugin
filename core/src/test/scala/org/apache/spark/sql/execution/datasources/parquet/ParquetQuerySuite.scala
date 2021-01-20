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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.{DebugFilesystem, SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.execution.datasources.parquet.TestingUDT.{NestedStruct, NestedStructUDT, SingleElement}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A test suite that tests various Parquet queries.
 */
abstract class ParquetQuerySuite extends QueryTest with ParquetTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf =
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
      .set("spark.sql.parquet.enableVectorizedReader", "false")
      .set("spark.sql.orc.enableVectorizedReader", "false")
      .set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")
      .set("spark.oap.sql.columnar.testing", "true")

  test("simple select queries") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "t") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        checkAnswer(sql("SELECT _1 FROM t where t._1 > 5"), (6 until 10).map(Row.apply(_)))
        checkAnswer(sql("SELECT _1 FROM t as tmp where tmp._1 < 5"), (0 until 5).map(Row.apply(_)))
      }
    }
  }

  test("appending") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    // Query appends, don't test with both read modes.
    withParquetTable(data, "t", false) {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), (data ++ data).map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withParquetTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), data.map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
  }

  test("self-join") {
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    val data = (1 to 4).map { i =>
      val maybeInt = if (i % 2 == 0) None else Some(i)
      (maybeInt, i.toString)
    }

    // TODO: vectorized doesn't work here because it requires UnsafeRows
    withParquetTable(data, "t", false) {
      val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x._1 = y._1")
      val queryOutput = selfJoin.queryExecution.analyzed.output

      assertResult(4, "Field count mismatches")(queryOutput.size)
      assertResult(2, s"Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1._2[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> s"val_$i")))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1[0]._2 FROM t"), data.map {
        case Tuple1(Seq((_, string))) => Row(string)
      })
    }
  }

  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    withParquetTable((1 to 10).map(Tuple1.apply), "t") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        checkAnswer(sql("SELECT _1 FROM t WHERE _1 < 10"), (1 to 9).map(Row.apply(_)))
      }
    }
  }

  test("SPARK-5309 strings stored using dictionary compression in parquet") {
    withParquetTable((0 until 1000).map(i => ("same", "run_" + i /100, 1)), "t") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {

        checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2"),
          (0 until 10).map(i => Row("same", "run_" + i, 100)))

        checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t WHERE _2 = 'run_5' GROUP BY _1, _2"),
          List(Row("same", "run_5", 100)))
      }
    }
  }

  test("SPARK-6917 DecimalType should work with non-native types") {
    val data = (1 to 10).map(i => Row(Decimal(i, 18, 0), new java.sql.Timestamp(i)))
    val schema = StructType(List(StructField("d", DecimalType(18, 0), false),
      StructField("time", TimestampType, false)).toArray)
    withTempPath { file =>
      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
      df.write.parquet(file.getCanonicalPath)
      val df2 = spark.read.parquet(file.getCanonicalPath)
      checkAnswer(df2, df.collect().toSeq)
    }
  }

  test("SPARK-10634 timestamp written and read as INT64 - truncation") {
    withTable("ts") {
      sql("create table ts (c1 int, c2 timestamp) using parquet")
      sql("insert into ts values (1, timestamp'2016-01-01 10:11:12.123456')")
      sql("insert into ts values (2, null)")
      sql("insert into ts values (3, timestamp'1965-01-01 10:11:12.123456')")
      val expected = Seq(
        (1, "2016-01-01 10:11:12.123456"),
        (2, null),
        (3, "1965-01-01 10:11:12.123456"))
        .toDS().select('_1, $"_2".cast("timestamp"))
      checkAnswer(sql("select * from ts"), expected)
    }
  }

  test("SPARK-10365 timestamp written and read as INT64 - TIMESTAMP_MICROS") {
    val data = (1 to 10).map { i =>
      val ts = new java.sql.Timestamp(i)
      ts.setNanos(2000)
      Row(i, ts)
    }
    val schema = StructType(List(StructField("d", IntegerType, false),
      StructField("time", TimestampType, false)).toArray)
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      withTempPath { file =>
        val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
        df.write.parquet(file.getCanonicalPath)
        withAllParquetReaders {
          val df2 = spark.read.parquet(file.getCanonicalPath)
          checkAnswer(df2, df.collect().toSeq)
        }
      }
    }
  }

  test("Enabling/disabling merging partfiles when merging parquet schema") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        // delete summary files, so if we don't merge part-files, one column will not be included.
        Utils.deleteRecursively(new File(basePath + "/foo=1/_metadata"))
        Utils.deleteRecursively(new File(basePath + "/foo=1/_common_metadata"))
        assert(spark.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName,
      SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "true",
      ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL"
    ) {
      testSchemaMerging(2)
    }

    withSQLConf(
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName,
      SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "false"
    ) {
      testSchemaMerging(3)
    }
  }

  test("Enabling/disabling schema merging") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        assert(spark.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
      testSchemaMerging(3)
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
      testSchemaMerging(2)
    }
  }

  test("Enabling/disabling ignoreCorruptFiles") {
    def testIgnoreCorruptFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    def testIgnoreCorruptFilesWithoutSchemaInfer(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.schema("a long").parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
      testIgnoreCorruptFiles()
      testIgnoreCorruptFilesWithoutSchemaInfer()
    }

    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
      val exception = intercept[SparkException] {
        testIgnoreCorruptFiles()
      }
      assert(exception.getMessage().contains("is not a Parquet file"))
      val exception2 = intercept[SparkException] {
        testIgnoreCorruptFilesWithoutSchemaInfer()
      }
      assert(exception2.getMessage().contains("is not a Parquet file"))
    }
  }

  /**
   * this is part of test 'Enabling/disabling ignoreCorruptFiles' but run in a loop
   * to increase the chance of failure
    */
  ignore("SPARK-20407 ParquetQuerySuite 'Enabling/disabling ignoreCorruptFiles' flaky test") {
    def testIgnoreCorruptFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(
          df,
          Seq(Row(0), Row(1)))
      }
    }

    for (i <- 1 to 100) {
      DebugFilesystem.clearOpenStreams()
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
        val exception = intercept[SparkException] {
          testIgnoreCorruptFiles()
        }
        assert(exception.getMessage().contains("is not a Parquet file"))
      }
      DebugFilesystem.assertNoOpenStreams()
    }
  }

  test("SPARK-8990 DataFrameReader.parquet() should respect user specified options") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=a").toString)

      // Disables the global SQL option for schema merging
      withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
        assertResult(2) {
          // Disables schema merging via data source option
          spark.read.option("mergeSchema", "false").parquet(basePath).columns.length
        }

        assertResult(3) {
          // Enables schema merging via data source option
          spark.read.option("mergeSchema", "true").parquet(basePath).columns.length
        }
      }
    }
  }

  test("SPARK-9119 Decimal should be correctly written into parquet") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      val schema = StructType(Array(StructField("name", DecimalType(10, 5), false)))
      val rowRDD = sparkContext.parallelize(Seq(Row(Decimal("67123.45"))))
      val df = spark.createDataFrame(rowRDD, schema)
      df.write.parquet(basePath)

      val decimal = spark.read.parquet(basePath).first().getDecimal(0)
      assert(Decimal("67123.45") === Decimal(decimal))
    }
  }

  test("SPARK-10005 Schema merging for nested struct") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      def append(df: DataFrame): Unit = {
        df.write.mode(SaveMode.Append).parquet(path)
      }

      // Note that both the following two DataFrames contain a single struct column with multiple
      // nested fields.
      append((1 to 2).map(i => Tuple1((i, i))).toDF())
      append((1 to 2).map(i => Tuple1((i, i, i))).toDF())

      withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
        checkAnswer(
          spark.read.option("mergeSchema", "true").parquet(path),
          Seq(
            Row(Row(1, 1, null)),
            Row(Row(2, 2, null)),
            Row(Row(1, 1, 1)),
            Row(Row(2, 2, 2))))
      }
    }
  }

  test("SPARK-10301 requested schema clipping - same schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }
  }

  test("SPARK-11997 parquet with null partition values") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(1, 3)
        .selectExpr("if(id % 2 = 0, null, id) AS n", "id")
        .write.partitionBy("n").parquet(path)

      checkAnswer(
        spark.read.parquet(path).filter("n is null"),
        Row(2, null))
    }
  }

  // This test case is ignored because of parquet-mr bug PARQUET-370
  ignore("SPARK-10301 requested schema clipping - schemas with disjoint sets of fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(null, null)))
    }
  }

  test("SPARK-10301 requested schema clipping - requested schema contains physical schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L, null, null)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'd', id + 3) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, null, null, 3L)))
    }
  }

  test("SPARK-10301 requested schema clipping - physical schema contains requested schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 3L)))
    }
  }

  test("SPARK-10301 requested schema clipping - schemas overlap but don't contain each other") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(1L, 2L, null)))
    }
  }

  test("SPARK-10301 requested schema clipping - deeply nested struct") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', ARRAY(NAMED_STRUCT('b', id, 'c', id))) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add(
              "a",
              ArrayType(
                new StructType()
                  .add("b", LongType, nullable = true)
                  .add("d", StringType, nullable = true),
                containsNull = true),
              nullable = true),
          nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(Seq(Row(0, null)))))
    }
  }

  test("SPARK-10301 requested schema clipping - out of order") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = spark
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('c', id + 2, 'b', id + 1, 'd', id + 3) AS s")
        .coalesce(1)

      df1.write.parquet(path)
      df2.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add("a", LongType, nullable = true)
            .add("b", LongType, nullable = true)
            .add("d", LongType, nullable = true),
          nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Seq(
          Row(Row(0, 1, null)),
          Row(Row(null, 2, 4))))
    }
  }

  test("SPARK-10301 requested schema clipping - schema merging") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = spark
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      df1.write.mode(SaveMode.Append).parquet(path)
      df2.write.mode(SaveMode.Append).parquet(path)

      checkAnswer(
        spark
          .read
          .option("mergeSchema", "true")
          .parquet(path)
          .selectExpr("s.a", "s.b", "s.c"),
        Seq(
          Row(0, null, 2),
          Row(1, 2, 3)))
    }
  }

  testStandardAndLegacyModes("SPARK-10301 requested schema clipping - UDT") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark
        .range(1)
        .selectExpr(
          """NAMED_STRUCT(
            |  'f0', CAST(id AS STRING),
            |  'f1', NAMED_STRUCT(
            |    'a', CAST(id + 1 AS INT),
            |    'b', CAST(id + 2 AS LONG),
            |    'c', CAST(id + 3.5 AS DOUBLE)
            |  )
            |) AS s
          """.stripMargin)
        .coalesce(1)

      df.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("f1", new NestedStructUDT, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(NestedStruct(1, 2L, 3.5D))))
    }
  }

  test("expand UDT in StructType") {
    val schema = new StructType().add("n", new NestedStructUDT, nullable = true)
    val expected = new StructType().add("n", new NestedStructUDT().sqlType, nullable = true)
    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in ArrayType") {
    val schema = new StructType().add(
      "n",
      ArrayType(
        elementType = new NestedStructUDT,
        containsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      ArrayType(
        elementType = new NestedStructUDT().sqlType,
        containsNull = false),
      nullable = true)

    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in MapType") {
    val schema = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new NestedStructUDT,
        valueContainsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new NestedStructUDT().sqlType,
        valueContainsNull = false),
      nullable = true)

    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("SPARK-15719: disable writing summary files by default") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(path)

      val fs = FileSystem.get(spark.sessionState.newHadoopConf())
      val files = fs.listFiles(new Path(path), true)

      while (files.hasNext) {
        val file = files.next
        assert(!file.getPath.getName.contains("_metadata"))
      }
    }
  }

  test("SPARK-15804: write out the metadata to parquet file") {
    val df = Seq((1, "abc"), (2, "hello")).toDF("a", "b")
    val md = new MetadataBuilder().putString("key", "value").build()
    val dfWithmeta = df.select('a, 'b.as("b", md))

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      dfWithmeta.write.parquet(path)

      readParquetFile(path) { df =>
        assert(df.schema.last.metadata.getString("key") == "value")
      }
    }
  }

  test("SPARK-16344: array of struct with a single field named 'element'") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq(Tuple1(Array(SingleElement(42)))).toDF("f").write.parquet(path)

      checkAnswer(
        sqlContext.read.parquet(path),
        Row(Array(Row(42)))
      )
    }
  }

  test("SPARK-16632: read Parquet int32 as ByteType and ShortType") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        // When being written to Parquet, `TINYINT` and `SMALLINT` should be converted into
        // `int32 (INT_8)` and `int32 (INT_16)` respectively. However, Hive doesn't add the `INT_8`
        // and `INT_16` annotation properly (HIVE-14294). Thus, when reading files written by Hive
        // using Spark with the vectorized Parquet reader enabled, we may hit error due to type
        // mismatch.
        //
        // Here we are simulating Hive's behavior by writing a single `INT` field and then read it
        // back as `TINYINT` and `SMALLINT` in Spark to verify this issue.
        Seq(1).toDF("f").write.parquet(path)

        val withByteField = new StructType().add("f", ByteType)
        checkAnswer(spark.read.schema(withByteField).parquet(path), Row(1: Byte))

        val withShortField = new StructType().add("f", ShortType)
        checkAnswer(spark.read.schema(withShortField).parquet(path), Row(1: Short))
      }
    }
  }

  // ignored in maven test
  test("SPARK-24230: filter row group using dictionary") {
    withSQLConf(("parquet.filter.dictionary.enabled", "true")) {
      // create a table with values from 0, 2, ..., 18 that will be dictionary-encoded
      withParquetTable((0 until 100).map(i => ((i * 2) % 20, s"data-$i")), "t") {
        // search for a key that is not present so the dictionary filter eliminates all row groups
        // Fails without SPARK-24230:
        //   java.io.IOException: expecting more rows but reached last block. Read 0 out of 50
        checkAnswer(sql("SELECT _2 FROM t WHERE t._1 = 5"), Seq.empty)
      }
    }
  }

  ignore("SPARK-26677: negated null-safe equality comparison should not filter matched row groups") {
    withAllParquetReaders {
      withTempPath { path =>
        // Repeated values for dictionary encoding.
        Seq(Some("A"), Some("A"), None).toDF.repartition(1)
          .write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(stripSparkFilter(df.where("NOT (value <=> 'A')")), df)
      }
    }
  }

  test("Migration from INT96 to TIMESTAMP_MICROS timestamp type") {
    def testMigration(fromTsType: String, toTsType: String): Unit = {
      def checkAppend(write: DataFrameWriter[_] => Unit, readback: => DataFrame): Unit = {
        def data(start: Int, end: Int): Seq[Row] = (start to end).map { i =>
          val ts = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(i))
          ts.setNanos(123456000)
          Row(ts)
        }
        val schema = new StructType().add("time", TimestampType)
        val df1 = spark.createDataFrame(sparkContext.parallelize(data(0, 1)), schema)
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> fromTsType) {
          write(df1.write)
        }
        val df2 = spark.createDataFrame(sparkContext.parallelize(data(2, 10)), schema)
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> toTsType) {
          write(df2.write.mode(SaveMode.Append))
        }
        withAllParquetReaders {
          checkAnswer(readback, df1.unionAll(df2))
        }
      }

      Seq(false, true).foreach { mergeSchema =>
        withTempPath { file =>
          checkAppend(_.parquet(file.getCanonicalPath),
            spark.read.option("mergeSchema", mergeSchema).parquet(file.getCanonicalPath))
        }

        withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> mergeSchema.toString) {
          val tableName = "parquet_timestamp_migration"
          withTable(tableName) {
            checkAppend(_.saveAsTable(tableName), spark.table(tableName))
          }
        }
      }
    }

    testMigration(fromTsType = "INT96", toTsType = "TIMESTAMP_MICROS")
    testMigration(fromTsType = "TIMESTAMP_MICROS", toTsType = "INT96")
  }
}

class ParquetV1QuerySuite extends ParquetQuerySuite {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  test("returning batch for wide table") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "10") {
      withTempPath { dir =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
          val path = dir.getCanonicalPath
          val df = spark.range(10).select(Seq.tabulate(11) { i => ('id + i).as(s"c$i") }: _*)
          df.write.mode(SaveMode.Overwrite).parquet(path)

          // donot return batch, because whole stage codegen is disabled for wide table (>200 columns)
          val df2 = spark.read.parquet(path)
          val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
          assert(!fileScan2.asInstanceOf[FileSourceScanExec].supportsColumnar)
          checkAnswer(df2, df)

          // return batch
          val columns = Seq.tabulate(9) { i => s"c$i" }
          val df3 = df2.selectExpr(columns: _*)
          val fileScan3 = df3.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
          // Rui: we disabled columnar reader
          assert(!fileScan3.asInstanceOf[FileSourceScanExec].supportsColumnar)
          checkAnswer(df3, df.selectExpr(columns: _*))
        }
      }
    }
  }
}

class ParquetV2QuerySuite extends ParquetQuerySuite {
  import testImplicits._

  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("returning batch for wide table") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "10") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(10).select(Seq.tabulate(11) {i => ('id + i).as(s"c$i")} : _*)
        df.write.mode(SaveMode.Overwrite).parquet(path)

        // donot return batch, because whole stage codegen is disabled for wide table (>200 columns)
        val df2 = spark.read.parquet(path)
        val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
        val parquetScan2 = fileScan2.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
        // The method `supportColumnarReads` in Parquet doesn't depends on the input partition.
        // Here we can pass null input partition to the method for testing propose.
        assert(!parquetScan2.createReaderFactory().supportColumnarReads(null))
        checkAnswer(df2, df)

        // return batch
        val columns = Seq.tabulate(9) {i => s"c$i"}
        val df3 = df2.selectExpr(columns : _*)
        val fileScan3 = df3.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
        val parquetScan3 = fileScan3.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
        // Rui: we disabled columnar reader
        assert(!parquetScan3.createReaderFactory().supportColumnarReads(null))
        checkAnswer(df3, df.selectExpr(columns : _*))
      }
    }
  }
}

object TestingUDT {
  case class SingleElement(element: Long)

  @SQLUserDefinedType(udt = classOf[NestedStructUDT])
  case class NestedStruct(a: Integer, b: Long, c: Double)

  class NestedStructUDT extends UserDefinedType[NestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(n: NestedStruct): Any = {
      val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      row.setInt(0, n.a)
      row.setLong(1, n.b)
      row.setDouble(2, n.c)
    }

    override def userClass: Class[NestedStruct] = classOf[NestedStruct]

    override def deserialize(datum: Any): NestedStruct = {
      datum match {
        case row: InternalRow =>
          NestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }
}
