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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf.BUCKETING_MAX_BUCKETS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class CreateTableAsSelectSuite extends DataSourceTest with SharedSparkSession {
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

  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val ds = (1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}""").toDS()
    spark.read.json(ds).createOrReplaceTempView("jt")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("jt")
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    path = Utils.createTempDir()
    path.delete()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(path)
    super.afterEach()
  }

  test("CREATE TABLE USING AS SELECT") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))
    }
  }

  // ignored in maven test
  ignore("CREATE TABLE USING AS SELECT based on the file without write permission") {
    // setWritable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)
    val childPath = new File(path.toString, "child")
    path.mkdir()
    path.setWritable(false)

    val e = intercept[SparkException] {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${childPath.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)
      sql("SELECT a, b FROM jsonTable").collect()
    }

    assert(e.getMessage().contains("Job aborted"))
    path.setWritable(true)
  }

  // ignored in maven test
  ignore("create a table, drop it and create another one with the same name") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Creates a table of the same name with flag "if not exists", nothing happens
      sql(
        s"""
           |CREATE TABLE IF NOT EXISTS jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a * 4 FROM jt
         """.stripMargin)
      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Explicitly drops the table and deletes the underlying data.
      sql("DROP TABLE jsonTable")
      if (path.exists()) Utils.deleteRecursively(path)

      // Creates a table of the same name again, this time we succeed.
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT b FROM jt"))
    }
  }

  test("disallows CREATE TEMPORARY TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE TEMPORARY TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toURI}')
             |PARTITIONED BY (a)
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage
      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE TEMPORARY TABLE ... USING ... AS query"))
    }
  }

  test("disallows CREATE EXTERNAL TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE EXTERNAL TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toURI}')
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage

      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE EXTERNAL TABLE ..."))
    }
  }

  test("create table using as select - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.partitionColumnNames == Seq("a"))
    }
  }

  test("create table using as select - with valid number of buckets") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.bucketSpec == Option(BucketSpec(5, Seq("a"), Seq("b"))))
    }
  }

  test("create table using as select - with invalid number of buckets") {
    withTable("t") {
      Seq(0, 100001).foreach(numBuckets => {
        val e = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE t USING PARQUET
               |OPTIONS (PATH '${path.toURI}')
               |CLUSTERED BY (a) SORTED BY (b) INTO $numBuckets BUCKETS
               |AS SELECT 1 AS a, 2 AS b
             """.stripMargin
          )
        }.getMessage
        assert(e.contains("Number of buckets should be greater than 0 but less than"))
      })
    }
  }

  test("create table using as select - with overriden max number of buckets") {
    def createTableSql(numBuckets: Int): String =
      s"""
         |CREATE TABLE t USING PARQUET
         |OPTIONS (PATH '${path.toURI}')
         |CLUSTERED BY (a) SORTED BY (b) INTO $numBuckets BUCKETS
         |AS SELECT 1 AS a, 2 AS b
       """.stripMargin

    val maxNrBuckets: Int = 200000
    val catalog = spark.sessionState.catalog
    withSQLConf(BUCKETING_MAX_BUCKETS.key -> maxNrBuckets.toString) {

      // Within the new limit
      Seq(100001, maxNrBuckets).foreach(numBuckets => {
        withTable("t") {
          sql(createTableSql(numBuckets))
          val table = catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.bucketSpec == Option(BucketSpec(numBuckets, Seq("a"), Seq("b"))))
        }
      })

      // Over the new limit
      withTable("t") {
        val e = intercept[AnalysisException](sql(createTableSql(maxNrBuckets + 1)))
        assert(
          e.getMessage.contains("Number of buckets should be greater than 0 but less than "))
      }
    }
  }

  test("SPARK-17409: CTAS of decimal calculation") {
    withTable("tab2") {
      withTempView("tab1") {
        spark.range(99, 101).createOrReplaceTempView("tab1")
        val sqlStmt =
          "SELECT id, cast(id as long) * cast('1.0' as decimal(38, 18)) as num FROM tab1"
        sql(s"CREATE TABLE tab2 USING PARQUET AS $sqlStmt")
        checkAnswer(spark.table("tab2"), sql(sqlStmt))
      }
    }
  }

  test("specifying the column list for CTAS") {
    withTable("t") {
      val e = intercept[ParseException] {
        sql("CREATE TABLE t (a int, b int) USING parquet AS SELECT 1, 2")
      }.getMessage
      assert(e.contains("Schema may not be specified in a Create Table As Select (CTAS)"))
    }
  }
}
