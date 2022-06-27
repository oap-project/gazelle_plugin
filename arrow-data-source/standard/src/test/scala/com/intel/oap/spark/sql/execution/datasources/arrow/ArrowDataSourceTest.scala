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

package com.intel.oap.spark.sql.execution.datasources.arrow

import java.io.File
import java.lang.management.ManagementFactory

import com.intel.oap.spark.sql.ArrowWriteExtension
import com.intel.oap.spark.sql.DataFrameReaderImplicits._
import com.intel.oap.spark.sql.DataFrameWriterImplicits._
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowOptions
import com.sun.management.UnixOperatingSystemMXBean
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class ArrowDataSourceTest extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val parquetFile1 = "parquet-1.parquet"
  private val parquetFile2 = "parquet-2.parquet"
  private val parquetFile3 = "parquet-3.parquet"
  private val parquetFile4 = "parquet-4.parquet"
  private val parquetFile5 = "parquet-5.parquet"
  private val parquetFile6 = "parquet-6.parquet"
  private val parquetFile7 = "parquet-7.parquet"
  private val orcFile1 = "orc-1.orc"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(100 * 1024 * 1024))
    conf.set("spark.sql.codegen.wholeStage", "false")
    conf.set("spark.unsafe.exceptionOnMemoryLeak", "false")
    conf.set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
    conf.set(SPARK_SESSION_EXTENSIONS.key, classOf[ArrowWriteExtension].getCanonicalName)
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._
    spark.read
      .json(Seq("{\"col\": -1}", "{\"col\": 0}", "{\"col\": 1}", "{\"col\": 2}", "{\"col\": null}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile1))

    spark.read
      .json(Seq("{\"col\": \"a\"}", "{\"col\": \"b\"}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile2))

    spark.read
      .json(Seq("{\"col1\": \"apple\", \"col2\": 100}", "{\"col1\": \"pear\", \"col2\": 200}",
        "{\"col1\": \"apple\", \"col2\": 300}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile3))

    spark.range(1000)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile4))

    spark.range(100)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile5))

    spark.range(100)
        .map(i => Tuple1((i, Seq(s"val1_$i", s"val2_$i"), Map((s"ka_$i", s"va_$i"),
          (s"kb_$i", s"vb_$i")))))
        .write
        .format("parquet")
        .mode("overwrite")
        .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile6))

    spark.range(100)
      .map(i => Tuple1((i, Seq(s"val1_$i", s"val2_$i"), Map((s"ka_$i", s"va_$i"),
        (s"kb_$i", s"vb_$i")))))
      .write
      .mode("overwrite")
      .orc(ArrowDataSourceTest.locateResourcePath(orcFile1))

    spark.range(100)
        .map(i => Tuple4(s"val1_$i", Seq(s"val1_$i", s"val2_$i"), Seq(Seq(s"val1_$i", s"val2_$i"),
          Seq(s"val3_$i", s"val4_$i")), Map((s"ka_$i", s"va_$i"))))
        .write
        .format("parquet")
        .mode("overwrite")
        .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile7))

  }

  override def afterAll(): Unit = {
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile1))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile2))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile3))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile4))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile5))
    super.afterAll()
  }

  test("read parquet file") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    verifyFrame(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path), 5, 1)
  }

  ignore("simple sql query on s3") {
    val path = "s3a://mlp-spark-dataset-bucket/test_arrowds_s3_small"
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("stab")
    assert(spark.sql("select id from stab").count() === 1000)
  }

  test("create catalog table") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    spark.catalog.createTable("ptab", path, "arrow")
    val sql = "select * from ptab"
    spark.sql(sql).explain()
    verifyFrame(spark.sql(sql), 5, 1)
  }

  test("create table statement") {
    spark.sql("drop table if exists ptab")
    spark.sql("create table ptab (col1 varchar(14), col2 bigint, col3 bigint) " +
      "using arrow " +
      "partitioned by (col1)")
    spark.sql("select * from ptab")
  }

  test("simple SQL query on parquet file - 1") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    verifyFrame(spark.sql("select * from ptab"), 5, 1)
    verifyFrame(spark.sql("select col from ptab"), 5, 1)
    verifyFrame(spark.sql("select col from ptab where col is not null or col is null"),
      5, 1)
  }

  test("simple SQL query on parquet file - 2") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile3)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val sqlFrame = spark.sql("select * from ptab")
    assert(
      sqlFrame.schema ===
        StructType(Seq(StructField("col1", StringType), StructField("col2", LongType))))
    val rows = sqlFrame.collect()
    assert(rows(0).get(0) == "apple")
    assert(rows(0).get(1) == 100)
    assert(rows(1).get(0) == "pear")
    assert(rows(1).get(1) == 200)
    assert(rows(2).get(0) == "apple")
    assert(rows(2).get(1) == 300)
    assert(rows.length === 3)
  }

  test("simple parquet write") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile3)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val sqlFrame = spark.sql("select * from ptab")

    val writtenPath = FileUtils.getTempDirectory + File.separator + "written.parquet"
    sqlFrame.write.mode(SaveMode.Overwrite)
        .option(ArrowOptions.KEY_TARGET_FORMAT, "parquet")
        .arrow(writtenPath)

    val frame2 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(writtenPath)
    frame2.createOrReplaceTempView("ptab2")
    val sqlFrame2 = spark.sql("select * from ptab2")

    verifyFrame(sqlFrame2, 3, 2)
  }

  test("simple SQL query on parquet file with pushed filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    spark.sql("select col from ptab where col = 1").explain(true)
    val result = spark.sql("select col from ptab where col = 1") // fixme rowcount == 2?
    assert(
      result.schema ===
        StructType(Seq(StructField("col", LongType))))
    assert(result.collect().length === 1)
  }

  test("ignore unrecognizable types when pushing down filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile2)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val rows = spark.sql("select * from ptab where col = 'b'").collect()
    assert(rows.length === 1)
  }

  ignore("dynamic partition pruning") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      SQLConf.USE_V1_SOURCE_LIST.key -> "arrow",
      SQLConf.CBO_ENABLED.key -> "true") {

      var path: String = null
      path = ArrowDataSourceTest.locateResourcePath(parquetFile4)
      spark.catalog.createTable("df1", path, "arrow")
      path = ArrowDataSourceTest.locateResourcePath(parquetFile5)
      spark.catalog.createTable("df2", path, "arrow")

      sql("ALTER TABLE df1 RECOVER PARTITIONS")
      sql("ALTER TABLE df2 RECOVER PARTITIONS")

      sql("ANALYZE TABLE df1 COMPUTE STATISTICS FOR COLUMNS id")
      sql("ANALYZE TABLE df2 COMPUTE STATISTICS FOR COLUMNS id")

      val df = sql("SELECT df1.id, df2.k FROM df1 " +
        "JOIN df2 ON df1.k = df2.k AND df2.id < 2")
      assert(df.queryExecution.executedPlan.toString().contains("dynamicpruningexpression"))
      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("count(*) without group by v2") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val df = sql("SELECT COUNT(*) FROM ptab")
    checkAnswer(df, Row(5) :: Nil)

  }

  test("read and write with case sensitive or insensitive") {
    val caseSensitiveAnalysisEnabled = Seq[Boolean](true, false)
    val v1SourceList = Seq[String]("", "arrow")
    caseSensitiveAnalysisEnabled.foreach{ caseSensitiveAnalysis =>
      v1SourceList.foreach{v1Source =>
        withSQLConf(
          SQLConf.CASE_SENSITIVE.key -> caseSensitiveAnalysis.toString,
          SQLConf.USE_V1_SOURCE_LIST.key -> v1Source) {
          withTempPath { tempPath =>
            spark.range(0, 100)
              .withColumnRenamed("id", "Id")
              .write
              .mode("overwrite")
              .arrow(tempPath.getPath)
            val selectColName = if (caseSensitiveAnalysis) {
              "Id"
            } else {
              "id"
            }
            val df = spark.read
              .arrow(tempPath.getPath)
              .filter(s"$selectColName <= 2")
            checkAnswer(df, Row(0) :: Row(1) :: Row(2) :: Nil)
          }
        }
      }
    }
  }

  test("file descriptor leak") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")

    def getFdCount: Long = {
      ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[UnixOperatingSystemMXBean]
        .getOpenFileDescriptorCount
    }

    val initialFdCount = getFdCount
    for (_ <- 0 until 100) {
      verifyFrame(spark.sql("select * from ptab"), 5, 1)
    }
    val fdGrowth = getFdCount - initialFdCount
    assert(fdGrowth < 100)
  }

  test("file descriptor leak - v1") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab2")

    def getFdCount: Long = {
      ManagementFactory.getOperatingSystemMXBean
          .asInstanceOf[UnixOperatingSystemMXBean]
          .getOpenFileDescriptorCount
    }

    val initialFdCount = getFdCount
    for (_ <- 0 until 100) {
      verifyFrame(spark.sql("select * from ptab2"), 5, 1)
    }
    val fdGrowth = getFdCount - initialFdCount
    assert(fdGrowth < 100)
  }

  test("parquet reader on data type: struct, array, map") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile6)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab3")
    val df = spark.sql("select * from ptab3")
    df.explain()
    df.show()
  }

  test("orc reader on data type: struct, array, map") {
    val path = ArrowDataSourceTest.locateResourcePath(orcFile1)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "orc")
        .arrow(path)
    frame.createOrReplaceTempView("orctab1")
    val df = spark.sql("select * from orctab1")
    df.explain()
    df.show()
  }

  test("parquet reader on struct schema: string, array[string], array[array[string]], " +
      "map[string, string]") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile7)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab4")
    val df = spark.sql("select _4['ka_0'] from ptab4")
    df.explain()
    df.show(200, false)
  }

  private val orcFile = "people.orc"
  test("read orc file") {
    val path = ArrowDataSourceTest.locateResourcePath(orcFile)
    verifyFrame(
      spark.read
          .format("arrow")
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "orc")
          .load(path), 2, 3)
  }

  test("read orc file - programmatic API ") {
    val path = ArrowDataSourceTest.locateResourcePath(orcFile)
    verifyFrame(
      spark.read
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "orc")
          .arrow(path), 2, 3)
  }

  test("create catalog table for orc") {
    val path = ArrowDataSourceTest.locateResourcePath(orcFile)
    //    spark.catalog.createTable("people", path, "arrow")
    spark.catalog.createTable("people", "arrow", Map("path" -> path, "originalFormat" -> "orc"))
    val sql = "select * from people"
    spark.sql(sql).explain()
    verifyFrame(spark.sql(sql), 2, 3)
  }

  test("simple SQL query on orc file ") {
    val path = ArrowDataSourceTest.locateResourcePath(orcFile)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "orc")
        .arrow(path)
    frame.createOrReplaceTempView("people")
    val sqlFrame = spark.sql("select * from people")
    assert(
      sqlFrame.schema ===
          StructType(Seq(StructField("name", StringType),
            StructField("age", IntegerType), StructField("job", StringType))))
    val rows = sqlFrame.collect()
    assert(rows(0).get(0) == "Jorge")
    assert(rows(0).get(1) == 30)
    assert(rows(0).get(2) == "Developer")
    assert(rows.length === 2)
  }

  private val csvFile1 = "people.csv"
  private val csvFile2 = "example.csv"
  private val csvFile3 = "example-tab.csv"

  ignore("read csv file without specifying original format") {
    // not implemented
    verifyFrame(spark.read.format("arrow")
        .load(ArrowDataSourceTest.locateResourcePath(csvFile1)), 1, 2)
  }

  test("read csv file") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile1)
    verifyFrame(
      spark.read
        .format("arrow")
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .load(path), 2, 3)
  }

  test("read csv file 2") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile2)
    verifyFrame(
      spark.read
          .format("arrow")
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
          .load(path), 34, 9)
  }

  test("read csv file 3 - tab separated") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile3)
    verifyFrame(
      spark.read
          .format("arrow")
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
          .option("delimiter", "\t")
          .load(path), 34, 9)
  }

  test("read csv file - programmatic API ") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile1)
    verifyFrame(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .arrow(path), 2, 3)
  }

  def verifyFrame(frame: DataFrame, rowCount: Int, columnCount: Int): Unit = {
    assert(frame.schema.length === columnCount)
    assert(frame.collect().length === rowCount)
  }

  def verifyCsv(frame: DataFrame): Unit = {
    // todo assert something
  }

  def verifyParquet(frame: DataFrame): Unit = {
    verifyFrame(frame, 5, 1)
  }

  def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }

  def closeAllocators(): Unit = {
    SparkMemoryUtils.contextAllocator().close()
  }
}

object ArrowDataSourceTest {
  private def locateResourcePath(resource: String): String = {
    classOf[ArrowDataSourceTest].getClassLoader.getResource("")
      .getPath.concat(File.separator).concat(resource)
  }
}
