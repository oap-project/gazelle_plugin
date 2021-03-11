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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}

class HiveResultSuite extends SharedSparkSession {
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

  test("date formatting in hive result") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { zoneId =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zoneId) {
        val dates = Seq("2018-12-28", "1582-10-03", "1582-10-04", "1582-10-15")
        val df = dates.toDF("a").selectExpr("cast(a as date) as b")
        val executedPlan1 = df.queryExecution.executedPlan
        val result = HiveResult.hiveResultString(executedPlan1)
        assert(result == dates)
        val executedPlan2 = df.selectExpr("array(b)").queryExecution.executedPlan
        val result2 = HiveResult.hiveResultString(executedPlan2)
        assert(result2 == dates.map(x => s"[$x]"))
      }
    }
  }

  test("timestamp formatting in hive result") {
    val timestamps = Seq(
      "2018-12-28 01:02:03",
      "1582-10-03 01:02:03",
      "1582-10-04 01:02:03",
      "1582-10-15 01:02:03")
    val df = timestamps.toDF("a").selectExpr("cast(a as timestamp) as b")
    val executedPlan1 = df.queryExecution.executedPlan
    val result = HiveResult.hiveResultString(executedPlan1)
    assert(result == timestamps)
    val executedPlan2 = df.selectExpr("array(b)").queryExecution.executedPlan
    val result2 = HiveResult.hiveResultString(executedPlan2)
    assert(result2 == timestamps.map(x => s"[$x]"))
  }

  test("toHiveString correctly handles UDTs") {
    val point = new ExamplePoint(50.0, 50.0)
    val tpe = new ExamplePointUDT()
    assert(HiveResult.toHiveString((point, tpe)) === "(50.0, 50.0)")
  }

  test("decimal formatting in hive result") {
    val df = Seq(new java.math.BigDecimal("1")).toDS()
    Seq(2, 6, 18).foreach { scala =>
      val executedPlan =
        df.selectExpr(s"CAST(value AS decimal(38, $scala))").queryExecution.executedPlan
      val result = HiveResult.hiveResultString(executedPlan)
      assert(result.head.split("\\.").last.length === scala)
    }

    val executedPlan = Seq(java.math.BigDecimal.ZERO).toDS()
      .selectExpr(s"CAST(value AS decimal(38, 8))").queryExecution.executedPlan
    val result = HiveResult.hiveResultString(executedPlan)
    assert(result.head === "0.00000000")
  }

  test("SHOW TABLES in hive result") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq(("testcat.ns", "tbl", "foo"), ("spark_catalog.default", "tbl", "csv")).foreach {
        case (ns, tbl, source) =>
          withTable(s"$ns.$tbl") {
            spark.sql(s"CREATE TABLE $ns.$tbl (id bigint) USING $source")
            val df = spark.sql(s"SHOW TABLES FROM $ns")
            val executedPlan = df.queryExecution.executedPlan
            assert(HiveResult.hiveResultString(executedPlan).head == tbl)
          }
      }
    }
  }

  test("DESCRIBE TABLE in hive result") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq(("testcat.ns", "tbl", "foo"), ("spark_catalog.default", "tbl", "csv")).foreach {
        case (ns, tbl, source) =>
          withTable(s"$ns.$tbl") {
            spark.sql(s"CREATE TABLE $ns.$tbl (id bigint COMMENT 'col1') USING $source")
            val df = spark.sql(s"DESCRIBE $ns.$tbl")
            val executedPlan = df.queryExecution.executedPlan
            val expected = "id                  " +
              "\tbigint              " +
              "\tcol1                "
            assert(HiveResult.hiveResultString(executedPlan).head == expected)
          }
      }
    }
  }
}
