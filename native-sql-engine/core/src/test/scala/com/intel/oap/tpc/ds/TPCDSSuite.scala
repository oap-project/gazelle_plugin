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

package com.intel.oap.tpc.ds

import com.intel.oap.tpc.util.TPCRunner
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, exp, expr}
import org.apache.spark.sql.test.SharedSparkSession

import java.nio.file.Files

class TPCDSSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "20g"
  private val TPCDS_QUERIES_RESOURCE = "tpcds"
  private val TPCDS_WRITE_PATH = "/tmp/tpcds-generated"

  private var runner: TPCRunner = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.plugins", "com.intel.oap.GazellePlugin")
        .set("spark.sql.codegen.wholeStage", "true")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.oap.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
        .set("spark.oap.sql.columnar.sortmergejoin", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
        .set("spark.sql.sources.useV1SourceList", "arrow,parquet")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.oap.sql.columnar.sortmergejoin.lazyread", "true")
        .set("spark.oap.sql.columnar.autorelease", "false")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    Configurator.setRootLevel(Level.WARN)
    val tGen = new TPCDSTableGen(spark, 0.1D, TPCDS_WRITE_PATH)
    tGen.gen()
    tGen.createTables()
    runner = new TPCRunner(spark, TPCDS_QUERIES_RESOURCE)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("window queries") {
    runner.runTPCQuery("q12", 1, true)
    runner.runTPCQuery("q20", 1, true)
    runner.runTPCQuery("q36", 1, true)
    runner.runTPCQuery("q44", 1, true)
    runner.runTPCQuery("q47", 1, true)
    runner.runTPCQuery("q49", 1, true)
    runner.runTPCQuery("q51", 1, true)
    runner.runTPCQuery("q53", 1, true)
    runner.runTPCQuery("q57", 1, true)
    runner.runTPCQuery("q63", 1, true)
    runner.runTPCQuery("q67", 1, true)
    runner.runTPCQuery("q70", 1, true)
    runner.runTPCQuery("q86", 1, true)
    runner.runTPCQuery("q89", 1, true)
    runner.runTPCQuery("q98", 1, true)
  }

  test("window query") {
    runner.runTPCQuery("q67", 1, true)
  }

  test("smj query") {
    runner.runTPCQuery("q1", 1, true)
  }

  test("smj query 2") {
    runner.runTPCQuery("q24a", 1, true)
  }

  test("smj query 3") {
    runner.runTPCQuery("q95", 1, true)
  }

  test("q2") {
    runner.runTPCQuery("q2", 1, true)
  }

  test("q2 - shj") {
    withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true")) {
      runner.runTPCQuery("q2", 1, true)
    }
  }

  test("q95 - shj") {
    withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true")) {
      runner.runTPCQuery("q95", 1, true)
    }
  }

  test("simple shj query") {
    withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true"),
      ("spark.oap.sql.columnar.shuffledhashjoin.resizeinputpartitions", "true")) {
      val df = spark.sql("SELECT ws_item_sk, i_item_sk, i_class FROM web_sales, item WHERE " +
          "ws_item_sk = i_item_sk LIMIT 10")
      df.explain(true)
      df.show()
    }
  }

  test("simple shj query - left semi") {
    withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true")) {
      val df = spark.sql("SELECT i_item_sk FROM item WHERE i_item_sk IN (SELECT ws_item_sk FROM " +
          "web_sales) LIMIT 10")
      df.explain()
      df.show()
    }
  }

  //test("simple shj query - memory consuming 2") {
  //  withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true"),
  //    ("spark.oap.sql.columnar.shuffledhashjoin.resizeinputpartitions", "true")) {
  //    val df = spark.sql("WITH big as (SELECT " +
  //        "(a.i_item_sk + b.i_item_sk + c.i_item_sk + d.i_item_sk + e.i_item_sk + " +
  //        "f.i_item_sk + g.i_item_sk + h.i_item_sk + i.i_item_sk) as unq " +
  //        "FROM item a, item b, item c, item d, item e, item f, item g, item h, item i " +
  //        "WHERE a.i_item_id = b.i_item_id AND a.i_item_id = c.i_item_id " +
  //        "AND a.i_item_id = d.i_item_id AND a.i_item_id = e.i_item_id " +
  //        "AND a.i_item_id = f.i_item_id AND a.i_item_id = g.i_item_id " +
  //        "AND a.i_item_id = h.i_item_id AND a.i_item_id = i.i_item_id) " +
  //        ", big2 as" +
  //        "(SELECT q.unq as unq FROM big q, big p WHERE q.unq = p.unq)" +
  //        ", big3 as" +
  //        "(SELECT q.unq as unq FROM big2 q, big2 p WHERE q.unq = p.unq)" +
  //        "SELECT COUNT(*) FROM big3 q, big3 p WHERE q.unq = p.unq"
  //    )
  //    df.explain(true)
  //    df.show()
  //  }
  //}

  test("q47") {
    runner.runTPCQuery("q47", 1, true)
  }

  test("q59") {
    runner.runTPCQuery("q59", 1, true)
  }

  test("window function with non-decimal input") {
    val df = spark.sql("SELECT i_item_sk, i_class_id, SUM(i_category_id)" +
            " OVER (PARTITION BY i_class_id) FROM item LIMIT 1000")
    df.explain()
    df.show()
  }

  test("window function with decimal input") {
    val df = spark.sql("SELECT i_item_sk, i_class_id, SUM(i_current_price)" +
        " OVER (PARTITION BY i_class_id) FROM item LIMIT 1000")
    df.explain()
    df.show()
  }

  test("window function with date input") {
    val df = spark.sql("SELECT MAX(cc_rec_end_date) OVER (PARTITION BY cc_company)," +
        "MIN(cc_rec_end_date) OVER (PARTITION BY cc_company)" +
        "FROM call_center LIMIT 100")
    df.explain()
    df.show()
  }

  test("window function with decimal input 2") {
    val df = spark.sql("SELECT i_item_sk, i_class_id, RANK()" +
        " OVER (PARTITION BY i_class_id ORDER BY i_current_price) FROM item LIMIT 1000")
    df.explain()
    df.show()
  }

  test("window function with decimal input 3") {
    val df = spark.sql("SELECT i_item_sk, i_class_id, AVG(i_current_price)" +
        " OVER (PARTITION BY i_class_id) FROM item LIMIT 1000")
    df.explain()
    df.show()
  }

  test("window functions not used in TPC-DS") {
    val df = spark.sql("SELECT i_item_sk, i_class_id," +
      " MIN(i_current_price) OVER (PARTITION BY i_class_id)," +
      " MAX(i_current_price) OVER (PARTITION BY i_class_id)," +
      " COUNT(*) OVER (PARTITION BY i_class_id)" +
      " FROM item LIMIT 1000")
    df.explain()
    df.show()
  }

  test("simple UDF") {
    spark.udf.register("strLenScala",
      (s: String) => Option(s).map(_.length).orElse(Option(0)).get)
    val df = spark.sql("SELECT i_item_sk, i_item_desc, strLenScala(i_item_desc) FROM " +
        "item LIMIT 100")
    df.explain()
    df.show()
  }

  test("count() without group by") {
    val df = spark.sql("SELECT count(*) as cnt FROM " +
        "item LIMIT 100")
    df.explain()
    df.show()
  }

  test("collect list with decimal input") {
    val df = spark.sql("SELECT COLLECT_LIST(i_current_price)" +
        " FROM item GROUP BY i_class_id LIMIT 1000")
    df.explain()
    df.show()
  }
}

object TPCDSSuite {
  def stdoutLog(line: Any): Unit = {
    println("[RAM Reporter] %s".format(line))
  }
}
