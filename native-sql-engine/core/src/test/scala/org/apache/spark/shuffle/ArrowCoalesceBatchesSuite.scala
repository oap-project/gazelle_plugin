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

package org.apache.spark.shuffle

import java.nio.file.Files

import com.intel.oap.execution.{ArrowCoalesceBatchesExec}
import com.intel.oap.tpc.util.TPCRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.test.SharedSparkSession

class ArrowCoalesceBatchesSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "5000m"
  private var runner: TPCRunner = _

  private var lPath: String = _
  private var rPath: String = _
  private val scale = 100

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.plugins", "com.intel.oap.GazellePlugin")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.oap.sql.columnar.forceshuffledhashjoin", "true")
    return conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    LogManager.getRootLogger.setLevel(Level.WARN)

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    lPath = lfile.getAbsolutePath
    spark.range(2).select(col("id"), expr("1").as("kind"),
        expr("array(1, 2)").as("arr_field"),
      expr("array(\"hello\", \"world\")").as("arr_str_field"),
        expr("array(array(1, 2), array(3, 4))").as("arr_arr_field"),
        expr("array(struct(1, 2), struct(1, 2))").as("arr_struct_field"),
        expr("array(map(1, 2), map(3,4))").as("arr_map_field"),
        expr("struct(1, 2)").as("struct_field"),
        expr("struct(1, struct(1, 2))").as("struct_struct_field"),
        expr("struct(1, array(1, 2))").as("struct_array_field"),
        expr("map(1, 2)").as("map_field"),
        expr("map(1, map(3,4))").as("map_map_field"),
        expr("map(1, array(1, 2))").as("map_arr_field"),
        expr("map(struct(1, 2), 2)").as("map_struct_field"))
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .parquet(lPath)

    val rfile = Files.createTempFile("", ".parquet").toFile
    rfile.deleteOnExit()
    rPath = rfile.getAbsolutePath
    spark.range(2).select(col("id"), expr("id % 2").as("kind"),
      expr("array(1, 2)").as("arr_field"),
      expr("struct(1, 2)").as("struct_field"))
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .parquet(rPath)

    spark.catalog.createTable("ltab", lPath, "arrow")
    spark.catalog.createTable("rtab", rPath, "arrow")
  }

  test("Test Array in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.arr_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count == 2)
  }

  test("Test Nest Array in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.arr_arr_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count == 2)
  }

  test("Test Array_Struct in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.arr_struct_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count == 2)
  }

  test("Test Array_Map in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.arr_map_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count == 2)
  }

  test("Test Struct in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.struct_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Nest Struct in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.struct_struct_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Struct_Array in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.struct_array_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Map in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.map_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Nest Map in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.map_map_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Map_Array in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.map_arr_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Map_Struct in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.map_struct_field FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count() == 2)
  }

  test("Test Array String in CoalesceBatches") {
    val df = spark.sql("SELECT ltab.arr_str_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.printSchema()
    df.explain(true)
    df.show()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ArrowCoalesceBatchesExec]).isDefined)
    assert(df.count == 2)
  }
  
  override def afterAll(): Unit = {
    super.afterAll()
  }
}
