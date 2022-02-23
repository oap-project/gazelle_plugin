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

package com.intel.oap.misc

import java.nio.file.Files

import com.intel.oap.tpc.util.TPCRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.test.SharedSparkSession

class ComplexTypeSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "5000m"
  private var runner: TPCRunner = _

  private var lPath: String = _
  private var rPath: String = _
  private val scale = 100

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.plugins", "com.intel.oap.GazellePlugin")
        .set("spark.sql.codegen.wholeStage", "true")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.oap.sql.columnar.tmp_dir", "/tmp/")
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
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.shuffle.partitions", "50")
        .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "5")
        .set("spark.oap.sql.columnar.shuffledhashjoin.buildsizelimit", "200m")
        .set("spark.oap.sql.columnar.rowtocolumnar", "false")
        .set("spark.oap.sql.columnar.columnartorow", "false")
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
        expr("struct(1, 2)").as("struct_field"))
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
        .write
        .format("parquet")
        .mode("overwrite")
        .parquet(rPath)

    spark.catalog.createTable("ltab", lPath, "arrow")
    spark.catalog.createTable("rtab", rPath, "arrow")
  }

  test("Test Array in Shuffle split") {
    val df = spark.sql("SELECT ltab.arr_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.count() == 2)
  }

  test("Test Struct in Shuffle stage") {
    val df = spark.sql("SELECT ltab.struct_field  FROM ltab, rtab WHERE ltab.kind = rtab.kind")
    df.explain(true)
    df.show()
    assert(df.count() == 2)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
