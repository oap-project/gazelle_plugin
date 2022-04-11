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

import com.intel.oap.tpc.ds.TPCDSTableGen
import com.intel.oap.tpc.util.TPCRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.test.SharedSparkSession

import java.nio.file.Files

class PartitioningSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "10000m"
  private var runner: TPCRunner = _

  private var lPath: String = _
  private var rPath: String = _
  private val scale = 100000000

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
    return conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    LogManager.getRootLogger.setLevel(Level.WARN)

    lPath = Files.createTempFile("", ".parquet").toFile.getAbsolutePath
    spark.range(scale)
        .select(col("id"), expr("id % 500").as("k"))
        .write
        .partitionBy("k")
        .format("parquet")
        .mode("overwrite")
        .parquet(lPath)

    rPath = Files.createTempFile("", ".parquet").toFile.getAbsolutePath
    spark.range(scale)
        .select(col("id"), expr("id % 500").as("k"))
        .write
        .partitionBy("k")
        .format("parquet")
        .mode("overwrite")
        .parquet(rPath)

    spark.catalog.createTable("ltab", lPath, "arrow")
    spark.catalog.recoverPartitions("ltab")
    spark.catalog.createTable("rtab", rPath, "arrow")
    spark.catalog.recoverPartitions("rtab")
  }

  test("simple shj query - memory consuming 1") {
    withSQLConf(("spark.oap.sql.columnar.forceshuffledhashjoin", "true"),
      ("spark.oap.sql.columnar.shuffledhashjoin.resizeinputpartitions", "true")) {
      val df = spark.sql("SELECT COUNT(*) AS cnt FROM ltab, rtab WHERE ltab.id = rtab.id")
      df.explain(true)
      df.show()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
