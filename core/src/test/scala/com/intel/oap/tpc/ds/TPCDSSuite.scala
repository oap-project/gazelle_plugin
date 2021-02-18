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
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class TPCDSSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCDS_QUERIES_RESOURCE = "tpcds-queries-double"
  private val TPCDS_WRITE_PATH = "/tmp/tpcds-generated"

  private var runner: TPCRunner = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "false")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    LogManager.getRootLogger.setLevel(Level.WARN)
    new TPCDSTableGen(spark, 0.1D, TPCDS_WRITE_PATH).gen()
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
}

object TPCDSSuite {
  def stdoutLog(line: Any): Unit = {
    println("[RAM Reporter] %s".format(line))
  }
}
