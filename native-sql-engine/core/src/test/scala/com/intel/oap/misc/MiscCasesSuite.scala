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

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MiscCasesSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val MAX_DIRECT_MEMORY = "6g"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "true")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
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
    return conf
  }

  test("timestamp data type - show") {
    withTempView("timestamps") {
      val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")

      val df = sql("SELECT time FROM timestamps")
      df.explain()
      df.show(100, 50)
    }
  }

  test("timestamp data type - cast to string") {
    withTempView("timestamps") {
      val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT cast(time as string) FROM timestamps"),
        Seq(
          Row("1970-01-01 00:00:00.000"),
          Row("1970-01-01 00:00:00.001"),
          Row("1970-01-01 00:00:00.002"),
          Row("1970-01-01 00:00:00.003")
        ))
    }
  }

  test("timestamp data type - cast from string") {
    withTempView("timestamps") {
      val timestamps =
        Seq(
          ("1970-01-01 00:00:00.000"),
          ("1970-01-01 00:00:00.001"),
          ("1970-01-01 00:00:00.002"),
          ("1970-01-01 00:00:00.003"))
            .toDF("time")
      timestamps.createOrReplaceTempView("timestamps")

      val expected = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")

      checkAnswer(
        sql("SELECT cast(time as timestamp) FROM timestamps"),
        expected)
    }
  }
}
