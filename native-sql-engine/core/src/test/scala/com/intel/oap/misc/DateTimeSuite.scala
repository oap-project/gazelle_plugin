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

import java.sql.Date
import java.sql.Timestamp
import java.util.Locale
import java.util.TimeZone

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DateTimeSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val MAX_DIRECT_MEMORY = "6g"

  override protected def sparkConf: SparkConf = {

//    val zoneID = "Asia/Shanghai"
//    val locale = Locale.PRC
    val zoneID = "GMT-8"
    val locale = Locale.US

    TimeZone.setDefault(TimeZone.getTimeZone(zoneID))
    Locale.setDefault(locale)

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
        .set("spark.oap.sql.columnar.sortmergejoin", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
        .set("spark.sql.sources.useV1SourceList", "arrow,parquet")
        .set("spark.sql.session.timeZone", zoneID)
    return conf
  }

  test("timestamp type - show") {
    withTempView("timestamps") {
      val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")

      val df = sql("SELECT time FROM timestamps")
      df.explain()
      df.show(100, 50)
    }
  }

  test("timestamp type - cast to string") {
    withTempView("timestamps") {
      val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT cast(time as string) FROM timestamps"),
        Seq(
          // Different from vanilla Spark: Arrow erases timestamp's time zone in Gandiva-Java
          // Although we set time zone to America/Los_Angeles explicitly, the display
          // of timestamp still conforms to UTC.
          // See method: ArrowTypeHelper#initArrowTypeTimestamp
          Row("1970-01-01 00:00:00.000"),
          Row("1970-01-01 00:00:00.001"),
          Row("1970-01-01 00:00:00.002"),
          Row("1970-01-01 00:00:00.003")
        ))
    }
  }

  test("timestamp type - cast from string") {
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

  test("timestamp type - cast to long") {
    withTempView("timestamps") {
      val timestamps = Seq(0, 1000, 2000, 3000)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT cast(time as long) FROM timestamps"),
        Seq(
          Row(0L),
          Row(1L),
          Row(2L),
          Row(3L)
        ))
    }
  }

  test("timestamp type - cast from long") {
    withTempView("timestamps") {
      val timestamps =
        Seq(
          (0L),
          (1L),
          (2L),
          (3L)).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")

      val expected = Seq(0, 1000, 2000, 3000).map(i => Tuple1(new Timestamp(i)))
          .toDF("time")

      checkAnswer(
        sql("SELECT cast(time as timestamp) FROM timestamps"),
        expected)
    }
  }

  test("date type - show") {
    withTempView("dates") {
      val dates = (0 to 3).map(i => Tuple1(new Date(i))).toDF("time")
      dates.createOrReplaceTempView("dates")

      val df = sql("SELECT time FROM dates")
      df.explain()
      df.show(100, 50)
    }
  }

  test("date type - cast to string") {
    withTempView("dates") {
      val dates = (0L to 3L).map(i => i * 1000 * 3600 * 24)
          .map(i => Tuple1(new Date(i))).toDF("time")
      dates.createOrReplaceTempView("dates")

      checkAnswer(sql("SELECT cast(time AS string) FROM dates"),
        Seq(
          Row("1969-12-31"),
          Row("1970-01-01"),
          Row("1970-01-02"),
          Row("1970-01-03")
        ))
    }
  }

  test("date type - cast from string") {
    withTempView("dates") {
      val dates =
        Seq(
          ("1969-12-31"),
          ("1970-01-01"),
          ("1970-01-02"),
          ("1970-01-03"))
            .toDF("time")
      dates.createOrReplaceTempView("dates")

      val expected = (0L to 3L).map(i => i * 1000 * 3600 * 24)
          .map(i => Tuple1(new Date(i))).toDF("time")

      checkAnswer(
        sql("SELECT cast(time as date) FROM dates"),
        expected)
    }
  }

  test("date type - cast to timestamp") {
    withTempView("dates") {
      // UTC day 0 -> UTC-8 (LA) day -1
      val dates = (0L to 3L).map(i => i * 24 * 1000 * 3600)
          .map(i => Tuple1(new Date(i))).toDF("time")
      dates.createOrReplaceTempView("dates")

      checkAnswer(sql("SELECT cast(time AS timestamp) FROM dates"),
        (0L to 3L).map(i => Row(new Timestamp(((i - 1) * 24 + 8) * 1000 * 3600))))
    }
  }

  test("date type - cast from timestamp") {
    withTempView("dates") {
      val dates = (0L to 3L).map(i => i * 24 * 1000 * 3600)
          .map(i => Tuple1(new Timestamp(i)))
          .toDF("time")
      dates.createOrReplaceTempView("dates")

      checkAnswer(sql("SELECT cast(time AS date) FROM dates"),
        (0L to 3L).map(i => i * 24 * 1000 * 3600).map(i => Row(new Date(i))))
    }
  }

  test("date type - order by") {
    withTempView("dates") {
      val dates = (0L to 3L).map(i => i * 1000 * 3600 * 24)
          .map(i => Tuple1(new Date(i))).toDF("time")
      dates.createOrReplaceTempView("dates")

      checkAnswer(sql("SELECT time FROM dates ORDER BY time DESC"),
        (0L to 3L).reverse.map(i => i * 1000 * 3600 * 24)
            .map(i => Tuple1(new Date(i))).toDF("time"))
    }
  }

  test("timestamp type - order by") {
    withTempView("timestamps") {
      val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
      timestamps.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT time FROM timestamps ORDER BY time DESC"),
        (0 to 3).reverse.map(i => Tuple1(new Timestamp(i))).toDF("time"))
    }
  }

  // todo: fix field/literal implicit conversion in ColumnarExpressionConverter

  test("date type - join on, bhj") {
    withTempView("dates1", "dates2") {
      val dates1 = (0L to 3L).map(i => i * 1000 * 3600 * 24)
          .map(i => Tuple1(new Date(i))).toDF("time1")
      dates1.createOrReplaceTempView("dates1")
      val dates2 = (1L to 4L).map(i => i * 1000 * 3600 * 24)
          .map(i => Tuple1(new Date(i))).toDF("time2")
      dates2.createOrReplaceTempView("dates2")
      checkAnswer(
        sql("SELECT time1, time2 FROM dates1, dates2 WHERE time1 = time2"),
        (1L to 3L).map(i => i * 1000 * 3600 * 24).map(i => Row(new Date(i), new Date(i))))
    }
  }

  test("timestamp type - join on, bhj") {
    withTempView("timestamps1", "timestamps2") {
      val dates1 = (0L to 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time1")
      dates1.createOrReplaceTempView("timestamps1")
      val dates2 = (1L to 4L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time2")
      dates2.createOrReplaceTempView("timestamps2")
      checkAnswer(
        sql("SELECT time1, time2 FROM timestamps1, timestamps2 WHERE time1 = time2"),
        (1L to 3L).map(i => Row(new Timestamp(i), new Timestamp(i))))
    }
  }
}
