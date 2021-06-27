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

import com.intel.oap.execution.ColumnarConditionProjectExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.ColumnarProjectExec
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
        (0L to 3L).map(i => Row(new Timestamp(((i - 1) * 24) * 1000 * 3600))))
    }
  }

  // FIXME ZONE issue
  ignore("date type - cast from timestamp") {
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
  ignore("date type - join on, bhj") {
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

  test("timestamp type - join on, smj") {
    withTempView("timestamps1", "timestamps2") {
      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1") {
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

  test("timestamp type - group by") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple2(new Timestamp(i), Integer.valueOf(1))).toDF("time", "weight")
      dates.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT time, SUM(weight) as s FROM timestamps GROUP BY time"),
        Seq(Row(new Timestamp(0), Integer.valueOf(1)),
          Row(new Timestamp(1), Integer.valueOf(1)),
          Row(new Timestamp(2), Integer.valueOf(2)),
          Row(new Timestamp(3), Integer.valueOf(2)),
          Row(new Timestamp(4), Integer.valueOf(1))))
    }
  }

  test("timestamp type - window partition by") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple2(new Timestamp(i), Integer.valueOf(1))).toDF("time", "weight")
      dates.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT time, SUM(weight) OVER (PARTITION BY time) as s FROM timestamps"),
        Seq(Row(new Timestamp(0), Integer.valueOf(1)),
          Row(new Timestamp(1), Integer.valueOf(1)),
          Row(new Timestamp(2), Integer.valueOf(2)),
          Row(new Timestamp(2), Integer.valueOf(2)),
          Row(new Timestamp(3), Integer.valueOf(2)),
          Row(new Timestamp(3), Integer.valueOf(2)),
          Row(new Timestamp(4), Integer.valueOf(1))))
    }
  }

  test("timestamp type - window order by") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple2(new Timestamp(i), Integer.valueOf(1))).toDF("time", "weight")
      dates.createOrReplaceTempView("timestamps")
      checkAnswer(
        sql("SELECT time, RANK() OVER (ORDER BY time DESC) as s FROM timestamps"),
        Seq(Row(new Timestamp(0), Integer.valueOf(7)),
          Row(new Timestamp(1), Integer.valueOf(6)),
          Row(new Timestamp(2), Integer.valueOf(4)),
          Row(new Timestamp(2), Integer.valueOf(4)),
          Row(new Timestamp(3), Integer.valueOf(2)),
          Row(new Timestamp(3), Integer.valueOf(2)),
          Row(new Timestamp(4), Integer.valueOf(1))))
    }
  }

  test("datetime function - currenttimestamp") {
    withTempView("tab") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("id")
      dates.createOrReplaceTempView("tab")
      val frame = sql("SELECT CURRENT_TIMESTAMP FROM tab")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    }
  }

  test("datetime function - currentdate") {
    withTempView("tab") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("id")
      dates.createOrReplaceTempView("tab")
      val frame = sql("SELECT CURRENT_DATE FROM tab")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    }
  }

  test("datetime function - now") {
    withTempView("tab") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("id")
      dates.createOrReplaceTempView("tab")
      val frame = sql("SELECT NOW() FROM tab")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    }
  }

  test("datetime function - hour") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT HOUR(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16)),
          Row(Integer.valueOf(16))))
    }
  }

  test("datetime function - minute") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT MINUTE(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0))))
    }
  }

  test("datetime function - second") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT SECOND(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0))))
    }
  }

  test("datetime function - dayofmonth") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT DAYOFMONTH(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1))))
    }
  }

  test("datetime function - dayofweek") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT DAYOFWEEK(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(4))))
    }
  }

  test("datetime function - dayofyear") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT DAYOFYEAR(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365)),
          Row(Integer.valueOf(365))))
    }
  }

  test("datetime function - month") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT MONTH(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(1))))
    }
  }

  test("datetime function - year") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")
      val frame = sql("SELECT YEAR(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970)),
          Row(Integer.valueOf(1970))))
    }
  }

  test("datetime function - unix_date") {
    withTempView("dates") {
      val dates = (0 to 3).map(i => Tuple1(new Date(i))).toDF("time")
      dates.createOrReplaceTempView("dates")

      val frame = sql("SELECT unix_date(time) FROM dates")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(-1)),
          Row(Integer.valueOf(-1)),
          Row(Integer.valueOf(-1)),
          Row(Integer.valueOf(-1))))
    }
  }

  test("datetime function - unix_seconds") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT unix_seconds(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(0))))
    }
  }

  test("datetime function - unix_millis") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT unix_millis(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(0)),
          Row(Integer.valueOf(1)),
          Row(Integer.valueOf(2)),
          Row(Integer.valueOf(3)),
          Row(Integer.valueOf(4)),
          Row(Integer.valueOf(2)),
          Row(Integer.valueOf(3))))
    }
  }

  test("datetime function - unix_micros") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(new Timestamp(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT unix_micros(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(0)),
          Row(Integer.valueOf(1000)),
          Row(Integer.valueOf(2000)),
          Row(Integer.valueOf(3000)),
          Row(Integer.valueOf(4000)),
          Row(Integer.valueOf(2000)),
          Row(Integer.valueOf(3000))))
    }
  }

  test("datetime function - timestamp_seconds") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT timestamp_seconds(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(new Timestamp(0L * 1000)),
          Row(new Timestamp(1L * 1000)),
          Row(new Timestamp(2L * 1000)),
          Row(new Timestamp(3L * 1000)),
          Row(new Timestamp(4L * 1000)),
          Row(new Timestamp(2L * 1000)),
          Row(new Timestamp(3L * 1000))))
    }
  }

  test("datetime function - timestamp_millis") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1L, 2L, 3L, 4L, 2L, 3L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT timestamp_millis(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(new Timestamp(0L)),
          Row(new Timestamp(1L)),
          Row(new Timestamp(2L)),
          Row(new Timestamp(3L)),
          Row(new Timestamp(4L)),
          Row(new Timestamp(2L)),
          Row(new Timestamp(3L))))
    }
  }

  test("datetime function - timestamp_micros") {
    withTempView("timestamps") {
      val dates = Seq(0L, 1000L, 2000L, 3000L, 4000L, 2000L, 3000L)
          .map(i => Tuple1(java.lang.Long.valueOf(i))).toDF("time")
      dates.createOrReplaceTempView("timestamps")

      val frame = sql("SELECT timestamp_micros(time) FROM timestamps")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(new Timestamp(0L)),
          Row(new Timestamp(1L)),
          Row(new Timestamp(2L)),
          Row(new Timestamp(3L)),
          Row(new Timestamp(4L)),
          Row(new Timestamp(2L)),
          Row(new Timestamp(3L))))
    }
  }

  test("datetime function - datediff") {
    withTempView("timestamps") {
      val dates = Seq(Tuple2(new Date(1L * 24 * 60 * 60 * 1000),
        new Date(3L * 24 * 60 * 60 * 1000)),
        Tuple2(new Date(2L * 24 * 60 * 60 * 1000), new Date(2L * 24 * 60 * 60 * 1000)),
        Tuple2(new Date(3L * 24 * 60 * 60 * 1000), new Date(1L * 24 * 60 * 60 * 1000)))
          .toDF("time1", "time2")
      dates.createOrReplaceTempView("dates")

      val frame = sql("SELECT datediff(time1, time2) FROM dates")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(Integer.valueOf(-2)),
          Row(Integer.valueOf(0)),
          Row(Integer.valueOf(2))))
    }
  }

  test("datetime function - to_date") {
    withTempView("dates") {

      val dates = Seq("2009-07-30 04:17:52", "2009-07-31 04:20:52", "2009-08-01 03:15:12")
          .map(s => Tuple1(s)).toDF("time")
      dates.createOrReplaceTempView("dates")

      val frame = sql("SELECT to_date(time) FROM dates")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    }
  }

  ignore("datetime function - to_date with format") { // todo GetTimestamp IS PRIVATE ?
    withTempView("dates") {

      val dates = Seq("2009-07-30", "2009-07-31", "2009-08-01")
          .map(s => Tuple1(s)).toDF("time")
      dates.createOrReplaceTempView("dates")

      val frame = sql("SELECT to_date(time, 'yyyy-MM-dd') FROM dates")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    }
  }

  test("datetime function - unix_timestamp") {
    withTempView("dates") {
      val dates = Seq("2009-07-30", "2009-07-31", "2009-08-01")
          .map(s => Tuple1(s)).toDF("time")
      dates.createOrReplaceTempView("dates")

      val frame = sql("SELECT unix_timestamp(time, 'yyyy-MM-dd') FROM dates")
      frame.explain()
      frame.show()
      assert(frame.queryExecution.executedPlan.find(p => p
          .isInstanceOf[ColumnarConditionProjectExec]).isDefined)
      checkAnswer(
        frame,
        Seq(Row(java.lang.Long.valueOf(1248912000000L)),
          Row(java.lang.Long.valueOf(1248998400000L)),
          Row(java.lang.Long.valueOf(1249084800000L))))
    }
  }
}
