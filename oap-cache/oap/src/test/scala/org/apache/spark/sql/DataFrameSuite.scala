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

package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

import org.scalatest.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Uuid
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, OneRowRelation}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{FilterExec, QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.test.SQLTestData.{DecimalData, TestData2}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

class DataFrameSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("reuse exchange OAP") {
    val path = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "2",
      OapConf.OAP_PARQUET_BINARY_DATA_CACHE_ENABLED.key -> "true") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table parquet_test select * from t")
      val df = sql("select * from parquet_test")
      val join = df.join(df, "a")
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "a").join(broadcasted, "a")
      join2.explain()
      assert(
        join2.queryExecution.executedPlan
          .collect { case _: ReusedExchangeExec => true }.size == 4)
    }
    sqlContext.dropTempTable("parquet_test")
  }

  test("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        collect(join.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size === 1)
      assert(
        collect(join.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size == 1)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        collect(join2.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size == 4)
    }
  }

}


