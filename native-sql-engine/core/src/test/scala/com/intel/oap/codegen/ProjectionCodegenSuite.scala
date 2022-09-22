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

package com.intel.oap.codegen

import java.sql.Timestamp

import com.intel.oap.GazellePluginConfig
import com.intel.oap.execution.ColumnarWholeStageCodegenExec

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class ProjectionCodegenSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("less than function in codegen") {
    val intData = Seq((7, 3), (10, 15)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a < b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(false), Row(true))
      )
    }
  }

  test("less than or equal to function in codegen") {
    val intData = Seq((7, 7), (10, 15), (10, 9)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a <= b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(true), Row(true), Row(false))
      )
    }
  }

  test("greater than function in codegen") {
    val intData = Seq((7, 3), (10, 15)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a > b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(true), Row(false))
      )
    }
  }

  test("greater than or equal to function in codegen") {
    val intData = Seq((7, 7), (10, 15), (10, 9)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a >= b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(true), Row(false), Row(true))
      )
    }
  }

  test("equal function in codegen") {
    val intData = Seq((7, 3), (10, 10)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a = b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(false), Row(true))
      )
    }
  }

  test("translate function in codegen") {
    val intData = Seq(("AaBbCc", "abc", "123")).toDF("a", "b", "c")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("translate(a, b, c)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("A1B2C3"))
      )
    }
  }

  test("substr function in codegen") {
    val intData = Seq(("Spark SQL", 5)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("substr(a, b)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("k SQL"))
      )
    }
  }

  test("instr function in codegen") {
    val intData = Seq(("SparkSQL", "SQL")).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("instr(a, b)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(6))
      )
    }
  }

  test("btrim/ltrim/rtrim function in codegen") {
    val intData = Seq((" SparkSQL ")).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      var df = intData.selectExpr("trim(a)")
      var executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("SparkSQL"))
      )

      df = intData.selectExpr("ltrim(a)")
      executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("SparkSQL "))
      )

      df = intData.selectExpr("rtrim(a)")
      executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(" SparkSQL"))
      )
    }
  }

  test("upper/lower function in codegen") {
    val intData = Seq(("SparkSQL")).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      var df = intData.selectExpr("upper(a)")
      var executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("SPARKSQL"))
      )

      df = intData.selectExpr("lower(a)")
      executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row("sparksql"))
      )
    }
  }

  test("cast INT/BIGINT function in codegen") {
    val intData = Seq(("123")).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      var df = intData.selectExpr("cast(a as INT)")
      var executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(123))
      )

      df = intData.selectExpr("cast(a as BIGINT)")
      executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(123L))
      )
    }
  }

  test("cast FLOAT function in codegen") {
    val intData = Seq(("123.456")).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      var df = intData.selectExpr("cast(a as float)")
      var executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(123.456f))
      )
    }
  }

  test("round function in codegen") {
    val intData = Seq((2.4), (2.5)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("round(a)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(2), Row(3))
      )
    }
  }

  test("abs function in codegen") {
    val intData = Seq((2.0), (-3.0)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("abs(a)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(2.0), Row(3.0))
      )
    }
  }

  test("add function in codegen") {
    val intData = Seq((2.4), (2.5)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a + 1")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(3.4), Row(3.5))
      )
    }
  }

  test("subtract function in codegen") {
    val intData = Seq((2.4), (2.5)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a - 1")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(1.4), Row(1.5))
      )
    }
  }

  test("multiply function in codegen") {
    val intData = Seq((2), (3)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a * 2")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(4), Row(6))
      )
    }
  }

  test("divide function in codegen") {
    val intData = Seq((2), (4)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a / 2")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(1), Row(2))
      )
    }
  }

  test("shift left function in codegen") {
    val intData = Seq((2), (4)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("shiftleft(a, 1)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(4), Row(8))
      )
    }
  }

  test("shift right function in codegen") {
    val intData = Seq((2), (4)).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("shiftright(a, 1)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(1), Row(2))
      )
    }
  }

  test("bitwise and function in codegen") {
    val intData = Seq((3, 5)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a & b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(1))
      )
    }
  }

  test("bitwise or function in codegen") {
    val intData = Seq((3, 5)).toDF("a", "b")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = intData.selectExpr("a | b")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(
        df, Seq(Row(7))
      )
    }
  }

  test("timestamp_micros function in codegen") {
    val inputData = Seq(1000L).toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = inputData.selectExpr("timestamp_micros(a)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
      checkAnswer(df, Row(new Timestamp(1L)))
    }
  }

  // cast string to timestamp is not supported in codegen.
  test("block casting string to timestamp in codegen") {
    val inputData = Seq("1970-01-01 00:00:00.000").toDF("a")
    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
      val df = inputData.selectExpr("cast(a as timestamp)")
      val executedPlan = df.queryExecution.executedPlan
      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == false)
      // non-codegen result.
      checkAnswer(df, Row(new Timestamp(0L)))
    }
  }

}
