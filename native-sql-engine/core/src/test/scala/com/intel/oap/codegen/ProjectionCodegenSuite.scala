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

  // TODO: after locate is used in expression_codegen_visitor.cc.
//  test("instr function in codegen") {
//    val intData = Seq(("SparkSQL", "SQL")).toDF("a", "b")
//    withSQLConf(GazellePluginConfig.getSessionConf.enableProjectionCodegenKey -> "true") {
//      val df = intData.selectExpr("instr(a, b)")
//      val executedPlan = df.queryExecution.executedPlan
//      assert(executedPlan.children(0).isInstanceOf[ColumnarWholeStageCodegenExec] == true)
//      checkAnswer(
//        df, Seq(Row(6))
//      )
//    }
//  }

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



}
