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

  test("less than codegen") {
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
}
