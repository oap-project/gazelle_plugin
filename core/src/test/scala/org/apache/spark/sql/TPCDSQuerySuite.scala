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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf

/**
 * This test suite ensures all the TPC-DS queries can be successfully analyzed, optimized
 * and compiled without hitting the max iteration threshold.
 */
class TPCDSQuerySuite extends BenchmarkQueryTest with TPCDSSchema {

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
      //.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "50m")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.columnar.codegen.hashAggregate", "false")
      .set("spark.oap.sql.columnar.wholestagecodegen", "false")
      .set("spark.sql.columnar.window", "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      //.set("spark.sql.columnar.tmp_dir", "/codegen/nativesql/")
      .set("spark.sql.columnar.sort.broadcastJoin", "true")
      .set("spark.oap.sql.columnar.preferColumnar", "true")

  override def beforeAll(): Unit = {
    super.beforeAll()
    for (tableName <- tableNames) {
      createTable(spark, tableName)
    }
  }

  // The TPCDS queries below are based on v1.4
  val tpcdsQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
    "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

  tpcdsQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // check the plans can be properly generated
        val plan = sql(queryString).queryExecution.executedPlan
        checkGeneratedCode(plan)
      }
    }
  }

  // This list only includes TPCDS v2.7 queries that are different from v1.4 ones
  val tpcdsQueriesV2_7_0 = Seq(
    "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
    "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
    "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
    "q80a", "q86a", "q98")

  tpcdsQueriesV2_7_0.foreach { name =>
    val queryString = resourceToString(s"tpcds-v2.7.0/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(s"$name-v2.7") {
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // check the plans can be properly generated
        val plan = sql(queryString).queryExecution.executedPlan
        checkGeneratedCode(plan)
      }
    }
  }

  // These queries are from https://github.com/cloudera/impala-tpcds-kit/tree/master/queries
  val modifiedTPCDSQueries = Seq(
    "q3", "q7", "q10", "q19", "q27", "q34", "q42", "q43", "q46", "q52", "q53", "q55", "q59",
    "q63", "q65", "q68", "q73", "q79", "q89", "q98", "ss_max")

  // List up the known queries having too large code in a generated function.
  // A JIRA file for `modified-q3` is as follows;
  // [SPARK-29128] Split predicate code in OR expressions
  val blackListForMethodCodeSizeCheck = Set("modified-q3")

  modifiedTPCDSQueries.foreach { name =>
    val queryString = resourceToString(s"tpcds-modifiedQueries/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val testName = s"modified-$name"
    test(testName) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan, !blackListForMethodCodeSizeCheck.contains(testName))
    }
  }
}
