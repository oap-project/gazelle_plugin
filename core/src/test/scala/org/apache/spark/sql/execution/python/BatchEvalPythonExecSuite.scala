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

package org.apache.spark.sql.execution.python

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, In}
import org.apache.spark.sql.execution.{FilterExec, InputAdapter, SparkPlanTest, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType}

class BatchEvalPythonExecSuite extends SparkPlanTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

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
    spark.udf.registerPython("dummyPythonUDF", new MyDummyPythonUDF)
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.functionRegistry.dropFunction(FunctionIdentifier("dummyPythonUDF"))
    } finally {
      super.afterAll()
    }
  }

  ignore("Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(b) and dummyPythonUDF(a) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: AttributeReference),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  ignore("Nested Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a, dummyPythonUDF(a, b)) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(_: AttributeReference, InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  ignore("Python UDF: no push down on non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("b > 4 and dummyPythonUDF(a) and rand() > 0.3")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  ignore("Python UDF: push down on deterministic predicates after the first non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a) and rand() > 0.3 and b > 4")

    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF refers to the attributes from more than one child") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = Seq(("Hello", 4)).toDF("c", "d")
    val joinDF = df.crossJoin(df2).where("dummyPythonUDF(a, c) == dummyPythonUDF(d, c)")
    val qualifiedPlanNodes = collect(joinDF.queryExecution.executedPlan) {
      case b: BatchEvalPythonExec => b
    }
    assert(qualifiedPlanNodes.size == 1)
  }

  test("SPARK-28422: GROUPED_AGG pandas_udf should work without group by clause") {
    val aggPandasUdf = new MyDummyGroupedAggPandasUDF
    spark.udf.registerPython("dummyGroupedAggPandasUDF", aggPandasUdf)

    withTempView("table") {
      val df = spark.range(0, 100)
      df.createTempView("table")

      val agg1 = df.agg(aggPandasUdf(df("id")))
      val agg2 = sql("select dummyGroupedAggPandasUDF(id) from table")

      comparePlans(agg1.queryExecution.optimizedPlan, agg2.queryExecution.optimizedPlan)
    }
  }
}

// This Python UDF is dummy and just for testing. Unable to execute.
class DummyUDF extends PythonFunction(
  command = Array[Byte](),
  envVars = Map("" -> "").asJava,
  pythonIncludes = ArrayBuffer("").asJava,
  pythonExec = "",
  pythonVer = "",
  broadcastVars = null,
  accumulator = null)

class MyDummyPythonUDF extends UserDefinedPythonFunction(
  name = "dummyUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
  udfDeterministic = true)

class MyDummyGroupedAggPandasUDF extends UserDefinedPythonFunction(
  name = "dummyGroupedAggPandasUDF",
  func = new DummyUDF,
  dataType = DoubleType,
  pythonEvalType = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
  udfDeterministic = true)

class MyDummyScalarPandasUDF extends UserDefinedPythonFunction(
  name = "dummyScalarPandasUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
  udfDeterministic = true)
