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

package org.apache.spark.sql.streaming.continuous

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED
import org.apache.spark.sql.streaming.OutputMode

class ContinuousAggregationSuite extends ContinuousSuiteBase {
  import testImplicits._

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

  test("not enabled") {
    val ex = intercept[AnalysisException] {
      val input = ContinuousMemoryStream.singlePartition[Int]
      testStream(input.toDF().agg(max('value)), OutputMode.Complete)()
    }

    assert(ex.getMessage.contains(
      "In continuous processing mode, coalesce(1) must be called before aggregate operation"))
  }

  test("basic") {
    withSQLConf((UNSUPPORTED_OPERATION_CHECK_ENABLED.key, "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        AddData(input, 3, 4, 5),
        StartStream(),
        CheckAnswer(5),
        AddData(input, -1, -2, -3),
        CheckAnswer(5))
    }
  }

  test("multiple partitions with coalesce") {
    val input = ContinuousMemoryStream[Int]

    val df = input.toDF().coalesce(1).agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("multiple partitions with coalesce - multiple transformations") {
    val input = ContinuousMemoryStream[Int]

    // We use a barrier to make sure predicates both before and after coalesce work
    val df = input.toDF()
      .select('value as 'copy, 'value)
      .where('copy =!= 1)
      .logicalPlan
      .coalesce(1)
      .where('copy =!= 2)
      .agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(0),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("multiple partitions with multiple coalesce") {
    val input = ContinuousMemoryStream[Int]

    val df = input.toDF()
      .coalesce(1)
      .logicalPlan
      .coalesce(1)
      .select('value as 'copy, 'value)
      .agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("repeated restart") {
    withSQLConf((UNSUPPORTED_OPERATION_CHECK_ENABLED.key, "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        AddData(input, 0),
        CheckAnswer(2),
        AddData(input, 5),
        CheckAnswer(5))
    }
  }
}
