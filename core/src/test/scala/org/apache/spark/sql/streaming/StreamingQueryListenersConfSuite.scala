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

package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.StaticSQLConf.STREAMING_QUERY_LISTENERS
import org.apache.spark.sql.streaming.StreamingQueryListener._


class StreamingQueryListenersConfSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  override protected def sparkConf: SparkConf =
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
      .set(STREAMING_QUERY_LISTENERS.key,
        "org.apache.spark.sql.streaming.TestListener")

  test("test if the configured query listener is loaded") {
    testStream(MemoryStream[Int].toDS)(
      StartStream(),
      StopStream
    )

    spark.sparkContext.listenerBus.waitUntilEmpty()

    assert(TestListener.queryStartedEvent != null)
    assert(TestListener.queryTerminatedEvent != null)
  }

}

object TestListener {
  @volatile var queryStartedEvent: QueryStartedEvent = null
  @volatile var queryTerminatedEvent: QueryTerminatedEvent = null
}

class TestListener(sparkConf: SparkConf) extends StreamingQueryListener {

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    TestListener.queryStartedEvent = event
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {}

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    TestListener.queryTerminatedEvent = event
  }
}
