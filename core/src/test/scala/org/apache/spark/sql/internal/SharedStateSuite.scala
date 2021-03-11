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

package org.apache.spark.sql.internal

import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession


/**
 * Tests for [[org.apache.spark.sql.internal.SharedState]].
 */
class SharedStateSuite extends SharedSparkSession {

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
      .set("spark.hadoop.fs.defaultFS", "file:///")

  test("SPARK-31692: Url handler factory should have the hadoop configs from Spark conf") {
    // Accessing shared state to init the object since it is `lazy val`
    spark.sharedState
    val field = classOf[URL].getDeclaredField("factory")
    field.setAccessible(true)
    val value = field.get(null)
    assert(value.isInstanceOf[FsUrlStreamHandlerFactory])
    val streamFactory = value.asInstanceOf[FsUrlStreamHandlerFactory]

    val confField = classOf[FsUrlStreamHandlerFactory].getDeclaredField("conf")
    confField.setAccessible(true)
    val conf = confField.get(streamFactory)

    assert(conf.isInstanceOf[Configuration])
    assert(conf.asInstanceOf[Configuration].get("fs.defaultFS") == "file:///")
  }
}
