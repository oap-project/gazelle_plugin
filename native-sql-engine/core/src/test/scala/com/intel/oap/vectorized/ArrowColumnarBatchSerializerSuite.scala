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
package com.intel.oap.vectorized

import java.io.FileInputStream

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkFunSuite

class ArrowColumnarBatchSerializerSuite extends SparkFunSuite with SharedSparkSession {

  protected var avgBatchNumRows: SQLMetric = _
  protected var outputNumRows: SQLMetric = _

  override def beforeEach() = {
    avgBatchNumRows = SQLMetrics.createAverageMetric(
      spark.sparkContext,
      "test serializer avg read batch num rows")
    outputNumRows =
      SQLMetrics.createAverageMetric(spark.sparkContext, "test serializer number of output rows")
  }

  test("deserialize all null") {
    val input = getTestResourcePath("test-data/native-splitter-output-all-null")
    val serializer =
      new ArrowColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 4)
        assert(batch.numCols == 3)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[ArrowWritableColumnVector]
              .getValueVector
          assert(valueVector.getValueCount == batch.numRows)
          assert(valueVector.getNullCount === batch.numRows)
        }
    }
    assert(length == 2)
    deserializedStream.close()
  }

  test("deserialize nullable string") {
    val input = getTestResourcePath("test-data/native-splitter-output-nullable-string")
    val serializer =
      new ArrowColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 8)
        assert(batch.numCols == 3)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[ArrowWritableColumnVector]
              .getValueVector
          assert(valueVector.getValueCount == batch.numRows)
        }
    }
    assert(length == 2)
    deserializedStream.close()
  }
}
