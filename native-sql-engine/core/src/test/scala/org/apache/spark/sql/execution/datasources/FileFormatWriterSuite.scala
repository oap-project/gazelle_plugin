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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.test.SharedSparkSession

class FileFormatWriterSuite
  extends QueryTest
  with SharedSparkSession
  with CodegenInterpretedPlanTest{

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
      .set("spark.sql.parquet.enableVectorizedReader", "false")
      .set("spark.sql.orc.enableVectorizedReader", "false")
      .set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")
      .set("spark.oap.sql.columnar.testing", "true")

  test("empty file should be skipped while write to file") {
    withTempPath { path =>
      spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 2)
    }
  }

  test("SPARK-22252: FileFormatWriter should respect the input query schema") {
    withTable("t1", "t2", "t3", "t4") {
      spark.range(1).select('id as 'col1, 'id as 'col2).write.saveAsTable("t1")
      spark.sql("select COL1, COL2 from t1").write.saveAsTable("t2")
      checkAnswer(spark.table("t2"), Row(0, 0))

      // Test picking part of the columns when writing.
      spark.range(1).select('id, 'id as 'col1, 'id as 'col2).write.saveAsTable("t3")
      spark.sql("select COL1, COL2 from t3").write.saveAsTable("t4")
      checkAnswer(spark.table("t4"), Row(0, 0))
    }
  }

  test("Null and '' values should not cause dynamic partition failure of string types") {
    withTable("t1", "t2") {
      Seq((0, None), (1, Some("")), (2, None)).toDF("id", "p")
        .write.partitionBy("p").saveAsTable("t1")
      checkAnswer(spark.table("t1").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))

      sql("create table t2(id long, p string) using parquet partitioned by (p)")
      sql("insert overwrite table t2 partition(p) select id, p from t1")
      checkAnswer(spark.table("t2").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))
    }
  }
}
