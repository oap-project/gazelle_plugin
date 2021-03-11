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

package org.apache.spark.sql.sources

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._


// please note that the META-INF/services had to be modified for the test directory for this to work
class DDLSourceLoadSuite extends DataSourceTest with SharedSparkSession {

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

  test("data sources with the same name - internal data sources") {
    val e = intercept[AnalysisException] {
      spark.read.format("Fluet da Bomb").load()
    }
    assert(e.getMessage.contains("Multiple sources found for Fluet da Bomb"))
  }

  test("data sources with the same name - internal data source/external data source") {
    assert(spark.read.format("datasource").load().schema ==
      StructType(Seq(StructField("longType", LongType, nullable = false))))
  }

  test("data sources with the same name - external data sources") {
    val e = intercept[AnalysisException] {
      spark.read.format("Fake external source").load()
    }
    assert(e.getMessage.contains("Multiple sources found for Fake external source"))
  }

  test("load data source from format alias") {
    assert(spark.read.format("gathering quorum").load().schema ==
      StructType(Seq(StructField("stringType", StringType, nullable = false))))
  }

  test("specify full classname with duplicate formats") {
    assert(spark.read.format("org.apache.spark.sql.sources.FakeSourceOne")
      .load().schema == StructType(Seq(StructField("stringType", StringType, nullable = false))))
  }
}


class FakeSourceOne extends RelationProvider with DataSourceRegister {

  def shortName(): String = "Fluet da Bomb"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("stringType", StringType, nullable = false)))
    }
}

class FakeSourceTwo extends RelationProvider with DataSourceRegister {

  def shortName(): String = "Fluet da Bomb"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("integerType", IntegerType, nullable = false)))
    }
}

class FakeSourceThree extends RelationProvider with DataSourceRegister {

  def shortName(): String = "gathering quorum"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("stringType", StringType, nullable = false)))
    }
}

class FakeSourceFour extends RelationProvider with DataSourceRegister {

  def shortName(): String = "datasource"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("longType", LongType, nullable = false)))
    }
}
