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

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to test Kryo custom registrators.
 */
class DatasetSerializerRegistratorSuite extends QueryTest with SharedSparkSession {
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
      .set(KRYO_USER_REGISTRATORS, TestRegistrator().getClass.getCanonicalName)

//  override protected def sparkConf: SparkConf = {
//    // Make sure we use the KryoRegistrator
//    super.sparkConf.set(KRYO_USER_REGISTRATORS, TestRegistrator().getClass.getCanonicalName)
//  }

  test("Kryo registrator") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()
    assert(ds.collect().toSet == Set(KryoData(0), KryoData(0)))
  }

}

/** Used to test user provided registrator. */
class TestRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit =
    kryo.register(classOf[KryoData], new ZeroKryoDataSerializer())
}

object TestRegistrator {
  def apply(): TestRegistrator = new TestRegistrator()
}

/**
 * A `Serializer` that takes a [[KryoData]] and serializes it as KryoData(0).
 */
class ZeroKryoDataSerializer extends Serializer[KryoData] {
  override def write(kryo: Kryo, output: Output, t: KryoData): Unit = {
    output.writeInt(0)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[KryoData]): KryoData = {
    KryoData(input.readInt())
  }
}
