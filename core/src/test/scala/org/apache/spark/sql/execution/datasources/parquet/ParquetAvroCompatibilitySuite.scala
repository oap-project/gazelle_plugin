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

package org.apache.spark.sql.execution.datasources.parquet

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.test.avro._
import org.apache.spark.sql.test.SharedSparkSession

class ParquetAvroCompatibilitySuite extends ParquetCompatibilityTest with SharedSparkSession {

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

  private def withWriter[T <: IndexedRecord]
      (path: String, schema: Schema)
      (f: ParquetWriter[T] => Unit): Unit = {
    logInfo(
      s"""Writing Avro records with the following Avro schema into Parquet file:
         |
         |${schema.toString(true)}
       """.stripMargin)

    val writer = AvroParquetWriter.builder[T](new Path(path)).withSchema(schema).build()
    try f(writer) finally writer.close()
  }

  ignore("required primitives") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroPrimitives](path, AvroPrimitives.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(
            AvroPrimitives.newBuilder()
              .setBoolColumn(i % 2 == 0)
              .setIntColumn(i)
              .setLongColumn(i.toLong * 10)
              .setFloatColumn(i.toFloat + 0.1f)
              .setDoubleColumn(i.toDouble + 0.2d)
              .setBinaryColumn(ByteBuffer.wrap(s"val_$i".getBytes(StandardCharsets.UTF_8)))
              .setStringColumn(s"val_$i")
              .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        Row(
          i % 2 == 0,
          i,
          i.toLong * 10,
          i.toFloat + 0.1f,
          i.toDouble + 0.2d,
          s"val_$i".getBytes(StandardCharsets.UTF_8),
          s"val_$i")
      })
    }
  }

  ignore("optional primitives") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroOptionalPrimitives](path, AvroOptionalPrimitives.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          val record = if (i % 3 == 0) {
            AvroOptionalPrimitives.newBuilder()
              .setMaybeBoolColumn(null)
              .setMaybeIntColumn(null)
              .setMaybeLongColumn(null)
              .setMaybeFloatColumn(null)
              .setMaybeDoubleColumn(null)
              .setMaybeBinaryColumn(null)
              .setMaybeStringColumn(null)
              .build()
          } else {
            AvroOptionalPrimitives.newBuilder()
              .setMaybeBoolColumn(i % 2 == 0)
              .setMaybeIntColumn(i)
              .setMaybeLongColumn(i.toLong * 10)
              .setMaybeFloatColumn(i.toFloat + 0.1f)
              .setMaybeDoubleColumn(i.toDouble + 0.2d)
              .setMaybeBinaryColumn(ByteBuffer.wrap(s"val_$i".getBytes(StandardCharsets.UTF_8)))
              .setMaybeStringColumn(s"val_$i")
              .build()
          }

          writer.write(record)
        }
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        if (i % 3 == 0) {
          Row.apply(Seq.fill(7)(null): _*)
        } else {
          Row(
            i % 2 == 0,
            i,
            i.toLong * 10,
            i.toFloat + 0.1f,
            i.toDouble + 0.2d,
            s"val_$i".getBytes(StandardCharsets.UTF_8),
            s"val_$i")
        }
      })
    }
  }

  ignore("non-nullable arrays") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroNonNullableArrays](path, AvroNonNullableArrays.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          val record = {
            val builder =
              AvroNonNullableArrays.newBuilder()
                .setStringsColumn(Seq.tabulate(3)(i => s"val_$i").asJava)

            if (i % 3 == 0) {
              builder.setMaybeIntsColumn(null).build()
            } else {
              builder.setMaybeIntsColumn(Seq.tabulate(3)(Int.box).asJava).build()
            }
          }

          writer.write(record)
        }
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        Row(
          Seq.tabulate(3)(i => s"val_$i"),
          if (i % 3 == 0) null else Seq.tabulate(3)(identity))
      })
    }
  }

  test("nullable arrays (parquet-avro 1.7.0 does not properly support this)") {
    // TODO Complete this test case after upgrading to parquet-mr 1.8+
  }

  ignore("SPARK-10136 array of primitive array") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroArrayOfArray](path, AvroArrayOfArray.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(AvroArrayOfArray.newBuilder()
            .setIntArraysColumn(
              Seq.tabulate(3, 3)((i, j) => i * 3 + j: Integer).map(_.asJava).asJava)
            .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        Row(Seq.tabulate(3, 3)((i, j) => i * 3 + j))
      })
    }
  }

  ignore("map of primitive array") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroMapOfArray](path, AvroMapOfArray.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(AvroMapOfArray.newBuilder()
            .setStringToIntsColumn(
              Seq.tabulate(3) { i =>
                i.toString -> Seq.tabulate(3)(j => i + j: Integer).asJava
              }.toMap.asJava)
            .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        Row(Seq.tabulate(3)(i => i.toString -> Seq.tabulate(3)(j => i + j)).toMap)
      })
    }
  }

  ignore("various complex types") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[ParquetAvroCompat](path, ParquetAvroCompat.getClassSchema) { writer =>
        (0 until 10).foreach(i => writer.write(makeParquetAvroCompat(i)))
      }

      logParquetSchema(path)

      checkAnswer(spark.read.parquet(path), (0 until 10).map { i =>
        Row(
          Seq.tabulate(3)(n => s"arr_${i + n}"),
          Seq.tabulate(3)(n => n.toString -> (i + n: Integer)).toMap,
          Seq.tabulate(3) { n =>
            (i + n).toString -> Seq.tabulate(3) { m =>
              Row(Seq.tabulate(3)(j => i + j + m), s"val_${i + m}")
            }
          }.toMap)
      })
    }
  }

  def makeParquetAvroCompat(i: Int): ParquetAvroCompat = {
    def makeComplexColumn(i: Int): JMap[String, JList[Nested]] = {
      Seq.tabulate(3) { n =>
        (i + n).toString -> Seq.tabulate(3) { m =>
          Nested
            .newBuilder()
            .setNestedIntsColumn(Seq.tabulate(3)(j => i + j + m: Integer).asJava)
            .setNestedStringColumn(s"val_${i + m}")
            .build()
        }.asJava
      }.toMap.asJava
    }

    ParquetAvroCompat
      .newBuilder()
      .setStringsColumn(Seq.tabulate(3)(n => s"arr_${i + n}").asJava)
      .setStringToIntColumn(Seq.tabulate(3)(n => n.toString -> (i + n: Integer)).toMap.asJava)
      .setComplexColumn(makeComplexColumn(i))
      .build()
  }

  ignore("SPARK-9407 Push down predicates involving Parquet ENUM columns") {
    import testImplicits._

    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[ParquetEnum](path, ParquetEnum.getClassSchema) { writer =>
        (0 until 4).foreach { i =>
          writer.write(ParquetEnum.newBuilder().setSuit(Suit.values.apply(i)).build())
        }
      }

      checkAnswer(spark.read.parquet(path).filter('suit === "SPADES"), Row("SPADES"))
    }
  }
}
