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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class ParquetSchemaTest extends ParquetTest with SharedSparkSession {

  /**
   * Checks whether the reflected Parquet message type for product type `T` conforms `messageType`.
   */
  protected def testSchemaInference[T <: Product: ClassTag: TypeTag](
      testName: String,
      messageType: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean,
      writeLegacyParquetFormat: Boolean): Unit = {
    testSchema(
      testName,
      StructType.fromAttributes(ScalaReflection.attributesFor[T]),
      messageType,
      binaryAsString,
      int96AsTimestamp,
      writeLegacyParquetFormat)
  }

  protected def testParquetToCatalyst(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean): Unit = {
    val converter = new ParquetToSparkSchemaConverter(
      assumeBinaryIsString = binaryAsString,
      assumeInt96IsTimestamp = int96AsTimestamp)

    test(s"sql <= parquet: $testName") {
      val actual = converter.convert(MessageTypeParser.parseMessageType(parquetSchema))
      val expected = sqlSchema
      assert(
        actual === expected,
        s"""Schema mismatch.
           |Expected schema: ${expected.json}
           |Actual schema:   ${actual.json}
         """.stripMargin)
    }
  }

  protected def testCatalystToParquet(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      writeLegacyParquetFormat: Boolean,
      outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
        SQLConf.ParquetOutputTimestampType.INT96): Unit = {
    val converter = new SparkToParquetSchemaConverter(
      writeLegacyParquetFormat = writeLegacyParquetFormat,
      outputTimestampType = outputTimestampType)

    test(s"sql => parquet: $testName") {
      val actual = converter.convert(sqlSchema)
      val expected = MessageTypeParser.parseMessageType(parquetSchema)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }

  protected def testSchema(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean,
      writeLegacyParquetFormat: Boolean,
      outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
        SQLConf.ParquetOutputTimestampType.INT96): Unit = {

    testCatalystToParquet(
      testName,
      sqlSchema,
      parquetSchema,
      writeLegacyParquetFormat,
      outputTimestampType)

    testParquetToCatalyst(
      testName,
      sqlSchema,
      parquetSchema,
      binaryAsString,
      int96AsTimestamp)
  }
}

class ParquetSchemaInferenceSuite extends ParquetSchemaTest {

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
      .set("spark.oap.sql.columnar.testing", "true")

  testSchemaInference[(Boolean, Int, Long, Float, Double, Array[Byte])](
    "basic types",
    """
      |message root {
      |  required boolean _1;
      |  required int32   _2;
      |  required int64   _3;
      |  required float   _4;
      |  required double  _5;
      |  optional binary  _6;
      |}
    """.stripMargin,
    binaryAsString = false,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[(Byte, Short, Int, Long, java.sql.Date)](
    "logical integral types",
    """
      |message root {
      |  required int32 _1 (INT_8);
      |  required int32 _2 (INT_16);
      |  required int32 _3 (INT_32);
      |  required int64 _4 (INT_64);
      |  optional int32 _5 (DATE);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[String]](
    "string",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[String]](
    "binary enum as string",
    """
      |message root {
      |  optional binary _1 (ENUM);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[Seq[Int]]](
    "non-nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[Seq[Int]]](
    "non-nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchemaInference[Tuple1[Seq[Integer]]](
    "nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[Seq[Integer]]](
    "nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchemaInference[Tuple1[Map[Int, String]]](
    "map - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchemaInference[Tuple1[Map[Int, String]]](
    "map - non-standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[(Int, String)]](
    "struct",
    """
      |message root {
      |  optional group _1 {
      |    required int32 _1;
      |    optional binary _2 (UTF8);
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type - non-standard",
    """
      |message root {
      |  optional group _1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group bag {
      |            optional group array {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group list {
      |            optional group element {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchemaInference[(Option[Int], Map[Int, Option[Double]])](
    "optional types",
    """
      |message root {
      |  optional int32 _1;
      |  optional group _2 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional double value;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)
}

class ParquetSchemaSuite extends ParquetSchemaTest {

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
      .set("spark.oap.sql.columnar.testing", "true")

  test("DataType string parser compatibility") {
    // This is the generated string from previous versions of the Spark SQL, using the following:
    // val schema = StructType(List(
    //  StructField("c1", IntegerType, false),
    //  StructField("c2", BinaryType, true)))
    val caseClassString =
      "StructType(List(StructField(c1,IntegerType,false), StructField(c2,BinaryType,true)))"

    // scalastyle:off
    val jsonString = """{"type":"struct","fields":[{"name":"c1","type":"integer","nullable":false,"metadata":{}},{"name":"c2","type":"binary","nullable":true,"metadata":{}}]}"""
    // scalastyle:on

    val fromCaseClassString = StructType.fromString(caseClassString)
    val fromJson = StructType.fromString(jsonString)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
      assert(a.nullable === b.nullable)
    }
  }

  test("schema merging failure error message") {
    import testImplicits._

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(s"$path/p=1")
      spark.range(3).select('id cast IntegerType as 'id).write.parquet(s"$path/p=2")

      val message = intercept[SparkException] {
        spark.read.option("mergeSchema", "true").parquet(path).schema
      }.getMessage

      assert(message.contains("Failed merging schema"))
    }
  }

  // =======================================
  // Tests for parquet schema mismatch error
  // =======================================
  def testSchemaMismatch(path: String, vectorizedReaderEnabled: Boolean): SparkException = {
    import testImplicits._

    var e: SparkException = null
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorizedReaderEnabled.toString) {
      // Create two parquet files with different schemas in the same folder
      Seq(("bcd", 2)).toDF("a", "b").coalesce(1).write.mode("overwrite").parquet(s"$path/parquet")
      Seq((1, "abc")).toDF("a", "b").coalesce(1).write.mode("append").parquet(s"$path/parquet")

      e = intercept[SparkException] {
        spark.read.parquet(s"$path/parquet").collect()
      }
    }
    e
  }

  test("schema mismatch failure error message for parquet reader") {
    withTempPath { dir =>
      val e = testSchemaMismatch(dir.getCanonicalPath, vectorizedReaderEnabled = false)
      val expectedMessage = "Encounter error while reading parquet files. " +
        "One possible cause: Parquet column cannot be converted in the corresponding " +
        "files. Details:"
      assert(e.getCause.isInstanceOf[QueryExecutionException])
      assert(e.getCause.getCause.isInstanceOf[ParquetDecodingException])
      assert(e.getCause.getMessage.startsWith(expectedMessage))
    }
  }

  test("schema mismatch failure error message for parquet vectorized reader") {
    withTempPath { dir =>
      val e = testSchemaMismatch(dir.getCanonicalPath, vectorizedReaderEnabled = true)
      assert(e.getCause.isInstanceOf[QueryExecutionException])
      assert(e.getCause.getCause.isInstanceOf[SchemaColumnConvertNotSupportedException])

      // Check if the physical type is reporting correctly
      val errMsg = e.getCause.getMessage
      assert(errMsg.startsWith("Parquet column cannot be converted in file"))
      val file = errMsg.substring("Parquet column cannot be converted in file ".length,
        errMsg.indexOf(". "))
      val col = spark.read.parquet(file).schema.fields.filter(_.name == "a")
      assert(col.length == 1)
      if (col(0).dataType == StringType) {
        assert(errMsg.contains("Column: [a], Expected: int, Found: BINARY"))
      } else {
        assert(errMsg.endsWith("Column: [a], Expected: string, Found: INT32"))
      }
    }
  }

  // =======================================================
  // Tests for converting Parquet LIST to Catalyst ArrayType
  // =======================================================

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with nullable element type - 2",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      optional int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 2",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 3",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 element;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 4",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false),
            StructField("num", IntegerType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required binary str (UTF8);
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 5 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group array {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 6 - parquet-thrift style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group f1_tuple {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 7 - " +
      "parquet-protobuf primitive lists",
    new StructType()
      .add("f1", ArrayType(IntegerType, containsNull = false), nullable = false),
    """message root {
      |  repeated int32 f1;
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 8 - " +
      "parquet-protobuf non-primitive lists",
    {
      val elementType =
        new StructType()
          .add("c1", StringType, nullable = true)
          .add("c2", IntegerType, nullable = false)

      new StructType()
        .add("f1", ArrayType(elementType, containsNull = false), nullable = false)
    },
    """message root {
      |  repeated group f1 {
      |    optional binary c1 (UTF8);
      |    required int32 c2;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  // =======================================================
  // Tests for converting Catalyst ArrayType to Parquet LIST
  // =======================================================

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  // ====================================================
  // Tests for converting Parquet Map to Catalyst MapType
  // ====================================================

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 num;
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 num;
      |      optional binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 3 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true)

  // ====================================================
  // Tests for converting Catalyst MapType to Parquet Map
  // ====================================================

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  // =================================
  // Tests for conversion for decimals
  // =================================

  testSchema(
    "DECIMAL(1, 0) - standard",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional int32 f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(8, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(9, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(18, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional int64 f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(19, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(19, 3)))),
    """message root {
      |  optional fixed_len_byte_array(9) f1 (DECIMAL(19, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(1, 0) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional fixed_len_byte_array(1) f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(8, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional fixed_len_byte_array(4) f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(9, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(18, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional fixed_len_byte_array(8) f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "Timestamp written and read as INT64 with TIMESTAMP_MILLIS",
    StructType(Seq(StructField("f1", TimestampType))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MILLIS);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = false,
    writeLegacyParquetFormat = true,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS)

  testSchema(
    "Timestamp written and read as INT64 with TIMESTAMP_MICROS",
    StructType(Seq(StructField("f1", TimestampType))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MICROS);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = false,
    writeLegacyParquetFormat = true,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS)

  private def testSchemaClipping(
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: String,
      caseSensitive: Boolean = true): Unit = {
    testSchemaClipping(testName, parquetSchema, catalystSchema,
      MessageTypeParser.parseMessageType(expectedSchema), caseSensitive)
  }

  private def testSchemaClipping(
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: MessageType,
      caseSensitive: Boolean): Unit = {
    test(s"Clipping - $testName") {
      val actual = ParquetReadSupport.clipParquetSchema(
        MessageTypeParser.parseMessageType(parquetSchema), catalystSchema, caseSensitive)

      try {
        expectedSchema.checkContains(actual)
        actual.checkContains(expectedSchema)
      } catch { case cause: Throwable =>
        fail(
          s"""Expected clipped schema:
             |$expectedSchema
             |Actual clipped schema:
             |$actual
           """.stripMargin,
          cause)
      }
    }
  }

  testSchemaClipping(
    "simple nested struct",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |    optional int32 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType().add("f00", IntegerType, nullable = true)
      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", IntegerType, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |  }
        |  optional int32 f1;
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-protobuf style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional int32 f010;
        |      optional double f011;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f00Type = ArrayType(StringType, containsNull = false)
      val f01Type = ArrayType(
        new StructType()
          .add("f011", DoubleType, nullable = true),
        containsNull = false)

      val f0Type = new StructType()
        .add("f00", f00Type, nullable = false)
        .add("f01", f01Type, nullable = false)
      val f1Type = ArrayType(IntegerType, containsNull = true)

      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", f1Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional double f011;
        |    }
        |  }
        |
        |  optional group f1 (LIST) {
        |    repeated group list {
        |      optional int32 element;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-thrift style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-avro style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-hive style array",

    parquetSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = true), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = true), nullable = true)

      new StructType().add("f0", f0Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "2-level list of required struct",

    parquetSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        required int32 f000;
         |        optional int64 f001;
         |      }
         |    }
         |  }
         |}
       """.stripMargin,

    catalystSchema = {
      val f00ElementType =
        new StructType()
          .add("f001", LongType, nullable = true)
          .add("f002", DoubleType, nullable = false)

      val f00Type = ArrayType(f00ElementType, containsNull = false)
      val f0Type = new StructType().add("f00", f00Type, nullable = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        optional int64 f001;
         |        required double f002;
         |      }
         |    }
         |  }
         |}
       """.stripMargin)

  testSchemaClipping(
    "standard array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "empty requested schema",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = new StructType(),

    expectedSchema = ParquetSchemaConverter.EMPTY_MESSAGE,
    caseSensitive = true)

  testSchemaClipping(
    "disjoint field sets",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema =
      new StructType()
        .add(
          "f0",
          new StructType()
            .add("f02", FloatType, nullable = true)
            .add("f03", DoubleType, nullable = true),
          nullable = true),

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional float f02;
        |    optional double f03;
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-avro style map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map with complex key",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val keyType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(keyType, IntegerType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "case-insensitive resolution: no ambiguity",
    parquetSchema =
      """message root {
        |  required group A {
        |    optional int32 B;
        |  }
        |  optional int32 c;
        |}
      """.stripMargin,
    catalystSchema = {
      val nestedType = new StructType().add("b", IntegerType, nullable = true)
      new StructType()
        .add("a", nestedType, nullable = true)
        .add("c", IntegerType, nullable = true)
    },
    expectedSchema =
      """message root {
        |  required group A {
        |    optional int32 B;
        |  }
        |  optional int32 c;
        |}
      """.stripMargin,
    caseSensitive = false)

    test("Clipping - case-insensitive resolution: more than one field is matched") {
      val parquetSchema =
        """message root {
          |  required group A {
          |    optional int32 B;
          |  }
          |  optional int32 c;
          |  optional int32 a;
          |}
        """.stripMargin
      val catalystSchema = {
        val nestedType = new StructType().add("b", IntegerType, nullable = true)
        new StructType()
          .add("a", nestedType, nullable = true)
          .add("c", IntegerType, nullable = true)
      }
      assertThrows[RuntimeException] {
        ParquetReadSupport.clipParquetSchema(
          MessageTypeParser.parseMessageType(parquetSchema), catalystSchema, caseSensitive = false)
      }
    }
}
