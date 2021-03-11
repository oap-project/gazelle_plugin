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

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, StandardOpenOption}
import java.util.Locale

import com.intel.oap.execution.ColumnarBroadcastHashJoinExec

import scala.collection.mutable
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.TestingUDT.{IntervalUDT, NullData, NullUDT}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, FileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._


class FileBasedDataSourceSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
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
      .set("spark.oap.sql.columnar.testing", "true")
      .set("spark.sql.parquet.enableVectorizedReader", "false")
      .set("spark.sql.orc.enableVectorizedReader", "false")
      .set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.ORC_IMPLEMENTATION, "native")
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.conf.unsetConf(SQLConf.ORC_IMPLEMENTATION)
    } finally {
      super.afterAll()
    }
  }

  private val allFileBasedDataSources = Seq("orc", "parquet", "csv", "json", "text")
  private val nameWithSpecialChars = "sp&cial%c hars"

  allFileBasedDataSources.foreach { format =>
    test(s"Writing empty datasets should not fail - $format") {
      withTempPath { dir =>
        Seq("str").toDS().limit(0).write.format(format).save(dir.getCanonicalPath)
      }
    }
  }

  // `TEXT` data source always has a single column whose name is `value`.
  allFileBasedDataSources.filterNot(_ == "text").foreach { format =>
    test(s"SPARK-23072 Write and read back unicode column names - $format") {
      withTempPath { path =>
        val dir = path.getCanonicalPath

        // scalastyle:off nonascii
        val df = Seq("a").toDF("한글")
        // scalastyle:on nonascii

        df.write.format(format).option("header", "true").save(dir)
        val answerDf = spark.read.format(format).option("header", "true").load(dir)

        assert(df.schema.sameType(answerDf.schema))
        checkAnswer(df, answerDf)
      }
    }
  }

  // Only ORC/Parquet support this. `CSV` and `JSON` returns an empty schema.
  // `TEXT` data source always has a single column whose name is `value`.
  Seq("orc", "parquet").foreach { format =>
    // ignored in maven test
    test(s"SPARK-15474 Write and read back non-empty schema with empty dataframe - $format") {
      withTempPath { file =>
        val path = file.getCanonicalPath
        val emptyDf = Seq((true, 1, "str")).toDF().limit(0)
        emptyDf.write.format(format).save(path)

        val df = spark.read.format(format).load(path)
        assert(df.schema.sameType(emptyDf.schema))
        checkAnswer(df, emptyDf)
      }
    }
  }

  Seq("orc", "parquet").foreach { format =>
    // ignored in maven test
    test(s"SPARK-23271 empty RDD when saved should write a metadata only file - $format") {
      withTempPath { outputPath =>
        val df = spark.emptyDataFrame.select(lit(1).as("i"))
        df.write.format(format).save(outputPath.toString)
        val partFiles = outputPath.listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 1)

        // Now read the file.
        val df1 = spark.read.format(format).load(outputPath.toString)
        checkAnswer(df1, Seq.empty[Row])
        assert(df1.schema.equals(df.schema.asNullable))
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    test(s"SPARK-23372 error while writing empty schema files using $format") {
      withTempPath { outputPath =>
        val errMsg = intercept[AnalysisException] {
          spark.emptyDataFrame.write.format(format).save(outputPath.toString)
        }
        assert(errMsg.getMessage.contains(
          "Datasource does not support writing empty or nested empty schemas"))
      }

      // Nested empty schema
      withTempPath { outputPath =>
        val schema = StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StructType(Nil)),
          StructField("c", IntegerType)
        ))
        val df = spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
        val errMsg = intercept[AnalysisException] {
          df.write.format(format).save(outputPath.toString)
        }
        assert(errMsg.getMessage.contains(
          "Datasource does not support writing empty or nested empty schemas"))
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    test(s"SPARK-22146 read files containing special characters using $format") {
      withTempDir { dir =>
        val tmpFile = s"$dir/$nameWithSpecialChars"
        spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
        val fileContent = spark.read.format(format).load(tmpFile)
        checkAnswer(fileContent, Seq(Row("a"), Row("b")))
      }
    }
  }

  // Separate test case for formats that support multiLine as an option.
  Seq("json", "csv").foreach { format =>
    test("SPARK-23148 read files containing special characters " +
      s"using $format with multiline enabled") {
      withTempDir { dir =>
        val tmpFile = s"$dir/$nameWithSpecialChars"
        spark.createDataset(Seq("a", "b")).write.format(format).save(tmpFile)
        val reader = spark.read.format(format).option("multiLine", true)
        val fileContent = reader.load(tmpFile)
        checkAnswer(fileContent, Seq(Row("a"), Row("b")))
      }
    }
  }

  allFileBasedDataSources.foreach { format =>
    test(s"Enabling/disabling ignoreMissingFiles using $format") {
      def testIgnoreMissingFiles(): Unit = {
        withTempDir { dir =>
          val basePath = dir.getCanonicalPath

          Seq("0").toDF("a").write.format(format).save(new Path(basePath, "second").toString)
          Seq("1").toDF("a").write.format(format).save(new Path(basePath, "fourth").toString)

          val firstPath = new Path(basePath, "first")
          val thirdPath = new Path(basePath, "third")
          val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())
          Seq("2").toDF("a").write.format(format).save(firstPath.toString)
          Seq("3").toDF("a").write.format(format).save(thirdPath.toString)
          val files = Seq(firstPath, thirdPath).flatMap { p =>
            fs.listStatus(p).filter(_.isFile).map(_.getPath)
          }

          val df = spark.read.format(format).load(
            new Path(basePath, "first").toString,
            new Path(basePath, "second").toString,
            new Path(basePath, "third").toString,
            new Path(basePath, "fourth").toString)

          // Make sure all data files are deleted and can't be opened.
          files.foreach(f => fs.delete(f, false))
          assert(fs.delete(thirdPath, true))
          for (f <- files) {
            intercept[FileNotFoundException](fs.open(f))
          }

          checkAnswer(df, Seq(Row("0"), Row("1")))
        }
      }

      for {
        ignore <- Seq("true", "false")
        sources <- Seq("", format)
      } {
        withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> ignore,
          SQLConf.USE_V1_SOURCE_LIST.key -> sources) {
            if (ignore.toBoolean) {
              testIgnoreMissingFiles()
            } else {
              val exception = intercept[SparkException] {
                testIgnoreMissingFiles()
              }
              assert(exception.getMessage().contains("does not exist"))
            }
        }
      }
    }
  }

  // Text file format only supports string type
  test("SPARK-24691 error handling for unsupported types - text") {
    withTempDir { dir =>
      // write path
      val textDir = new File(dir, "text").getCanonicalPath
      var msg = intercept[AnalysisException] {
        Seq(1).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        Seq(1.2).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        Seq(true).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))

      msg = intercept[AnalysisException] {
        Seq(1).toDF("a").selectExpr("struct(a)").write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support struct<a:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Map("Tesla" -> 3))).toDF("cars").write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Array("Tesla", "Chevy", "Ford"))).toDF("brands")
          .write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support array<string> data type"))

      // read path
      Seq("aaa").toDF.write.mode("overwrite").text(textDir)
      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", IntegerType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", DoubleType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", BooleanType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))
    }
  }

  // Unsupported data types of csv, json, orc, and parquet are as follows;
  //  csv -> R/W: Null, Array, Map, Struct
  //  json -> R/W: Interval
  //  orc -> R/W: Interval, W: Null
  //  parquet -> R/W: Interval, Null
  test("SPARK-24204 error handling for unsupported Array/Map/Struct types - csv") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      var msg = intercept[AnalysisException] {
        Seq((1, "Tesla")).toDF("a", "b").selectExpr("struct(a, b)").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<a:int,b:string> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a struct<b: Int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<b:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Map("Tesla" -> 3))).toDF("id", "cars").write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a map<int, int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support map<int,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Array("Tesla", "Chevy", "Ford"))).toDF("id", "brands")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<string> data type"))

      msg = intercept[AnalysisException] {
         val schema = StructType.fromDDL("a array<int>")
         spark.range(1).write.mode("overwrite").csv(csvDir)
         spark.read.schema(schema).csv(csvDir).collect()
       }.getMessage
      assert(msg.contains("CSV data source does not support array<int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, new TestUDT.MyDenseVector(Array(0.25, 2.25, 4.25)))).toDF("id", "vectors")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", new TestUDT.MyDenseVectorUDT(), true) :: Nil)
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type."))
    }
  }

  test("SPARK-24204 error handling for unsupported Interval data types - csv, json, parquet, orc") {
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      // TODO: test file source V2 after write path is fixed.
      Seq(true).foreach { useV1 =>
        val useV1List = if (useV1) {
          "csv,json,orc,parquet"
        } else {
          ""
        }
        def validateErrorMessage(msg: String): Unit = {
          val msg1 = "cannot save interval data type into external storage."
          val msg2 = "data source does not support interval data type."
          assert(msg.toLowerCase(Locale.ROOT).contains(msg1) ||
            msg.toLowerCase(Locale.ROOT).contains(msg2))
        }

        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
          // write path
          Seq("csv", "json", "parquet", "orc").foreach { format =>
            val msg = intercept[AnalysisException] {
              sql("select interval 1 days").write.format(format).mode("overwrite").save(tempDir)
            }.getMessage
            validateErrorMessage(msg)
          }

          // read path
          Seq("parquet", "csv").foreach { format =>
            var msg = intercept[AnalysisException] {
              val schema = StructType(StructField("a", CalendarIntervalType, true) :: Nil)
              spark.range(1).write.format(format).mode("overwrite").save(tempDir)
              spark.read.schema(schema).format(format).load(tempDir).collect()
            }.getMessage
            validateErrorMessage(msg)

            msg = intercept[AnalysisException] {
              val schema = StructType(StructField("a", new IntervalUDT(), true) :: Nil)
              spark.range(1).write.format(format).mode("overwrite").save(tempDir)
              spark.read.schema(schema).format(format).load(tempDir).collect()
            }.getMessage
            validateErrorMessage(msg)
          }
        }
      }
    }
  }

  test("SPARK-24204 error handling for unsupported Null data types - csv, parquet, orc") {
    // TODO: test file source V2 after write path is fixed.
    Seq(true).foreach { useV1 =>
      val useV1List = if (useV1) {
        "csv,orc,parquet"
      } else {
        ""
      }
      def errorMessage(format: String): String = {
        s"$format data source does not support null data type."
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1List) {
        withTempDir { dir =>
          val tempDir = new File(dir, "files").getCanonicalPath

          Seq("parquet", "csv", "orc").foreach { format =>
            // write path
            var msg = intercept[AnalysisException] {
              sql("select null").write.format(format).mode("overwrite").save(tempDir)
            }.getMessage
            assert(msg.toLowerCase(Locale.ROOT)
              .contains(errorMessage(format)))

            msg = intercept[AnalysisException] {
              spark.udf.register("testType", () => new NullData())
              sql("select testType()").write.format(format).mode("overwrite").save(tempDir)
            }.getMessage
            assert(msg.toLowerCase(Locale.ROOT)
              .contains(errorMessage(format)))

            // read path
            msg = intercept[AnalysisException] {
              val schema = StructType(StructField("a", NullType, true) :: Nil)
              spark.range(1).write.format(format).mode("overwrite").save(tempDir)
              spark.read.schema(schema).format(format).load(tempDir).collect()
            }.getMessage
            assert(msg.toLowerCase(Locale.ROOT)
              .contains(errorMessage(format)))

            msg = intercept[AnalysisException] {
              val schema = StructType(StructField("a", new NullUDT(), true) :: Nil)
              spark.range(1).write.format(format).mode("overwrite").save(tempDir)
              spark.read.schema(schema).format(format).load(tempDir).collect()
            }.getMessage
            assert(msg.toLowerCase(Locale.ROOT)
              .contains(errorMessage(format)))
          }
        }
      }
    }
  }

  Seq("parquet", "orc").foreach { format =>
    test(s"Spark native readers should respect spark.sql.caseSensitive - ${format}") {
      withTempDir { dir =>
        val tableName = s"spark_25132_${format}_native"
        val tableDir = dir.getCanonicalPath + s"/$tableName"
        withTable(tableName) {
          val end = 5
          val data = spark.range(end).selectExpr("id as A", "id * 2 as b", "id * 3 as B")
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            data.write.format(format).mode("overwrite").save(tableDir)
          }
          sql(s"CREATE TABLE $tableName (a LONG, b LONG) USING $format LOCATION '$tableDir'")

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
            checkAnswer(sql(s"select a from $tableName"), data.select("A"))
            checkAnswer(sql(s"select A from $tableName"), data.select("A"))

            // RuntimeException is triggered at executor side, which is then wrapped as
            // SparkException at driver side
            val e1 = intercept[SparkException] {
              sql(s"select b from $tableName").collect()
            }
            assert(
              e1.getCause.isInstanceOf[RuntimeException] &&
                e1.getCause.getMessage.contains(
                  """Found duplicate field(s) "b": [b, B] in case-insensitive mode"""))
            val e2 = intercept[SparkException] {
              sql(s"select B from $tableName").collect()
            }
            assert(
              e2.getCause.isInstanceOf[RuntimeException] &&
                e2.getCause.getMessage.contains(
                  """Found duplicate field(s) "b": [b, B] in case-insensitive mode"""))
          }

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            checkAnswer(sql(s"select a from $tableName"), (0 until end).map(_ => Row(null)))
            checkAnswer(sql(s"select b from $tableName"), data.select("b"))
          }
        }
      }
    }
  }

  test("SPARK-25237 compute correct input metrics in FileScanRDD") {
    // TODO: Test CSV V2 as well after it implements [[SupportsReportStatistics]].
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "csv") {
      withTempPath { p =>
        val path = p.getAbsolutePath
        spark.range(1000).repartition(1).write.csv(path)
        val bytesReads = new mutable.ArrayBuffer[Long]()
        val bytesReadListener = new SparkListener() {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
          }
        }
        sparkContext.addSparkListener(bytesReadListener)
        try {
          spark.read.csv(path).limit(1).collect()
          sparkContext.listenerBus.waitUntilEmpty()
          assert(bytesReads.sum === 7860)
        } finally {
          sparkContext.removeSparkListener(bytesReadListener)
        }
      }
    }
  }

  test("Do not use cache on overwrite") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          val path = dir.toString
          spark.range(1000).write.mode("overwrite").orc(path)
          val df = spark.read.orc(path).cache()
          assert(df.count() == 1000)
          spark.range(10).write.mode("overwrite").orc(path)
          assert(df.count() == 10)
          assert(spark.read.orc(path).count() == 10)
        }
      }
    }
  }

  test("Do not use cache on append") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          val path = dir.toString
          spark.range(1000).write.mode("append").orc(path)
          val df = spark.read.orc(path).cache()
          assert(df.count() == 1000)
          spark.range(10).write.mode("append").orc(path)
          assert(df.count() == 1010)
          assert(spark.read.orc(path).count() == 1010)
        }
      }
    }
  }

  ignore("UDF input_file_name()") {
    Seq("", "orc").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.range(10).write.orc(path)
          val row = spark.read.orc(path).select(input_file_name).first()
          assert(row.getString(0).contains(path))
        }
      }
    }
  }

  test("Option pathGlobFilter: filter files correctly") {
    withTempPath { path =>
      val dataDir = path.getCanonicalPath
      Seq("foo").toDS().write.text(dataDir)
      Seq("bar").toDS().write.mode("append").orc(dataDir)
      val df = spark.read.option("pathGlobFilter", "*.txt").text(dataDir)
      checkAnswer(df, Row("foo"))

      // Both glob pattern in option and path should be effective to filter files.
      val df2 = spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*.orc")
      checkAnswer(df2, Seq.empty)

      val df3 = spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*xt")
      checkAnswer(df3, Row("foo"))
    }
  }

  test("Option pathGlobFilter: simple extension filtering should contains partition info") {
    withTempPath { path =>
      val input = Seq(("foo", 1), ("oof", 2)).toDF("a", "b")
      input.write.partitionBy("b").text(path.getCanonicalPath)
      Seq("bar").toDS().write.mode("append").orc(path.getCanonicalPath + "/b=1")

      // If we use glob pattern in the path, the partition column won't be shown in the result.
      val df = spark.read.text(path.getCanonicalPath + "/*/*.txt")
      checkAnswer(df, input.select("a"))

      val df2 = spark.read.option("pathGlobFilter", "*.txt").text(path.getCanonicalPath)
      checkAnswer(df2, input)
    }
  }

  test("Option recursiveFileLookup: recursive loading correctly") {

    val expectedFileList = mutable.ListBuffer[String]()

    def createFile(dir: File, fileName: String, format: String): Unit = {
      val path = new File(dir, s"${fileName}.${format}")
      Files.write(
        path.toPath,
        s"content of ${path.toString}".getBytes,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE
      )
      val fsPath = new Path(path.getAbsoluteFile.toURI).toString
      expectedFileList.append(fsPath)
    }

    def createDir(path: File, dirName: String, level: Int): Unit = {
      val dir = new File(path, s"dir${dirName}-${level}")
      dir.mkdir()
      createFile(dir, s"file${level}", "bin")
      createFile(dir, s"file${level}", "text")

      if (level < 4) {
        // create sub-dir
        createDir(dir, "sub0", level + 1)
        createDir(dir, "sub1", level + 1)
      }
    }

    withTempPath { path =>
      path.mkdir()
      createDir(path, "root", 0)

      val dataPath = new File(path, "dirroot-0").getAbsolutePath
      val fileList = spark.read.format("binaryFile")
        .option("recursiveFileLookup", true)
        .load(dataPath)
        .select("path").collect().map(_.getString(0))

      assert(fileList.toSet === expectedFileList.toSet)

      val fileList2 = spark.read.format("binaryFile")
        .option("recursiveFileLookup", true)
        .option("pathGlobFilter", "*.bin")
        .load(dataPath)
        .select("path").collect().map(_.getString(0))

      assert(fileList2.toSet === expectedFileList.filter(_.endsWith(".bin")).toSet)
    }
  }

  test("Option recursiveFileLookup: disable partition inferring") {
    val dataPath = Thread.currentThread().getContextClassLoader
      .getResource("test-data/text-partitioned").toString

    val df = spark.read.format("binaryFile")
      .option("recursiveFileLookup", true)
      .load(dataPath)

    assert(!df.columns.contains("year"), "Expect partition inferring disabled")
    val fileList = df.select("path").collect().map(_.getString(0))

    val expectedFileList = Array(
      dataPath + "/year=2014/data.txt",
      dataPath + "/year=2015/data.txt"
    ).map(path => new Path(path).toString)

    assert(fileList.toSet === expectedFileList.toSet)
  }

  test("Return correct results when data columns overlap with partition columns") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val tablePath = new File(s"${path.getCanonicalPath}/cOl3=c/cOl1=a/cOl5=e")
        Seq((1, 2, 3, 4, 5)).toDF("cOl1", "cOl2", "cOl3", "cOl4", "cOl5")
          .write.format(format).save(tablePath.getCanonicalPath)

        val df = spark.read.format(format).load(path.getCanonicalPath)
          .select("CoL1", "Col2", "CoL5", "CoL3")
        checkAnswer(df, Row("a", 2, "e", "c"))
      }
    }
  }

  test("Return correct results when data columns overlap with partition columns (nested data)") {
    Seq("parquet", "orc", "json").foreach { format =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        withTempPath { path =>
          val tablePath = new File(s"${path.getCanonicalPath}/c3=c/c1=a/c5=e")

          val inputDF = sql("SELECT 1 c1, 2 c2, 3 c3, named_struct('c4_1', 2, 'c4_2', 3) c4, 5 c5")
          inputDF.write.format(format).save(tablePath.getCanonicalPath)

          val resultDF = spark.read.format(format).load(path.getCanonicalPath)
            .select("c1", "c4.c4_1", "c5", "c3")
          checkAnswer(resultDF, Row("a", 2, "e", "c"))
        }
      }
    }
  }

  test("sizeInBytes should be the total size of all files") {
    Seq("orc", "").foreach { useV1SourceReaderList =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceReaderList) {
        withTempDir { dir =>
          dir.delete()
          spark.range(1000).write.orc(dir.toString)
          val df = spark.read.orc(dir.toString)
          assert(df.queryExecution.optimizedPlan.stats.sizeInBytes === BigInt(getLocalDirSize(dir)))
        }
      }
    }
  }

  test("SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect") {
    Seq(1.0, 0.5).foreach { compressionFactor =>
      withSQLConf(SQLConf.FILE_COMPRESSION_FACTOR.key -> compressionFactor.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "250") {
        withTempPath { workDir =>
          // the file size is 486 bytes
          val workDirPath = workDir.getAbsolutePath
          val data1 = Seq(100, 200, 300, 400).toDF("count")
          data1.write.orc(workDirPath + "/data1")
          val df1FromFile = spark.read.orc(workDirPath + "/data1")
          val data2 = Seq(100, 200, 300, 400).toDF("count")
          data2.write.orc(workDirPath + "/data2")
          val df2FromFile = spark.read.orc(workDirPath + "/data2")
          val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
          if (compressionFactor == 0.5) {
            val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case bJoin: ColumnarBroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.nonEmpty)
            val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.isEmpty)
          } else {
            // compressionFactor is 1.0
            val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case bJoin: BroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.isEmpty)
            val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.nonEmpty)
          }
        }
      }
    }
  }

  test("File source v2: support partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      allFileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("p1 = 1 and p2 = 2 and value != \"a\"")

          val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
            case f: Filter => f.condition
          }
          assert(filterCondition.isDefined)
          // The partitions filters should be pushed down and no need to be reevaluated.
          assert(filterCondition.get.collectFirst {
            case a: AttributeReference if a.name == "p1" || a.name == "p2" => a
          }.isEmpty)

          val fileScan = df.queryExecution.executedPlan collectFirst {
            case BatchScanExec(_, f: FileScan) => f
          }
          assert(fileScan.nonEmpty)
          assert(fileScan.get.partitionFilters.nonEmpty)
          assert(fileScan.get.dataFilters.nonEmpty)
          assert(fileScan.get.planInputPartitions().forall { partition =>
            partition.asInstanceOf[FilePartition].files.forall { file =>
              file.filePath.contains("p1=1") && file.filePath.contains("p2=2")
            }
          })
          checkAnswer(df, Row("b", 1, 2))
        }
      }
    }
  }

  test("File source v2: support passing data filters to FileScan without partitionFilters") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      allFileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("value = 'a'")

          val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
            case f: Filter => f.condition
          }
          assert(filterCondition.isDefined)

          val fileScan = df.queryExecution.executedPlan collectFirst {
            case BatchScanExec(_, f: FileScan) => f
          }
          assert(fileScan.nonEmpty)
          assert(fileScan.get.partitionFilters.isEmpty)
          assert(fileScan.get.dataFilters.nonEmpty)
          checkAnswer(df, Row("a", 1, 2))
        }
      }
    }
  }

  test("File table location should include both values of option `path` and `paths`") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPaths(3) { paths =>
        paths.zipWithIndex.foreach { case (path, index) =>
          Seq(index).toDF("a").write.mode("overwrite").parquet(path.getCanonicalPath)
        }
        val df = spark
          .read
          .option("path", paths.head.getCanonicalPath)
          .parquet(paths(1).getCanonicalPath, paths(2).getCanonicalPath)
        df.queryExecution.optimizedPlan match {
          case PhysicalOperation(_, _, DataSourceV2ScanRelation(table: ParquetTable, _, _)) =>
            assert(table.paths.toSet == paths.map(_.getCanonicalPath).toSet)
          case _ =>
            throw new AnalysisException("Can not match ParquetTable in the query.")
        }
        checkAnswer(df, Seq(0, 1, 2).map(Row(_)))
      }
    }
  }

  test("SPARK-31116: Select nested schema with case insensitive mode") {
    // This test case failed at only Parquet. ORC is added for test coverage parity.
    Seq("orc", "parquet").foreach { format =>
      Seq("true", "false").foreach { nestedSchemaPruningEnabled =>
        withSQLConf(
          SQLConf.CASE_SENSITIVE.key -> "false",
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedSchemaPruningEnabled) {
          withTempPath { dir =>
            val path = dir.getCanonicalPath

            // Prepare values for testing nested parquet data
            spark
              .range(1L)
              .selectExpr("NAMED_STRUCT('lowercase', id, 'camelCase', id + 1) AS StructColumn")
              .write
              .format(format)
              .save(path)

            val exactSchema = "StructColumn struct<lowercase: LONG, camelCase: LONG>"

            checkAnswer(spark.read.schema(exactSchema).format(format).load(path), Row(Row(0, 1)))

            // In case insensitive manner, parquet's column cases are ignored
            val innerColumnCaseInsensitiveSchema =
              "StructColumn struct<Lowercase: LONG, camelcase: LONG>"
            checkAnswer(
              spark.read.schema(innerColumnCaseInsensitiveSchema).format(format).load(path),
              Row(Row(0, 1)))

            val rootColumnCaseInsensitiveSchema =
              "structColumn struct<lowercase: LONG, camelCase: LONG>"
            checkAnswer(
              spark.read.schema(rootColumnCaseInsensitiveSchema).format(format).load(path),
              Row(Row(0, 1)))
          }
        }
      }
    }
  }
}

object TestingUDT {

  @SQLUserDefinedType(udt = classOf[IntervalUDT])
  class IntervalData extends Serializable

  class IntervalUDT extends UserDefinedType[IntervalData] {

    override def sqlType: DataType = CalendarIntervalType
    override def serialize(obj: IntervalData): Any =
      throw new UnsupportedOperationException("Not implemented")
    override def deserialize(datum: Any): IntervalData =
      throw new UnsupportedOperationException("Not implemented")
    override def userClass: Class[IntervalData] = classOf[IntervalData]
  }

  @SQLUserDefinedType(udt = classOf[NullUDT])
  private[sql] class NullData extends Serializable

  private[sql] class NullUDT extends UserDefinedType[NullData] {

    override def sqlType: DataType = NullType
    override def serialize(obj: NullData): Any =
      throw new UnsupportedOperationException("Not implemented")
    override def deserialize(datum: Any): NullData =
      throw new UnsupportedOperationException("Not implemented")
    override def userClass: Class[NullData] = classOf[NullData]
  }
}
