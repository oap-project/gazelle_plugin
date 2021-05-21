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
package org.apache.spark.sql.execution.benchmark

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{SpecificParquetRecordReaderBase, VectorizedParquetRecordReader}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector


/**
 * Benchmark to measure data source read performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DataSourceReadBenchmark-results.txt".
 * }}}
 */
object DataSourceReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("DataSourceReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    sparkSession
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def prepareTable(dir: File, df: DataFrame, partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsCsvTable(testDf, dir.getCanonicalPath + "/csv")
    saveAsJsonTable(testDf, dir.getCanonicalPath + "/json")
    saveAsParquetTable(testDf, dir.getCanonicalPath + "/parquet")
    saveAsOrcTable(testDf, dir.getCanonicalPath + "/orc")
  }

  private def saveAsCsvTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "gzip").option("header", true).csv(dir)
    spark.read.option("header", true).csv(dir).createOrReplaceTempView("csvTable")
  }

  private def saveAsJsonTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "gzip").json(dir)
    spark.read.json(dir).createOrReplaceTempView("jsonTable")
  }

  private def saveAsParquetTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetTable")
  }

  private def saveAsOrcTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").orc(dir)
    spark.read.orc(dir).createOrReplaceTempView("orcTable")
  }

  def numericScanBenchmark(values: Int, dataType: DataType): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark = new Benchmark(
      s"SQL Single ${dataType.sql} Column Scan",
      values,
      output = output)

    // Benchmarks driving reader component directly.
    val parquetReaderBenchmark = new Benchmark(
      s"Parquet Reader Single ${dataType.sql} Column Scan",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM t1"))

        sqlBenchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(id) from csvTable").noop()
        }

        sqlBenchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(id) from jsonTable").noop()
        }

        sqlBenchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(id) from parquetTable").noop()
        }

        sqlBenchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from parquetTable").noop()
          }
        }

        sqlBenchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM orcTable").noop()
        }

        sqlBenchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM orcTable").noop()
          }
        }

        sqlBenchmark.run()

        // Driving the parquet reader in batch mode directly.
        val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir, "parquet")).toArray
        val enableOffHeapColumnVector = spark.sessionState.conf.offHeapColumnVectorEnabled
        val vectorizedReaderBatchSize = spark.sessionState.conf.parquetVectorizedReaderBatchSize
        parquetReaderBenchmark.addCase("ParquetReader Vectorized") { _ =>
          var longSum = 0L
          var doubleSum = 0.0
          val aggregateValue: (ColumnVector, Int) => Unit = dataType match {
            case ByteType => (col: ColumnVector, i: Int) => longSum += col.getByte(i)
            case ShortType => (col: ColumnVector, i: Int) => longSum += col.getShort(i)
            case IntegerType => (col: ColumnVector, i: Int) => longSum += col.getInt(i)
            case LongType => (col: ColumnVector, i: Int) => longSum += col.getLong(i)
            case FloatType => (col: ColumnVector, i: Int) => doubleSum += col.getFloat(i)
            case DoubleType => (col: ColumnVector, i: Int) => doubleSum += col.getDouble(i)
          }

          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              enableOffHeapColumnVector, vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.isNullAt(i)) aggregateValue(col, i)
                  i += 1
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        // Decoding in vectorized but having the reader return rows.
        parquetReaderBenchmark.addCase("ParquetReader Vectorized -> Row") { num =>
          var longSum = 0L
          var doubleSum = 0.0
          val aggregateValue: (InternalRow) => Unit = dataType match {
            case ByteType => (col: InternalRow) => longSum += col.getByte(0)
            case ShortType => (col: InternalRow) => longSum += col.getShort(0)
            case IntegerType => (col: InternalRow) => longSum += col.getInt(0)
            case LongType => (col: InternalRow) => longSum += col.getLong(0)
            case FloatType => (col: InternalRow) => doubleSum += col.getFloat(0)
            case DoubleType => (col: InternalRow) => doubleSum += col.getDouble(0)
          }

          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              enableOffHeapColumnVector, vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) aggregateValue(record)
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        parquetReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("SELECT CAST(value AS INT) AS c1, CAST(value as STRING) AS c2 FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from csvTable").noop()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from jsonTable").noop()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from parquetTable").noop()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(c1), sum(length(c2)) from parquetTable").noop()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM orcTable").noop()
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM orcTable").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def repeatedStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Repeated String", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("select cast((value % 200) + 10000 as STRING) as c1 from t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(length(c1)) from csvTable").noop()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(length(c1)) from jsonTable").noop()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(length(c1)) from parquetTable").noop()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from parquetTable").noop()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("select sum(length(c1)) from orcTable").noop()
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from orcTable").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT value % 2 AS p, value AS id FROM t1"), Some("p"))

        benchmark.addCase("Data column - CSV") { _ =>
          spark.sql("select sum(id) from csvTable").noop()
        }

        benchmark.addCase("Data column - Json") { _ =>
          spark.sql("select sum(id) from jsonTable").noop()
        }

        benchmark.addCase("Data column - Parquet Vectorized") { _ =>
          spark.sql("select sum(id) from parquetTable").noop()
        }

        benchmark.addCase("Data column - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from parquetTable").noop()
          }
        }

        benchmark.addCase("Data column - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM orcTable").noop()
        }

        benchmark.addCase("Data column - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM orcTable").noop()
          }
        }

        benchmark.addCase("Partition column - CSV") { _ =>
          spark.sql("select sum(p) from csvTable").noop()
        }

        benchmark.addCase("Partition column - Json") { _ =>
          spark.sql("select sum(p) from jsonTable").noop()
        }

        benchmark.addCase("Partition column - Parquet Vectorized") { _ =>
          spark.sql("select sum(p) from parquetTable").noop()
        }

        benchmark.addCase("Partition column - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(p) from parquetTable").noop()
          }
        }

        benchmark.addCase("Partition column - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p) FROM orcTable").noop()
        }

        benchmark.addCase("Partition column - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM orcTable").noop()
          }
        }

        benchmark.addCase("Both columns - CSV") { _ =>
          spark.sql("select sum(p), sum(id) from csvTable").noop()
        }

        benchmark.addCase("Both columns - Json") { _ =>
          spark.sql("select sum(p), sum(id) from jsonTable").noop()
        }

        benchmark.addCase("Both columns - Parquet Vectorized") { _ =>
          spark.sql("select sum(p), sum(id) from parquetTable").noop()
        }

        benchmark.addCase("Both columns - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(p), sum(id) from parquetTable").noop()
          }
        }

        benchmark.addCase("Both columns - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM orcTable").noop()
        }

        benchmark.addCase("Both columns - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM orcTable").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    val percentageOfNulls = fractionOfNulls * 100
    val benchmark =
      new Benchmark(s"String with Nulls Scan ($percentageOfNulls%)", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        spark.range(values).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c1, " +
            s"IF(RAND(2) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c2 FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(length(c2)) from csvTable where c1 is " +
            "not NULL and c2 is not NULL").noop()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(length(c2)) from jsonTable where c1 is " +
            "not NULL and c2 is not NULL").noop()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(length(c2)) from parquetTable where c1 is " +
            "not NULL and c2 is not NULL").noop()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c2)) from parquetTable where c1 is " +
              "not NULL and c2 is not NULL").noop()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir, "parquet")).toArray
        val enableOffHeapColumnVector = spark.sessionState.conf.offHeapColumnVectorEnabled
        val vectorizedReaderBatchSize = spark.sessionState.conf.parquetVectorizedReaderBatchSize
        benchmark.addCase("ParquetReader Vectorized") { num =>
          var sum = 0
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              enableOffHeapColumnVector, vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("c1" :: "c2" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  val row = rowIterator.next()
                  val value = row.getUTF8String(0)
                  if (!row.isNullAt(0) && !row.isNullAt(1)) sum += value.numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM orcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").noop()
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM orcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val benchmark = new Benchmark(
      s"Single Column Scan from $width columns",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        val middle = width / 2
        val selectExpr = (1 to width).map(i => s"value as c$i")
        spark.range(values).map(_ => Random.nextLong).toDF()
          .selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM csvTable").noop()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM jsonTable").noop()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM parquetTable").noop()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM parquetTable").noop()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM orcTable").noop()
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM orcTable").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SQL Single Numeric Column Scan") {
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach {
        dataType => numericScanBenchmark(1024 * 1024 * 15, dataType)
      }
    }
    runBenchmark("Int and String Scan") {
      intStringScanBenchmark(1024 * 1024 * 10)
    }
    runBenchmark("Repeated String Scan") {
      repeatedStringScanBenchmark(1024 * 1024 * 10)
    }
    runBenchmark("Partitioned Table Scan") {
      partitionTableScanBenchmark(1024 * 1024 * 15)
    }
    runBenchmark("String with Nulls Scan") {
      for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
        stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
      }
    }
    runBenchmark("Single Column Scan From Wide Columns") {
      for (columnWidth <- List(10, 50, 100)) {
        columnsBenchmark(1024 * 1024 * 1, columnWidth)
      }
    }
  }
}
