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
package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite base for testing the redaction of DataSourceScanExec/BatchScanExec.
 */
abstract class DataSourceScanRedactionTest extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.redaction.string.regex", "file:/[^\\]\\s]+")

  final protected def isIncluded(queryExecution: QueryExecution, msg: String): Boolean = {
    queryExecution.toString.contains(msg) ||
      queryExecution.simpleString.contains(msg) ||
      queryExecution.stringWithStats.contains(msg)
  }

  protected def getRootPath(df: DataFrame): Path

  test("treeString is redacted") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)

      val rootPath = getRootPath(df)
      assert(rootPath.toString.contains(dir.toURI.getPath.stripSuffix("/")))

      assert(!df.queryExecution.sparkPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.executedPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.toString.contains(rootPath.getName))
      assert(!df.queryExecution.simpleString.contains(rootPath.getName))

      val replacement = "*********"
      assert(df.queryExecution.sparkPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.executedPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.toString.contains(replacement))
      assert(df.queryExecution.simpleString.contains(replacement))
    }
  }
}

/**
 * Suite that tests the redaction of DataSourceScanExec
 */
class DataSourceScanExecRedactionSuite extends DataSourceScanRedactionTest {

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
      .set("spark.oap.sql.columnar.testing", "true")
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "orc")

  override protected def getRootPath(df: DataFrame): Path =
    df.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
      .asInstanceOf[FileSourceScanExec].relation.location.rootPaths.head

  test("explain is redacted using SQLConf") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)
      val replacement = "*********"

      // Respect SparkConf and replace file:/
      assert(isIncluded(df.queryExecution, replacement))

      assert(isIncluded(df.queryExecution, "FileScan"))
      assert(!isIncluded(df.queryExecution, "file:/"))

      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "(?i)FileScan") {
        // Respect SQLConf and replace FileScan
        assert(isIncluded(df.queryExecution, replacement))

        assert(!isIncluded(df.queryExecution, "FileScan"))
        assert(isIncluded(df.queryExecution, "file:/"))
      }
    }
  }

  test("FileSourceScanExec metadata") {
    withTempPath { path =>
      val dir = path.getCanonicalPath
      spark.range(0, 10).write.orc(dir)
      val df = spark.read.orc(dir)

      assert(isIncluded(df.queryExecution, "Format"))
      assert(isIncluded(df.queryExecution, "ReadSchema"))
      assert(isIncluded(df.queryExecution, "Batched"))
      assert(isIncluded(df.queryExecution, "PartitionFilters"))
      assert(isIncluded(df.queryExecution, "PushedFilters"))
      assert(isIncluded(df.queryExecution, "DataFilters"))
      assert(isIncluded(df.queryExecution, "Location"))
    }
  }
}

/**
 * Suite that tests the redaction of BatchScanExec.
 */
class DataSourceV2ScanExecRedactionSuite extends DataSourceScanRedactionTest {

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
      .set("spark.oap.sql.columnar.testing", "true")
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "")

  override protected def getRootPath(df: DataFrame): Path =
    df.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
      .asInstanceOf[BatchScanExec].scan.asInstanceOf[OrcScan].fileIndex.rootPaths.head

  test("explain is redacted using SQLConf") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)
      val replacement = "*********"

      // Respect SparkConf and replace file:/
      assert(isIncluded(df.queryExecution, replacement))
      assert(isIncluded(df.queryExecution, "BatchScan"))
      assert(!isIncluded(df.queryExecution, "file:/"))

      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "(?i)BatchScan") {
        // Respect SQLConf and replace FileScan
        assert(isIncluded(df.queryExecution, replacement))

        assert(!isIncluded(df.queryExecution, "BatchScan"))
        assert(isIncluded(df.queryExecution, "file:/"))
      }
    }
  }

  test("FileScan description") {
    Seq("json", "orc", "parquet").foreach { format =>
      withTempPath { path =>
        val dir = path.getCanonicalPath
        spark.range(0, 10).write.format(format).save(dir)
        val df = spark.read.format(format).load(dir)

        withClue(s"Source '$format':") {
          assert(isIncluded(df.queryExecution, "ReadSchema"))
          assert(isIncluded(df.queryExecution, "BatchScan"))
          if (Seq("orc", "parquet").contains(format)) {
            assert(isIncluded(df.queryExecution, "PushedFilters"))
          }
          assert(isIncluded(df.queryExecution, "Location"))
        }
      }
    }
  }

  test("SPARK-30362: test input metrics for DSV2") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      Seq("json", "orc", "parquet").foreach { format =>
        withTempPath { path =>
          val dir = path.getCanonicalPath
          spark.range(0, 10).write.format(format).save(dir)
          val df = spark.read.format(format).load(dir)
          val bytesReads = new mutable.ArrayBuffer[Long]()
          val recordsRead = new mutable.ArrayBuffer[Long]()
          val bytesReadListener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
              recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
            }
          }
          sparkContext.addSparkListener(bytesReadListener)
          try {
            df.collect()
            sparkContext.listenerBus.waitUntilEmpty()
            assert(bytesReads.sum > 0)
            assert(recordsRead.sum == 10)
          } finally {
            sparkContext.removeSparkListener(bytesReadListener)
          }
        }
      }
    }
  }
}
