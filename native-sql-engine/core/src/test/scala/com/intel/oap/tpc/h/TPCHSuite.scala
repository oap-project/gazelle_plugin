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

package com.intel.oap.tpc.h

import java.io.{FileOutputStream, InputStreamReader, OutputStreamWriter}
import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import java.util.{Scanner, StringTokenizer}

import com.intel.oap.tags.{BroadcastHashJoinMode, SortMergeJoinMode, TestAndWriteLogs}
import com.intel.oap.tpc.MallocUtils
import com.intel.oap.tpc.h.TPCHSuite.RAMMonitor
import com.intel.oap.tpc.util.TPCRunner
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler.ChartTheme
import org.knowm.xchart.{BitmapEncoder, XYChartBuilder}

import scala.collection.mutable.ArrayBuffer

class TPCHSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCH_QUERIES_RESOURCE = "tpch"
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"

  private var runner: TPCRunner = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "false")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
      .set("spark.oap.sql.columnar.sortmergejoin", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    LogManager.getRootLogger.setLevel(Level.WARN)
    val tGen = new TPCHTableGen(spark, 0.1D, TPCH_WRITE_PATH)
    tGen.gen()
    tGen.createTables()
    runner = new TPCRunner(spark, TPCH_QUERIES_RESOURCE)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("memory usage test - broadcast hash join", TestAndWriteLogs, BroadcastHashJoinMode) {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "1TB")) {
      runMemoryUsageTest(comment = "BHJ")
    }
  }

  test("memory usage test - sort merge join", TestAndWriteLogs, SortMergeJoinMode) {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.oap.sql.columnar.sortmergejoin", "true")) {
      runMemoryUsageTest(comment = "SMJ", exclusions = Array("q12"))
    }
  }

  ignore("q12 SMJ failure") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.oap.sql.columnar.sortmergejoin", "true")) {
      runner.runTPCQuery("q12", 1, true)
    }
  }

  private def runMemoryUsageTest(exclusions: Array[String] = Array[String](), comment: String = ""): Unit = {
    val enableTPCHTests = Option(System.getenv("ENABLE_TPCH_TESTS"))
    if (!enableTPCHTests.exists(_.toBoolean)) {
      TPCHSuite.stdoutLog("TPCH tests are not enabled, Skipping... ")
      return
    }

    val commentTextOutputPath = System.getenv("COMMENT_TEXT_OUTPUT_PATH")
    if (StringUtils.isEmpty(commentTextOutputPath)) {
      TPCHSuite.stdoutLog("No COMMENT_TEXT_OUTPUT_PATH set. Aborting... ")
      throw new IllegalArgumentException("No COMMENT_TEXT_OUTPUT_PATH set")
    }

    val commentImageOutputPath = System.getenv("COMMENT_IMAGE_OUTPUT_PATH")
    if (StringUtils.isEmpty(commentImageOutputPath)) {
      TPCHSuite.stdoutLog("No COMMENT_IMAGE_OUTPUT_PATH set. Aborting... ")
      throw new IllegalArgumentException("No COMMENT_IMAGE_OUTPUT_PATH set")
    }

    val ramMonitor = new RAMMonitor()
    ramMonitor.startMonitorDaemon()
    val writer = new OutputStreamWriter(new FileOutputStream(commentTextOutputPath))

    def writeCommentLine(line: String): Unit = {
      writer.write(line)
      writer.write('\n')
      writer.flush()
      TPCHSuite.stdoutLog("Wrote log line: " + line)
    }

    writeCommentLine("GitHub Action TPC-H RAM usage test starts to run. " +
        "Report will be continuously updated in following block.")

    def genReportLine(): String = {
      val jvmHeapUsed = ramMonitor.getJVMHeapUsed()
      val jvmHeapTotal = ramMonitor.getJVMHeapTotal()
      val processRes = ramMonitor.getCurrentPIDRAMUsed()
      val os = ramMonitor.getOSRAMUsed()
      val lineBuilder = new StringBuilder
      lineBuilder
          .append("Off-Heap Allocated: %d MB,".format((processRes - jvmHeapTotal) / 1000L))
          .append(' ')
          .append("Off-Heap Allocation Ratio: %.2f%%,".format((processRes - jvmHeapTotal) * 100.0D / processRes))
          .append(' ')
          .append("JVM Heap Used: %d MB,".format(jvmHeapUsed / 1000L))
          .append(' ')
          .append("JVM Heap Total: %d MB,".format(jvmHeapTotal / 1000L))
          .append(' ')
          .append("Process Resident: %d MB,".format(processRes / 1000L))
          .append(' ')
          .append("OS Used: %d MB".format((os / 1000L)))
      val line = lineBuilder.toString()
      "Appending RAM report line: " + line
    }

    try {
      writeCommentLine("```")
      writeCommentLine("Before suite starts: %s".format(genReportLine()))
      (1 to 20).foreach { executionId =>
        writeCommentLine("Iteration %d:".format(executionId))
        runner.caseIds
            .filterNot(i => exclusions.toList.contains(i))
            .foreach(i => {
              runner.runTPCQuery(i, executionId)
              MallocUtils.mallocTrim()
              System.gc()
              System.gc()
              writeCommentLine("  Query %s: %s".format(i, genReportLine()))
              ramMonitor.writeImage("RAM Usage History (TPC-H)" +
                  (if (StringUtils.isEmpty(comment)) "" else " - %s".format(comment)), commentImageOutputPath)
            })
      }
    } catch {
      case e: Throwable =>
        writeCommentLine("Error executing TPC-H queries: %s".format(e.getMessage))
        throw e
    } finally {
      writeCommentLine("```")
      writer.close()
      ramMonitor.close()
    }
  }
}

object TPCHSuite {

  // not thread-safe
  class RAMMonitor() extends AutoCloseable {

    val executor = Executors.newSingleThreadScheduledExecutor()

    val heapUsedBuffer = ArrayBuffer[Int]()
    val heapTotalBuffer = ArrayBuffer[Int]()
    val pidRamUsedBuffer = ArrayBuffer[Int]()
    val osRamUsedBuffer = ArrayBuffer[Int]()

    var closed = false


    def getJVMHeapUsed(): Long = {
      val heapTotalBytes = Runtime.getRuntime.totalMemory()
      val heapUsed = (heapTotalBytes - Runtime.getRuntime.freeMemory()) / 1024L
      heapUsed
    }

    def getJVMHeapTotal(): Long = {
      val heapTotalBytes = Runtime.getRuntime.totalMemory()
      val heapTotal = heapTotalBytes / 1024L
      heapTotal
    }

    def getCurrentPIDRAMUsed(): Long = {
      val proc = Runtime.getRuntime.exec("ps -p " + getPID() + " -o rss")
      val in = new InputStreamReader(proc.getInputStream)
      val buff = new StringBuilder

      def scan: Unit = {
        while (true) {
          val ch = in.read()
          if (ch == -1) {
            return;
          }
          buff.append(ch.asInstanceOf[Char])
        }
      }
      scan
      in.close()
      val output = buff.toString()
      val scanner = new Scanner(output)
      scanner.nextLine()
      scanner.nextLine().toLong
    }

    private def getPID(): String = {
      val beanName = ManagementFactory.getRuntimeMXBean.getName
      return beanName.substring(0, beanName.indexOf('@'))
    }

    def getOSRAMUsed(): Long = {
      val proc = Runtime.getRuntime.exec("free")
      val in = new InputStreamReader(proc.getInputStream)
      val buff = new StringBuilder

      def scan: Unit = {
        while (true) {
          val ch = in.read()
          if (ch == -1) {
            return;
          }
          buff.append(ch.asInstanceOf[Char])
        }
      }
      scan
      in.close()
      val output = buff.toString()
      val scanner = new Scanner(output)
      scanner.nextLine()
      val memLine = scanner.nextLine()
      val tok = new StringTokenizer(memLine)
      tok.nextToken()
      tok.nextToken()
      return tok.nextToken().toLong
    }

    def startMonitorDaemon(): ScheduledFuture[_] = {
      if (closed) {
        throw new IllegalStateException()
      }
      executor.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          val pidRamUsed = getCurrentPIDRAMUsed()
          val osRamUsed = getOSRAMUsed()
          val heapUsed = getJVMHeapUsed()
          val heapTotal = getJVMHeapTotal()

          this.synchronized {
            heapUsedBuffer.append((heapUsed / 1024).toInt)
            heapTotalBuffer.append((heapTotal / 1024).toInt)
            pidRamUsedBuffer.append((pidRamUsed / 1024).toInt)
            osRamUsedBuffer.append((osRamUsed / 1024).toInt)
          }
        }
      }, 0L, 1000L, TimeUnit.MILLISECONDS)
    }


    def writeImage(title: String, chartOutputPath: String): Unit = {
      val chart = new XYChartBuilder()
          .width(768)
          .height(256)
          .title(title)
          .xAxisTitle("Up Time (Second)")
          .yAxisTitle("RAM Used (MB)")
          .theme(ChartTheme.XChart)
          .build
      chart.getStyler.setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter)

      this.synchronized {
        chart.addSeries("JVM Heap Used", heapUsedBuffer.toArray)
        chart.addSeries("JVM Heap Total", heapTotalBuffer.toArray)
        chart.addSeries("Process Res Total", pidRamUsedBuffer.toArray)
        chart.addSeries("OS Used Total", osRamUsedBuffer.toArray)
      }

      val out = new FileOutputStream(chartOutputPath)
      try {
        BitmapEncoder.saveBitmap(chart, out, BitmapFormat.PNG)
      } finally {
        out.close()
      }
    }

    override def close(): Unit = {
      executor.shutdown()
      executor.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
      closed = true
    }
  }

  def stdoutLog(line: Any): Unit = {
    println("[RAM Reporter] %s".format(line))
  }
}
