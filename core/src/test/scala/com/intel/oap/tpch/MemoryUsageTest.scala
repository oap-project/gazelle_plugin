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

package com.intel.oap.tpch

import java.io.{File, FileOutputStream, InputStreamReader, OutputStreamWriter}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Scanner, StringTokenizer}
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.intel.oap.tags.{CommentOnContextPR, TestAndWriteLogs}
import com.intel.oap.tpch.MemoryUsageTest.{commentOnContextPR, stdoutLog, RAMMonitor}
import io.prestosql.tpch._
import javax.imageio.ImageIO
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.codehaus.jackson.map.ObjectMapper
import org.knowm.xchart.{BitmapEncoder, XYChartBuilder}
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler.ChartTheme
import org.kohsuke.github.{GHIssueComment, GitHubBuilder}

import scala.collection.mutable.ArrayBuffer

class MemoryUsageTest extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"

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
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        //          .set("spark.sql.autoBroadcastJoinThreshold", "1")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  def createTPCHTables(): Unit = {
    // gen tpc-h data
    val scale = 0.1D
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // lineitem
    def lineItemGenerator = { () =>
      new LineItemGenerator(scale, 1, 1)
    }

    def lineItemSchema = {
      StructType(Seq(
        StructField("l_orderkey", LongType),
        StructField("l_partkey", LongType),
        StructField("l_suppkey", LongType),
        StructField("l_linenumber", IntegerType),
        StructField("l_quantity", LongType),
        StructField("l_extendedprice", DoubleType),
        StructField("l_discount", DoubleType),
        StructField("l_tax", DoubleType),
        StructField("l_returnflag", StringType),
        StructField("l_linestatus", StringType),
        StructField("l_commitdate", DateType),
        StructField("l_receiptdate", DateType),
        StructField("l_shipinstruct", StringType),
        StructField("l_shipmode", StringType),
        StructField("l_comment", StringType),
        StructField("l_shipdate", DateType)
      ))
    }

    def lineItemParser: LineItem => Row =
      lineItem =>
        Row(
          lineItem.getOrderKey,
          lineItem.getPartKey,
          lineItem.getSupplierKey,
          lineItem.getLineNumber,
          lineItem.getQuantity,
          lineItem.getExtendedPrice,
          lineItem.getDiscount,
          lineItem.getTax,
          lineItem.getReturnFlag,
          lineItem.getStatus,
          Date.valueOf(GenerateUtils.formatDate(lineItem.getCommitDate)),
          Date.valueOf(GenerateUtils.formatDate(lineItem.getReceiptDate)),
          lineItem.getShipInstructions,
          lineItem.getShipMode,
          lineItem.getComment,
          Date.valueOf(GenerateUtils.formatDate(lineItem.getShipDate))
        )

    // customer
    def customerGenerator = { () =>
      new CustomerGenerator(scale, 1, 1)
    }

    def customerSchema = {
      StructType(Seq(
        StructField("c_custkey", LongType),
        StructField("c_name", StringType),
        StructField("c_address", StringType),
        StructField("c_nationkey", LongType),
        StructField("c_phone", StringType),
        StructField("c_acctbal", DoubleType),
        StructField("c_comment", StringType),
        StructField("c_mktsegment", StringType)
      ))
    }

    def customerParser: Customer => Row =
      customer =>
        Row(
          customer.getCustomerKey,
          customer.getName,
          customer.getAddress,
          customer.getNationKey,
          customer.getPhone,
          customer.getAccountBalance,
          customer.getComment,
          customer.getMarketSegment,
        )

    def rowCountOf[U](itr: java.lang.Iterable[U]): Long = {
      var cnt = 0L
      val iterator = itr.iterator
      while (iterator.hasNext) {
        iterator.next()
        cnt = cnt + 1
      }
      cnt
    }

    // orders
    def orderGenerator = { () =>
      new OrderGenerator(scale, 1, 1)
    }

    def orderSchema = {
      StructType(Seq(
        StructField("o_orderkey", LongType),
        StructField("o_custkey", LongType),
        StructField("o_orderstatus", StringType),
        StructField("o_totalprice", DoubleType),
        StructField("o_orderpriority", StringType),
        StructField("o_clerk", StringType),
        StructField("o_shippriority", IntegerType),
        StructField("o_comment", StringType),
        StructField("o_orderdate", DateType)
      ))
    }

    def orderParser: Order => Row =
      order =>
        Row(
          order.getOrderKey,
          order.getCustomerKey,
          String.valueOf(order.getOrderStatus),
          order.getTotalPrice,
          order.getOrderPriority,
          order.getClerk,
          order.getShipPriority,
          order.getComment,
          Date.valueOf(GenerateUtils.formatDate(order.getOrderDate))
        )

    // partsupp
    def partSupplierGenerator = { () =>
      new PartSupplierGenerator(scale, 1, 1)
    }

    def partSupplierSchema = {
      StructType(Seq(
        StructField("ps_partkey", LongType),
        StructField("ps_suppkey", LongType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", DoubleType),
        StructField("ps_comment", StringType)
      ))
    }

    def partSupplierParser: PartSupplier => Row =
      ps =>
        Row(
          ps.getPartKey,
          ps.getSupplierKey,
          ps.getAvailableQuantity,
          ps.getSupplyCost,
          ps.getComment
        )

    // supplier
    def supplierGenerator = { () =>
      new SupplierGenerator(scale, 1, 1)
    }

    def supplierSchema = {
      StructType(Seq(
        StructField("s_suppkey", LongType),
        StructField("s_name", StringType),
        StructField("s_address", StringType),
        StructField("s_nationkey", LongType),
        StructField("s_phone", StringType),
        StructField("s_acctbal", DoubleType),
        StructField("s_comment", StringType)
      ))
    }

    def supplierParser: Supplier => Row =
      s =>
        Row(
          s.getSupplierKey,
          s.getName,
          s.getAddress,
          s.getNationKey,
          s.getPhone,
          s.getAccountBalance,
          s.getComment
        )

    // nation
    def nationGenerator = { () =>
      new NationGenerator()
    }

    def nationSchema = {
      StructType(Seq(
        StructField("n_nationkey", LongType),
        StructField("n_name", StringType),
        StructField("n_regionkey", LongType),
        StructField("n_comment", StringType)
      ))
    }

    def nationParser: Nation => Row =
      nation =>
        Row(
          nation.getNationKey,
          nation.getName,
          nation.getRegionKey,
          nation.getComment
        )

    // part
    def partGenerator = { () =>
      new PartGenerator(scale, 1, 1)
    }

    def partSchema = {
      StructType(Seq(
        StructField("p_partkey", LongType),
        StructField("p_name", StringType),
        StructField("p_mfgr", StringType),
        StructField("p_type", StringType),
        StructField("p_size", IntegerType),
        StructField("p_container", StringType),
        StructField("p_retailprice", DoubleType),
        StructField("p_comment", StringType),
        StructField("p_brand", StringType)
      ))
    }

    def partParser: Part => Row =
      part =>
        Row(
          part.getPartKey,
          part.getName,
          part.getManufacturer,
          part.getType,
          part.getSize,
          part.getContainer,
          part.getRetailPrice,
          part.getComment,
          part.getBrand
        )

    // region
    def regionGenerator = { () =>
      new RegionGenerator()
    }

    def regionSchema = {
      StructType(Seq(
        StructField("r_regionkey", LongType),
        StructField("r_name", StringType),
        StructField("r_comment", StringType)
      ))
    }

    def regionParser: Region => Row =
      region =>
        Row(
          region.getRegionKey,
          region.getName,
          region.getComment
        )

    def generate[U](tableName: String, schema: StructType, gen: () => java.lang.Iterable[U],
        parser: U => Row): Unit = {
      spark.range(0, rowCountOf(gen.apply()), 1L, 1)
          .mapPartitions { itr =>
            val lineItem = gen.apply()
            val lineItemItr = lineItem.iterator()
            val rows = itr.map { _ =>
              val item = lineItemItr.next()
              parser(item)
            }
            rows
          }(RowEncoder(schema))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(TPCH_WRITE_PATH + File.separator + tableName)
    }

    generate("lineitem", lineItemSchema, lineItemGenerator, lineItemParser)
    generate("customer", customerSchema, customerGenerator, customerParser)
    generate("orders", orderSchema, orderGenerator, orderParser)
    generate("partsupp", partSupplierSchema, partSupplierGenerator, partSupplierParser)
    generate("supplier", supplierSchema, supplierGenerator, supplierParser)
    generate("nation", nationSchema, nationGenerator, nationParser)
    generate("part", partSchema, partGenerator, partParser)
    generate("region", regionSchema, regionGenerator, regionParser)


    val files = new File(TPCH_WRITE_PATH).listFiles()
    files.foreach(file => {
      MemoryUsageTest.stdoutLog("Creating catalog table: " + file.getName)
      spark.catalog.createTable(file.getName, file.getAbsolutePath, "arrow")
      try {
        spark.catalog.recoverPartitions(file.getName)
      } catch {
        case _: Throwable =>
      }
    })
  }

  test("comment on context pr", CommentOnContextPR) {
    def run(): Unit = {
      val enableTPCHTests = Option(System.getenv("ENABLE_TPCH_TESTS"))
      if (!enableTPCHTests.exists(_.toBoolean)) {
        MemoryUsageTest.stdoutLog("TPCH tests are not enabled, Skipping... ")
        return
      }
      val commentContentPath = System.getenv("COMMENT_CONTENT_PATH")
      if (StringUtils.isEmpty(commentContentPath)) {
        MemoryUsageTest.stdoutLog("No COMMENT_CONTENT_PATH set. Aborting... ")
        throw new IllegalArgumentException("No COMMENT_CONTENT_PATH set")
      }

      val repoSlug = System.getenv("GITHUB_REPOSITORY")
      stdoutLog("Reading essential env variables... " +
        "Envs: GITHUB_REPOSITORY: %s" .format(repoSlug))

      if (StringUtils.isEmpty(repoSlug)) {
        throw new IllegalArgumentException("No GITHUB_REPOSITORY set")
      }

      val eventPath = System.getenv("PREVIOUS_EVENT_PATH")
      stdoutLog("Reading essential env variables... " +
        "Envs: PREVIOUS_EVENT_PATH: %s" .format(eventPath))

      if (StringUtils.isEmpty(eventPath)) {
        throw new IllegalArgumentException("No PREVIOUS_EVENT_PATH set")
      }

      val token = System.getenv("GITHUB_TOKEN")

      if (StringUtils.isEmpty(token)) {
        throw new IllegalArgumentException("No GITHUB_TOKEN set")
      }

      val ghEventPayloadJson = new ObjectMapper().readTree(FileUtils.readFileToString(new File(eventPath)))
      val prId = ghEventPayloadJson.get("number").asInt()

      commentOnContextPR(repoSlug, prId, token, FileUtils.readFileToString(new File(commentContentPath)))
    }
    run()
  }

  test("memory usage test long-run", TestAndWriteLogs) {
    def run(): Unit = {
      val enableTPCHTests = Option(System.getenv("ENABLE_TPCH_TESTS"))
      if (!enableTPCHTests.exists(_.toBoolean)) {
        MemoryUsageTest.stdoutLog("TPCH tests are not enabled, Skipping... ")
        return
      }

      LogManager.getRootLogger.setLevel(Level.WARN)
      val commentTextOutputPath = System.getenv("COMMENT_TEXT_OUTPUT_PATH")
      if (StringUtils.isEmpty(commentTextOutputPath)) {
        MemoryUsageTest.stdoutLog("No COMMENT_TEXT_OUTPUT_PATH set. Aborting... ")
        throw new IllegalArgumentException("No COMMENT_TEXT_OUTPUT_PATH set")
      }

      val commentImageOutputPath = System.getenv("COMMENT_IMAGE_OUTPUT_PATH")
      if (StringUtils.isEmpty(commentImageOutputPath)) {
        MemoryUsageTest.stdoutLog("No COMMENT_IMAGE_OUTPUT_PATH set. Aborting... ")
        throw new IllegalArgumentException("No COMMENT_IMAGE_OUTPUT_PATH set")
      }

      val ramMonitor = new RAMMonitor()
      ramMonitor.startMonitorDaemon()
      val writer = new OutputStreamWriter(new FileOutputStream(commentTextOutputPath))

      def writeCommentLine(line: String): Unit = {
        writer.write(line)
        writer.write('\n')
        writer.flush()
        MemoryUsageTest.stdoutLog("Wrote log line: " + line)
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
        createTPCHTables()
        writeCommentLine("```")
        writeCommentLine("Before suite starts: %s".format(genReportLine()))
        (1 to 20).foreach { executionId =>
          writeCommentLine("Iteration %d:".format(executionId))
          (1 to 22).foreach(i => {
            runTPCHQuery(i, executionId)
            MallocUtils.mallocTrim()
            System.gc()
            System.gc()
            writeCommentLine("  Query %d: %s".format(i, genReportLine()))
            ramMonitor.writeImage(commentImageOutputPath)
          })
        }
      } catch {
        case e: Throwable =>
          writeCommentLine("Error executing TPC-H queries: %s".format(e.getMessage))
      }
      writeCommentLine("```")
      writer.close()
      ramMonitor.close()
    }
    run()
  }

  private def runTPCHQuery(caseId: Int, roundId: Int): Unit = {
    val path = "tpch-queries/q" + caseId + ".sql";
    val absolute = MemoryUsageTest.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    MemoryUsageTest.stdoutLog("Running TPC-H query %d (round %d)... ".format(caseId, roundId))
    val df = spark.sql(sql)
    df.show(100)
  }
}

object MemoryUsageTest {

  private def locateResourcePath(resource: String): String = {
    classOf[MemoryUsageTest].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }

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


    def writeImage(chartOutputPath: String): Unit = {
      val chart = new XYChartBuilder()
          .width(768)
          .height(512)
          .title("RAM Usage History (TPC-H)")
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

  def commentOnContextPR(repoSlug: String, prId: Int, token: String, comment: String): Option[GHIssueComment] = {
    val inst = new GitHubBuilder()
        .withAppInstallationToken(token)
      .build()

    val repository = inst.getRepository(repoSlug)
    val pr = repository.getPullRequest(prId)
    val c = pr.comment(comment)
    stdoutLog("Comment successfully submitted. ")
    Some(c)
  }
  
  def stdoutLog(line: Any): Unit = {
    println("[RAM Reporter] %s".format(line))
  }
}
