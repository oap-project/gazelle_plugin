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

package org.apache.spark.sql.travis

import java.io.{File, FilenameFilter}
import java.nio.file.{Files, Paths}

import com.intel.oap.execution.ArrowColumnarCachedBatchSerializer

import scala.collection.mutable.HashSet
import scala.concurrent.duration._
import org.apache.commons.io.FileUtils
import org.apache.spark.CleanerListener
import org.apache.spark.SparkConf
import org.apache.spark.executor.DataReadMethod._
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join, JoinStrategyHint, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.execution.{ExecSubqueryExpression, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK_2, MEMORY_ONLY}
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.{AccumulatorContext, Utils}

class TravisCachedTableWithArrowSerializerSuite extends QueryTest with SQLTestUtils
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(
      StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
      classOf[ArrowColumnarCachedBatchSerializer].getName)
  }

  setupTestData()

  override def afterEach(): Unit = {
    try {
      spark.catalog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  test("read from cached table and uncache with Arrow Serializer") {
    /* This UT is for row based cache fallback */
    spark.catalog.cacheTable("testData")
    checkAnswer(spark.table("testData"), testData.collect().toSeq)
    assertCached(spark.table("testData"))

    uncacheTable("testData")
    checkAnswer(spark.table("testData"), testData.collect().toSeq)
    assertCached(spark.table("testData"), 0)
  }

  test("read from columnar cached table and uncache with Arrow Serializer") {
    /* This UT is for columnar cache */
    val tempTable = testData.selectExpr("key + 1", "value")
    tempTable.createOrReplaceTempView("tempTable")

    spark.catalog.cacheTable("tempTable")
    checkAnswer(spark.table("tempTable"), tempTable.collect().toSeq)
    assertCached(spark.table("tempTable"))

    uncacheTable("tempTable")
    checkAnswer(spark.table("tempTable"), tempTable.collect().toSeq)
    assertCached(spark.table("tempTable"), 0)
  }
  
}