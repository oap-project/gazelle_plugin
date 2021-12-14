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

package org.apache.spark.sql.execution.adaptive

import java.io.File
import java.net.URI

import org.apache.logging.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.{Dataset, QueryTest, Row, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, QueryExecution, ReusedSubqueryExec, ShuffledRowRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.noop.NoopDataSource
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, REPARTITION, REPARTITION_WITH_NUM, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

class ColumnarAdaptiveQueryExecSuite
  extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test repartition")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  setupTestData()

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith(
              "AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, result)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    spark.sparkContext.listenerBus.waitUntilEmpty()
    // AQE will post `SparkListenerSQLAdaptiveExecutionUpdate` twice in case of subqueries that
    // exist out of query stages.
    val expectedFinalPlanCnt = adaptivePlan.find(_.subqueries.nonEmpty).map(_ => 2).getOrElse(1)
    assert(finalPlanCnt == expectedFinalPlanCnt)
    spark.sparkContext.removeSparkListener(listener)

    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelBroadcastHashJoin(plan: SparkPlan): Seq[BroadcastHashJoinExec] = {
    collect(plan) {
      case j: BroadcastHashJoinExec => j
    }
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) {
      case j: SortMergeJoinExec => j
    }
  }

  private def findTopLevelBaseJoin(plan: SparkPlan): Seq[BaseJoinExec] = {
    collect(plan) {
      case j: BaseJoinExec => j
    }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectWithSubqueries(plan) {
      case e: ReusedSubqueryExec => e
    }
  }

  private def checkNumLocalShuffleReaders(
                                           plan: SparkPlan, numShufflesWithoutLocalReader: Int = 0): Unit = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReaders = collect(plan) {
      case reader: CustomShuffleReaderExec if reader.isLocalReader => reader
    }
    numLocalReaders.foreach { r =>
      val rdd = r.execute()
      val parts = rdd.partitions
      assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles === (numLocalReaders.length + numShufflesWithoutLocalReader))
  }

  private def checkInitialPartitionNum(df: Dataset[_], numPartition: Int): Unit = {
    // repartition obeys initialPartitionNum when adaptiveExecutionEnabled
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[AdaptiveSparkPlanExec])
    val shuffle = plan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.collect {
      case s: ShuffleExchangeExec => s
    }
    assert(shuffle.size == 1)
    assert(shuffle(0).outputPartitioning.numPartitions == numPartition)
  }

  test("Columnar exchange reuse") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT value FROM testData join testData2 ON key = a " +
          "join (SELECT value v from testData join testData3 ON key = a) on value = v")
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
    }
  }
}
