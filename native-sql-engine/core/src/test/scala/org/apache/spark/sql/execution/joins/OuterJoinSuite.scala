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

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

class OuterJoinSuite extends SparkPlanTest with SharedSparkSession {

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
      .set("spark.oap.sql.columnar.hashCompare", "true")

  private lazy val left = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(2, 100.0),
      Row(2, 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, 1.0),
      Row(3, 3.0),
      Row(5, 1.0),
      Row(6, 6.0),
      Row(null, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  private lazy val right = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(0, 0.0),
      Row(2, 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, -1.0),
      Row(2, -1.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(5, 3.0),
      Row(7, 7.0),
      Row(null, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val condition = {
    And((left.col("a") === right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testOuterJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      joinType: JoinType,
      condition: => Expression,
      expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan,
        Inner, Some(condition), JoinHint.NONE)
      ExtractEquiJoinKeys.unapply(join)
    }

    if (joinType != FullOuter) {
      ignore(s"$testName using ShuffledHashJoin") {
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            val buildSide = if (joinType == LeftOuter) BuildRight else BuildLeft
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements(spark.sessionState.conf).apply(
                ShuffledHashJoinExec(
                  leftKeys, rightKeys, joinType, buildSide, boundCondition, left, right)),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }

    if (joinType != FullOuter) {
      testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin") { _ =>
        val buildSide = joinType match {
          case LeftOuter => BuildRight
          case RightOuter => BuildLeft
          case _ => fail(s"Unsupported join type $joinType")
        }
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, buildSide, boundCondition, left, right),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }

    test(s"$testName using SortMergeJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(spark.sessionState.conf).apply(
              SortMergeJoinExec(leftKeys, rightKeys, joinType, boundCondition, left, right)),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build left") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildLeft, joinType, Some(condition)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build right") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildRight, joinType, Some(condition)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }

  // --- Basic outer joins ------------------------------------------------------------------------

  testOuterJoin(
    "basic left outer join",
    left,
    right,
    LeftOuter,
    condition,
    Seq(
      (null, null, null, null),
      (1, 2.0, null, null),
      (2, 100.0, null, null),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null),
      (5, 1.0, 5, 3.0),
      (6, 6.0, null, null)
    )
  )

  testOuterJoin(
    "basic right outer join",
    left,
    right,
    RightOuter,
    condition,
    Seq(
      (null, null, null, null),
      (null, null, 0, 0.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (null, null, 2, -1.0),
      (null, null, 2, -1.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0),
      (5, 1.0, 5, 3.0),
      (null, null, 7, 7.0)
    )
  )

  testOuterJoin(
    "basic full outer join",
    left,
    right,
    FullOuter,
    condition,
    Seq(
      (1, 2.0, null, null),
      (null, null, 2, -1.0),
      (null, null, 2, -1.0),
      (2, 100.0, null, null),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null),
      (5, 1.0, 5, 3.0),
      (6, 6.0, null, null),
      (null, null, 0, 0.0),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0),
      (null, null, 7, 7.0),
      (null, null, null, null),
      (null, null, null, null)
    )
  )

  // --- Both inputs empty ------------------------------------------------------------------------

  testOuterJoin(
    "left outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    LeftOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "right outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    RightOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "full outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    FullOuter,
    condition,
    Seq.empty
  )
}
