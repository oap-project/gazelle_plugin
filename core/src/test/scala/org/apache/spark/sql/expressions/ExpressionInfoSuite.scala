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

package org.apache.spark.sql.expressions

import scala.collection.parallel.immutable.ParVector
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ExpressionInfoSuite extends SparkFunSuite with SharedSparkSession {

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

  test("Replace _FUNC_ in ExpressionInfo") {
    val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier("upper"))
    assert(info.getName === "upper")
    assert(info.getClassName === "org.apache.spark.sql.catalyst.expressions.Upper")
    assert(info.getUsage === "upper(str) - Returns `str` with all characters changed to uppercase.")
    assert(info.getExamples.contains("> SELECT upper('SparkSql');"))
    assert(info.getSince === "1.0.1")
    assert(info.getNote === "")
    assert(info.getExtended.contains("> SELECT upper('SparkSql');"))
  }

  test("group info in ExpressionInfo") {
    val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier("sum"))
    assert(info.getGroup === "agg_funcs")

    Seq("agg_funcs", "array_funcs", "datetime_funcs", "json_funcs", "map_funcs", "window_funcs")
        .foreach { groupName =>
      val info = new ExpressionInfo(
        "testClass", null, "testName", null, "", "", "", groupName, "", "")
      assert(info.getGroup === groupName)
    }

    val errMsg = intercept[IllegalArgumentException] {
      val invalidGroupName = "invalid_group_funcs"
      new ExpressionInfo("testClass", null, "testName", null, "", "", "", invalidGroupName, "", "")
    }.getMessage
    assert(errMsg.contains("'group' is malformed in the expression [testName]."))
  }

  test("error handling in ExpressionInfo") {
    val errMsg1 = intercept[IllegalArgumentException] {
      val invalidNote = "  invalid note"
      new ExpressionInfo("testClass", null, "testName", null, "", "", invalidNote, "", "", "")
    }.getMessage
    assert(errMsg1.contains("'note' is malformed in the expression [testName]."))

    val errMsg2 = intercept[IllegalArgumentException] {
      val invalidSince = "-3.0.0"
      new ExpressionInfo("testClass", null, "testName", null, "", "", "", "", invalidSince, "")
    }.getMessage
    assert(errMsg2.contains("'since' is malformed in the expression [testName]."))

    val errMsg3 = intercept[IllegalArgumentException] {
      val invalidDeprecated = "  invalid deprecated"
      new ExpressionInfo("testClass", null, "testName", null, "", "", "", "", "", invalidDeprecated)
    }.getMessage
    assert(errMsg3.contains("'deprecated' is malformed in the expression [testName]."))
  }

  test("using _FUNC_ instead of function names in examples") {
    val exampleRe = "(>.*;)".r
    val setStmtRe = "(?i)^(>\\s+set\\s+).+".r
    val ignoreSet = Set(
      // Examples for CaseWhen show simpler syntax:
      // `CASE WHEN ... THEN ... WHEN ... THEN ... END`
      "org.apache.spark.sql.catalyst.expressions.CaseWhen",
      // _FUNC_ is replaced by `locate` but `locate(... IN ...)` is not supported
      "org.apache.spark.sql.catalyst.expressions.StringLocate",
      // _FUNC_ is replaced by `%` which causes a parsing error on `SELECT %(2, 1.8)`
      "org.apache.spark.sql.catalyst.expressions.Remainder",
      // Examples demonstrate alternative names, see SPARK-20749
      "org.apache.spark.sql.catalyst.expressions.Length")
    spark.sessionState.functionRegistry.listFunction().foreach { funcId =>
      val info = spark.sessionState.catalog.lookupFunctionInfo(funcId)
      val className = info.getClassName
      withClue(s"Expression class '$className'") {
        val exprExamples = info.getOriginalExamples
        if (!exprExamples.isEmpty && !ignoreSet.contains(className)) {
          assert(exampleRe.findAllIn(exprExamples).toIterable
            .filter(setStmtRe.findFirstIn(_).isEmpty) // Ignore SET commands
            .forall(_.contains("_FUNC_")))
        }
      }
    }
  }

  ignore("check outputs of expression examples") {
    def unindentAndTrim(s: String): String = {
      s.replaceAll("\n\\s+", "\n").trim
    }
    val beginSqlStmtRe = "  > ".r
    val endSqlStmtRe = ";\n".r
    def checkExampleSyntax(example: String): Unit = {
      val beginStmtNum = beginSqlStmtRe.findAllIn(example).length
      val endStmtNum = endSqlStmtRe.findAllIn(example).length
      assert(beginStmtNum === endStmtNum,
        "The number of ` > ` does not match to the number of `;`")
    }
    val exampleRe = """^(.+);\n(?s)(.+)$""".r
    val ignoreSet = Set(
      // One of examples shows getting the current timestamp
      "org.apache.spark.sql.catalyst.expressions.UnixTimestamp",
      "org.apache.spark.sql.catalyst.expressions.CurrentDate",
      "org.apache.spark.sql.catalyst.expressions.CurrentTimestamp",
      "org.apache.spark.sql.catalyst.expressions.Now",
      // Random output without a seed
      "org.apache.spark.sql.catalyst.expressions.Rand",
      "org.apache.spark.sql.catalyst.expressions.Randn",
      "org.apache.spark.sql.catalyst.expressions.Shuffle",
      "org.apache.spark.sql.catalyst.expressions.Uuid",
      // The example calls methods that return unstable results.
      "org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection")

    val parFuncs = new ParVector(spark.sessionState.functionRegistry.listFunction().toVector)
    parFuncs.foreach { funcId =>
      // Examples can change settings. We clone the session to prevent tests clashing.
      val clonedSpark = spark.cloneSession()
      // Coalescing partitions can change result order, so disable it.
      clonedSpark.sessionState.conf.setConf(SQLConf.COALESCE_PARTITIONS_ENABLED, false)
      val info = clonedSpark.sessionState.catalog.lookupFunctionInfo(funcId)
      val className = info.getClassName
      if (!ignoreSet.contains(className)) {
        withClue(s"Function '${info.getName}', Expression class '$className'") {
          val example = info.getExamples
          checkExampleSyntax(example)
          example.split("  > ").toList.foreach {
            case exampleRe(sql, output) =>
              val df = clonedSpark.sql(sql)
              val actual = unindentAndTrim(
                hiveResultString(df.queryExecution.executedPlan).mkString("\n"))
              val expected = unindentAndTrim(output)
              assert(actual === expected)
            case _ =>
          }
        }
      }
    }
  }
}
