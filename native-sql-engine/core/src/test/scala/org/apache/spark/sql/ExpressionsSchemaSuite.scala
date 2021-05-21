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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.test.SharedSparkSession
//import org.apache.spark.tags.ExtendedSQLTest

// scalastyle:off line.size.limit
/**
 * End-to-end test cases for SQL schemas of expression examples.
 * The golden result file is "spark/sql/core/src/test/resources/sql-functions/sql-expression-schema.md".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly *ExpressionsSchemaSuite"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly *ExpressionsSchemaSuite"
 * }}}
 *
 * For example:
 * {{{
 *   ...
 *   @ExpressionDescription(
 *     usage = "_FUNC_(str, n) - Returns the string which repeats the given string value n times.",
 *     examples = """
 *       Examples:
 *         > SELECT _FUNC_('123', 2);
 *          123123
 *     """,
 *     since = "1.5.0")
 *   case class StringRepeat(str: Expression, times: Expression)
 *   ...
 * }}}
 *
 * The format for golden result files look roughly like:
 * {{{
 *   ...
 *   | org.apache.spark.sql.catalyst.expressions.StringRepeat | repeat | SELECT repeat('123', 2) | struct<repeat(123, 2):string> |
 *   ...
 * }}}
 */
// scalastyle:on line.size.limit
//@ExtendedSQLTest
class ExpressionsSchemaSuite extends QueryTest with SharedSparkSession {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    java.nio.file.Paths.get(sparkHome,
      "sql", "core", "src", "test", "resources", "sql-functions").toFile
  }

  private val resultFile = new File(baseResourcePath, "sql-expression-schema.md")

  /** A single SQL query's SQL and schema. */
  protected case class QueryOutput(
      className: String,
      funcName: String,
      sql: String = "N/A",
      schema: String = "N/A") {
    override def toString: String = {
      s"| $className | $funcName | $sql | $schema |"
    }
  }

  test("Check schemas for expression examples") {
    val exampleRe = """^(.+);\n(?s)(.+)$""".r
    val funInfos = spark.sessionState.functionRegistry.listFunction().map { funcId =>
      spark.sessionState.catalog.lookupFunctionInfo(funcId)
    }

    val classFunsMap = funInfos.groupBy(_.getClassName).toSeq.sortBy(_._1).map {
      case (className, infos) => (className, infos.sortBy(_.getName))
    }
    val outputBuffer = new ArrayBuffer[String]
    val outputs = new ArrayBuffer[QueryOutput]
    val missingExamples = new ArrayBuffer[String]

    classFunsMap.foreach { kv =>
      val className = kv._1
      kv._2.foreach { funInfo =>
        val example = funInfo.getExamples
        val funcName = funInfo.getName.replaceAll("\\|", "&#124;")
        if (example == "") {
          val queryOutput = QueryOutput(className, funcName)
          outputBuffer += queryOutput.toString
          outputs += queryOutput
          missingExamples += funcName
        }

        // If expression exists 'Examples' segment, the first element is 'Examples'. Because
        // this test case is only used to print aliases of expressions for double checking.
        // Therefore, we only need to output the first SQL and its corresponding schema.
        // Note: We need to filter out the commands that set the parameters, such as:
        // SET spark.sql.parser.escapedStringLiterals=true
        example.split("  > ").tail.filterNot(_.trim.startsWith("SET")).take(1).foreach {
          case exampleRe(sql, _) =>
            val df = spark.sql(sql)
            val escapedSql = sql.replaceAll("\\|", "&#124;")
            val schema = df.schema.catalogString.replaceAll("\\|", "&#124;")
            val queryOutput = QueryOutput(className, funcName, escapedSql, schema)
            outputBuffer += queryOutput.toString
            outputs += queryOutput
          case _ =>
        }
      }
    }

    val header = Seq(
      s"<!-- Automatically generated by ${getClass.getSimpleName} -->",
      "## Summary",
      s"  - Number of queries: ${outputs.size}",
      s"  - Number of expressions that missing example: ${missingExamples.size}",
      s"  - Expressions missing examples: ${missingExamples.mkString(",")}",
      "## Schema of Built-in Functions",
      "| Class name | Function name or alias | Query example | Output schema |",
      "| ---------- | ---------------------- | ------------- | ------------- |"
    )

    if (regenerateGoldenFiles) {
      val goldenOutput = (header ++ outputBuffer).mkString("\n")
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    val outputSize = outputs.size
    val headerSize = header.size
    val (expectedMissingExamples, expectedOutputs) = {
      val expectedGoldenOutput = fileToString(resultFile)
      val lines = expectedGoldenOutput.split("\n")
      val expectedSize = lines.size

      assert(expectedSize == outputSize + headerSize,
        s"Expected $expectedSize blocks in result file but got " +
          s"${outputSize + headerSize}. Try regenerating the result files.")

      val numberOfQueries = lines(2).split(":")(1).trim.toInt
      val expectedOutputs = Seq.tabulate(outputSize) { i =>
        val segments = lines(i + headerSize).split('|')
        QueryOutput(
          className = segments(1).trim,
          funcName = segments(2).trim,
          sql = segments(3).trim,
          schema = segments(4).trim)
      }

      assert(numberOfQueries == expectedOutputs.size,
        s"expected outputs size: ${expectedOutputs.size} not same as numberOfQueries: " +
          s"$numberOfQueries record in result file. Try regenerating the result files.")

      val numberOfMissingExamples = lines(3).split(":")(1).trim.toInt
      val expectedMissingExamples = {
        val missingExamples = lines(4).split(":")(1).trim
        // Splitting on a empty string would return [""]
        if (missingExamples.nonEmpty) {
          missingExamples.split(",")
        } else {
          Array.empty[String]
        }
      }

      assert(numberOfMissingExamples == expectedMissingExamples.size,
        s"expected missing examples size: ${expectedMissingExamples.size} not same as " +
          s"numberOfMissingExamples: $numberOfMissingExamples " +
          "record in result file. Try regenerating the result files.")

      (expectedMissingExamples, expectedOutputs)
    }

    // Compare results.
    assert(expectedOutputs.size == outputSize,
      "The number of queries not equals the number of expected queries.")

    outputs.zip(expectedOutputs).foreach { case (output, expected) =>
      assert(expected.sql == output.sql, "SQL query did not match")
      assert(expected.schema == output.schema, s"Schema did not match for query ${expected.sql}")
    }

    // Compare expressions missing examples
    assert(expectedMissingExamples.length == missingExamples.size,
      "The number of missing examples not equals the number of expected missing examples.")

    missingExamples.zip(expectedMissingExamples).foreach { case (output, expected) =>
      assert(expected == output, "Missing example expression not match")
    }
  }
}
