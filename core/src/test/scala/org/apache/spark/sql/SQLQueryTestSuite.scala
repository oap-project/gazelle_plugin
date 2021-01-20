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
import java.util.Locale
import java.util.regex.Pattern

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
//import org.apache.spark.tags.ExtendedSQLTest

/**
 * End-to-end test cases for SQL queries.
 *
 * Each case is loaded from a file in "spark/sql/core/src/test/resources/sql-tests/inputs".
 * Each case has a golden result file in "spark/sql/core/src/test/resources/sql-tests/results".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   build/sbt "~sql/test-only *SQLQueryTestSuite -- -z inline-table.sql"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * The format for input files is simple:
 *  1. A list of SQL queries separated by semicolons by default. If the semicolon cannot effectively
 *     separate the SQL queries in the test file(e.g. bracketed comments), please use
 *     --QUERY-DELIMITER-START and --QUERY-DELIMITER-END. Lines starting with
 *     --QUERY-DELIMITER-START and --QUERY-DELIMITER-END represent the beginning and end of a query,
 *     respectively. Code that is not surrounded by lines that begin with --QUERY-DELIMITER-START
 *     and --QUERY-DELIMITER-END is still separated by semicolons.
 *  2. Lines starting with -- are treated as comments and ignored.
 *  3. Lines starting with --SET are used to specify the configs when running this testing file. You
 *     can set multiple configs in one --SET, using comma to separate them. Or you can use multiple
 *     --SET statements.
 *  4. Lines starting with --IMPORT are used to load queries from another test file.
 *  5. Lines starting with --CONFIG_DIM are used to specify config dimensions of this testing file.
 *     The dimension name is decided by the string after --CONFIG_DIM. For example, --CONFIG_DIM1
 *     belongs to dimension 1. One dimension can have multiple lines, each line representing one
 *     config set (one or more configs, separated by comma). Spark will run this testing file many
 *     times, each time picks one config set from each dimension, until all the combinations are
 *     tried. For example, if dimension 1 has 2 lines, dimension 2 has 3 lines, this testing file
 *     will be run 6 times (cartesian product).
 *
 * For example:
 * {{{
 *   -- this is a comment
 *   select 1, -1;
 *   select current_date;
 * }}}
 *
 * The format for golden result files look roughly like:
 * {{{
 *   -- some header information
 *
 *   -- !query
 *   select 1, -1
 *   -- !query schema
 *   struct<...schema...>
 *   -- !query output
 *   ... data row 1 ...
 *   ... data row 2 ...
 *   ...
 *
 *   -- !query
 *   ...
 * }}}
 *
 * Note that UDF tests work differently. After the test files under 'inputs/udf' directory are
 * detected, it creates three test cases:
 *
 *  - Scala UDF test case with a Scalar UDF registered as the name 'udf'.
 *
 *  - Python UDF test case with a Python UDF registered as the name 'udf'
 *    iff Python executable and pyspark are available.
 *
 *  - Scalar Pandas UDF test case with a Scalar Pandas UDF registered as the name 'udf'
 *    iff Python executable, pyspark, pandas and pyarrow are available.
 *
 * Therefore, UDF test cases should have single input and output files but executed by three
 * different types of UDFs. See 'udf/udf-inner-join.sql' as an example.
 */
//@ExtendedSQLTest
class SQLQueryTestSuite extends QueryTest with SharedSparkSession {

  import IntegratedUDFTestUtils._

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  protected val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    val sparkHome = {
      assert(sys.props.contains("spark.test.home") ||
        sys.env.contains("SPARK_HOME"), "spark.test.home or SPARK_HOME is not set.")
      sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    }

    java.nio.file.Paths.get(sparkHome,
      "sql", "core", "src", "test", "resources", "sql-tests").toFile
  }

  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  protected val validFileExtensions = ".sql"

  private val notIncludedMsg = "[not included in comparison]"
  private val clsName = this.getClass.getCanonicalName

  protected val emptySchema = StructType(Seq.empty).catalogString

  protected override def sparkConf: SparkConf = super.sparkConf
    // Fewer shuffle partitions to speed up testing.
    .set(SQLConf.SHUFFLE_PARTITIONS, 4)

  /** List of test cases to ignore, in lower cases. */
  protected def blackList: Set[String] = Set(
    "blacklist.sql"   // Do NOT remove this one. It is here to test the blacklist functionality.
  )

  // Create all the test cases.
  listTestCases.foreach(createScalaTestCase)

  /** A single SQL query's output. */
  protected case class QueryOutput(sql: String, schema: String, output: String) {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query schema\n" +
        schema + "\n" +
        s"-- !query output\n" +
        output
    }
  }

  /** A test case. */
  protected trait TestCase {
    val name: String
    val inputFile: String
    val resultFile: String
  }

  /**
   * traits that indicate UDF or PgSQL to trigger the code path specific to each. For instance,
   * PgSQL tests require to register some UDF functions.
   */
  protected trait PgSQLTest

  /**
   * traits that indicate ANSI-related tests with the ANSI mode enabled.
   */
  protected trait AnsiTest

  protected trait UDFTest {
    val udf: TestUDF
  }

  /** A regular test case. */
  protected case class RegularTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase

  /** A PostgreSQL test case. */
  protected case class PgSQLTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with PgSQLTest

  /** A UDF test case. */
  protected case class UDFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF) extends TestCase with UDFTest

  /** A UDF PostgreSQL test case. */
  protected case class UDFPgSQLTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF) extends TestCase with UDFTest with PgSQLTest

  /** An ANSI-related test case. */
  protected case class AnsiTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with AnsiTest

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
        testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      test(testCase.name) { /* Do nothing */ }
    } else testCase match {
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && !shouldTestPythonUDFs =>
        test(s"${testCase.name} is skipped because " +
          s"[$pythonExec] and/or pyspark were not available.") {
          /* Do nothing */
        }
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && !shouldTestScalarPandasUDFs =>
        test(s"${testCase.name} is skipped because pyspark," +
          s"pandas and/or pyarrow were not available in [$pythonExec].") {
          /* Do nothing */
        }
      case _ =>
        // Create a test case to run this case.
        test(testCase.name) {
          runTest(testCase)
        }
    }
  }

  /** Run a test case. */
  protected def runTest(testCase: TestCase): Unit = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    def splitCommentsAndCodes(input: String) = input.split("\n").partition { line =>
      val newLine = line.trim
      newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = splitCommentsAndCodes(input)

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap { testCaseName =>
      listTestCases.find(_.name == testCaseName).map { testCase =>
        val input = fileToString(new File(testCase.inputFile))
        val (_, code) = splitCommentsAndCodes(input)
        code
      }
    }.flatten

    val allCode = importedCode ++ code
    val tempQueries = if (allCode.exists(_.trim.startsWith("--QUERY-DELIMITER"))) {
      // Although the loop is heavy, only used for bracketed comments test.
      val querys = new ArrayBuffer[String]
      val otherCodes = new ArrayBuffer[String]
      var tempStr = ""
      var start = false
      for (c <- allCode) {
        if (c.trim.startsWith("--QUERY-DELIMITER-START")) {
          start = true
          querys ++= splitWithSemicolon(otherCodes.toSeq)
          otherCodes.clear()
        } else if (c.trim.startsWith("--QUERY-DELIMITER-END")) {
          start = false
          querys += s"\n${tempStr.stripSuffix(";")}"
          tempStr = ""
        } else if (start) {
          tempStr += s"\n$c"
        } else {
          otherCodes += c
        }
      }
      if (otherCodes.nonEmpty) {
        querys ++= splitWithSemicolon(otherCodes.toSeq)
      }
      querys.toSeq
    } else {
      splitWithSemicolon(allCode).toSeq
    }

    // List of SQL queries to run
    val queries = tempQueries.map(_.trim).filter(_ != "").toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")

    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    val settings = settingLines.flatMap(_.split(",").map { kv =>
      val (conf, value) = kv.span(_ != '=')
      conf.trim -> value.substring(1).trim
    })

    if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings)
    } else {
      // A config dimension has multiple config sets, and a config set has multiple configs.
      // - config dim:     Seq[Seq[(String, String)]]
      //   - config set:   Seq[(String, String)]
      //     - config:     (String, String))
      // We need to do cartesian product for all the config dimensions, to get a list of
      // config sets, and run the query once for each config set.
      val configDimLines = comments.filter(_.startsWith("--CONFIG_DIM")).map(_.substring(12))
      val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).mapValues { lines =>
        lines.map(_.dropWhile(_ != ' ').substring(1)).map(_.split(",").map { kv =>
          val (conf, value) = kv.span(_ != '=')
          conf.trim -> value.substring(1).trim
        }.toSeq).toSeq
      }

      val configSets = configDims.values.foldLeft(Seq(Seq[(String, String)]())) { (res, dim) =>
        dim.flatMap { configSet => res.map(_ ++ configSet) }
      }

      configSets.foreach { configSet =>
        try {
          runQueries(queries, testCase, settings ++ configSet)
        } catch {
          case e: Throwable =>
            val configs = configSet.map {
              case (k, v) => s"$k=$v"
            }
            logError(s"Error using configs: ${configs.mkString(",")}")
            throw e
        }
      }
    }
  }

  protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Seq[(String, String)]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    testCase match {
      case udfTestCase: UDFTest =>
        registerTestUDF(udfTestCase.udf, localSparkSession)
      case _ =>
    }

    testCase match {
      case _: PgSQLTest =>
        // booleq/boolne used by boolean.sql
        localSparkSession.udf.register("booleq", (b1: Boolean, b2: Boolean) => b1 == b2)
        localSparkSession.udf.register("boolne", (b1: Boolean, b2: Boolean) => b1 != b2)
        // vol used by boolean.sql and case.sql.
        localSparkSession.udf.register("vol", (s: String) => s)
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
      case _: AnsiTest =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
      case _ =>
    }

    if (configSet.nonEmpty) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = configSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val (schema, output) = handleExceptions(getNormalizedResult(localSparkSession, sql))
      // We might need to do some query canonicalization in the future.
      QueryOutput(
        sql = sql,
        schema = schema,
        output = output.mkString("\n").replaceAll("\\s+$", ""))
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of queries: ${outputs.size}\n\n\n" +
        outputs.zipWithIndex.map{case (qr, i) => qr.toString}.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    // This is a temporary workaround for SPARK-28894. The test names are truncated after
    // the last dot due to a bug in SBT. This makes easier to debug via Jenkins test result
    // report. See SPARK-28894.
    // See also SPARK-29127. It is difficult to see the version information in the failed test
    // cases so the version information related to Python was also added.
    val clue = testCase match {
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && shouldTestScalarPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case _ =>
        s"${testCase.name}${System.lineSeparator()}"
    }

    withClue(clue) {
      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.*\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            s"Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          QueryOutput(
            sql = segments(i * 3 + 1).trim,
            schema = segments(i * 3 + 2).trim,
            output = segments(i * 3 + 3).replaceAll("\\s+$", "")
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        assertResult(expected.schema,
          s"Schema did not match for query #$i\n${expected.sql}: $output") {
          output.schema
        }
        assertResult(expected.output, s"Result did not match" +
          s" for query #$i\n${expected.sql}") { output.output }
      }
    }
  }

  /**
   * This method handles exceptions occurred during query execution as they may need special care
   * to become comparable to the expected output.
   *
   * @param result a function that returns a pair of schema and output
   */
  protected def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
    try {
      result
    } catch {
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        (emptySchema, Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        val cause = s.getCause
        (emptySchema, Seq(cause.getClass.getName, cause.getMessage))
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (emptySchema, Seq(e.getClass.getName, e.getMessage))
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  private def getNormalizedResult(session: SparkSession, sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase
          | _: DescribeColumnCommand
          | _: DescribeRelation
          | _: DescribeColumnStatement => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
    val answer = SQLExecution.withNewExecutionId(df.queryExecution, Some(sql)) {
      hiveResultString(df.queryExecution.executedPlan).map(replaceNotIncludedMsg)
    }

    // If the output is not pre-sorted, sort it.
    if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)
  }

  protected def replaceNotIncludedMsg(line: String): String = {
    line.replaceAll("#\\d+", "#x")
      .replaceAll(
        s"Location.*$clsName/",
        s"Location $notIncludedMsg/{warehouse_dir}/")
      .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      .replaceAll("\\*\\(\\d+\\) ", "*") // remove the WholeStageCodegen codegenStageIds
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      if (file.getAbsolutePath.startsWith(
        s"$inputFilePath${File.separator}udf${File.separator}postgreSQL")) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map { udf =>
          UDFPgSQLTestCase(
            s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udf")) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map { udf =>
          UDFTestCase(
            s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}postgreSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}ansi")) {
        AnsiTestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
    }
  }

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession. */
  private def createTestTables(session: SparkSession): Unit = {
    import session.implicits._

    (1 to 100).map(i => (i, i.toString)).toDF("key", "value")
      .repartition(1)
      .write
      .format("parquet")
      .saveAsTable("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .write
      .format("parquet")
      .saveAsTable("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .write
      .format("parquet")
      .saveAsTable("mapdata")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("a int, b float")
      .load(testFile("test-data/postgresql/agg.data"))
      .write
      .format("parquet")
      .saveAsTable("aggtest")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/onek.data"))
      .write
      .format("parquet")
      .saveAsTable("onek")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/tenk.data"))
      .write
      .format("parquet")
      .saveAsTable("tenk1")
  }

  private def removeTestTables(session: SparkSession): Unit = {
    session.sql("DROP TABLE IF EXISTS testdata")
    session.sql("DROP TABLE IF EXISTS arraydata")
    session.sql("DROP TABLE IF EXISTS mapdata")
    session.sql("DROP TABLE IF EXISTS aggtest")
    session.sql("DROP TABLE IF EXISTS onek")
    session.sql("DROP TABLE IF EXISTS tenk1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTables(spark)
    RuleExecutor.resetMetrics()
  }

  override def afterAll(): Unit = {
    try {
      removeTestTables(spark)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
    } finally {
      super.afterAll()
    }
  }
}
