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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Concat, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, RepartitionByExpression, Sort}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, RefreshResource}
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Parser test cases for rules defined in [[SparkSqlParser]].
 *
 * See [[org.apache.spark.sql.catalyst.parser.PlanParserSuite]] for rules
 * defined in the Catalyst module.
 */
class SparkSqlParserSuite extends AnalysisTest {

  val newConf = new SQLConf
  private lazy val parser = new SparkSqlParser(newConf)

  /**
   * Normalizes plans:
   * - CreateTable the createTime in tableDesc will replaced by -1L.
   */
  override def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case CreateTable(tableDesc, mode, query) =>
        val newTableDesc = tableDesc.copy(createTime = -1L)
        CreateTable(newTableDesc, mode, query)
      case _ => plan // Don't transform
    }
  }

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    val normalized1 = normalizePlan(parser.parsePlan(sqlCommand))
    val normalized2 = normalizePlan(plan)
    comparePlans(normalized1, normalized2)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parser.parsePlan)(sqlCommand, messages: _*)

  test("refresh resource") {
    assertEqual("REFRESH prefix_path", RefreshResource("prefix_path"))
    assertEqual("REFRESH /", RefreshResource("/"))
    assertEqual("REFRESH /path///a", RefreshResource("/path///a"))
    assertEqual("REFRESH pat1h/112/_1a", RefreshResource("pat1h/112/_1a"))
    assertEqual("REFRESH pat1h/112/_1a/a-1", RefreshResource("pat1h/112/_1a/a-1"))
    assertEqual("REFRESH path-with-dash", RefreshResource("path-with-dash"))
    assertEqual("REFRESH \'path with space\'", RefreshResource("path with space"))
    assertEqual("REFRESH \"path with space 2\"", RefreshResource("path with space 2"))
    intercept("REFRESH a b", "REFRESH statements cannot contain")
    intercept("REFRESH a\tb", "REFRESH statements cannot contain")
    intercept("REFRESH a\nb", "REFRESH statements cannot contain")
    intercept("REFRESH a\rb", "REFRESH statements cannot contain")
    intercept("REFRESH a\r\nb", "REFRESH statements cannot contain")
    intercept("REFRESH @ $a$", "REFRESH statements cannot contain")
    intercept("REFRESH  ", "Resource paths cannot be empty in REFRESH statements")
    intercept("REFRESH", "Resource paths cannot be empty in REFRESH statements")
  }

  private def createTableUsing(
      table: String,
      database: Option[String] = None,
      tableType: CatalogTableType = CatalogTableType.MANAGED,
      storage: CatalogStorageFormat = CatalogStorageFormat.empty,
      schema: StructType = new StructType,
      provider: Option[String] = Some("parquet"),
      partitionColumnNames: Seq[String] = Seq.empty,
      bucketSpec: Option[BucketSpec] = None,
      mode: SaveMode = SaveMode.ErrorIfExists,
      query: Option[LogicalPlan] = None): CreateTable = {
    CreateTable(
      CatalogTable(
        identifier = TableIdentifier(table, database),
        tableType = tableType,
        storage = storage,
        schema = schema,
        provider = provider,
        partitionColumnNames = partitionColumnNames,
        bucketSpec = bucketSpec
      ), mode, query
    )
  }

  private def createTable(
      table: String,
      database: Option[String] = None,
      tableType: CatalogTableType = CatalogTableType.MANAGED,
      storage: CatalogStorageFormat = CatalogStorageFormat.empty.copy(
        inputFormat = HiveSerDe.sourceToSerDe("textfile").get.inputFormat,
        outputFormat = HiveSerDe.sourceToSerDe("textfile").get.outputFormat,
        serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")),
      schema: StructType = new StructType,
      provider: Option[String] = Some("hive"),
      partitionColumnNames: Seq[String] = Seq.empty,
      comment: Option[String] = None,
      mode: SaveMode = SaveMode.ErrorIfExists,
      query: Option[LogicalPlan] = None): CreateTable = {
    CreateTable(
      CatalogTable(
        identifier = TableIdentifier(table, database),
        tableType = tableType,
        storage = storage,
        schema = schema,
        provider = provider,
        partitionColumnNames = partitionColumnNames,
        comment = comment
      ), mode, query
    )
  }

  test("create table - schema") {
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) STORED AS textfile",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
      )
    )
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) " +
      "PARTITIONED BY (c INT, d STRING COMMENT 'test2')",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
          .add("c", IntegerType)
          .add("d", StringType, nullable = true, "test2"),
        partitionColumnNames = Seq("c", "d")
      )
    )
    assertEqual("CREATE TABLE my_tab(id BIGINT, nested STRUCT<col1: STRING,col2: INT>) " +
      "STORED AS textfile",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("id", LongType)
          .add("nested", (new StructType)
            .add("col1", StringType)
            .add("col2", IntegerType)
          )
      )
    )
    // Partitioned by a StructType should be accepted by `SparkSqlParser` but will fail an analyze
    // rule in `AnalyzeCreateTable`.
    assertEqual("CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) " +
      "PARTITIONED BY (nested STRUCT<col1: STRING,col2: INT>)",
      createTable(
        table = "my_tab",
        schema = (new StructType)
          .add("a", IntegerType, nullable = true, "test")
          .add("b", StringType)
          .add("nested", (new StructType)
            .add("col1", StringType)
            .add("col2", IntegerType)
          ),
        partitionColumnNames = Seq("nested")
      )
    )
    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING)",
      "no viable alternative at input")
  }

  test("describe query") {
    val query = "SELECT * FROM t"
    assertEqual("DESCRIBE QUERY " + query, DescribeQueryCommand(query, parser.parsePlan(query)))
    assertEqual("DESCRIBE " + query, DescribeQueryCommand(query, parser.parsePlan(query)))
  }

  test("query organization") {
    // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
    val baseSql = "select * from t"
    val basePlan =
      Project(Seq(UnresolvedStar(None)), UnresolvedRelation(TableIdentifier("t")))

    assertEqual(s"$baseSql distribute by a, b",
      RepartitionByExpression(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil,
        basePlan,
        numPartitions = newConf.numShufflePartitions))
    assertEqual(s"$baseSql distribute by a sort by b",
      Sort(SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
        global = false,
        RepartitionByExpression(UnresolvedAttribute("a") :: Nil,
          basePlan,
          numPartitions = newConf.numShufflePartitions)))
    assertEqual(s"$baseSql cluster by a, b",
      Sort(SortOrder(UnresolvedAttribute("a"), Ascending) ::
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
        global = false,
        RepartitionByExpression(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil,
          basePlan,
          numPartitions = newConf.numShufflePartitions)))
  }

  test("pipeline concatenation") {
    val concat = Concat(
      Concat(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil) ::
      UnresolvedAttribute("c") ::
      Nil
    )
    assertEqual(
      "SELECT a || b || c FROM t",
      Project(UnresolvedAlias(concat) :: Nil, UnresolvedRelation(TableIdentifier("t"))))
  }

  test("database and schema tokens are interchangeable") {
    assertEqual("CREATE DATABASE foo", parser.parsePlan("CREATE SCHEMA foo"))
    assertEqual("DROP DATABASE foo", parser.parsePlan("DROP SCHEMA foo"))
    assertEqual("ALTER DATABASE foo SET DBPROPERTIES ('x' = 'y')",
      parser.parsePlan("ALTER SCHEMA foo SET DBPROPERTIES ('x' = 'y')"))
    assertEqual("DESC DATABASE foo", parser.parsePlan("DESC SCHEMA foo"))
  }

  test("manage resources") {
    assertEqual("ADD FILE abc.txt", AddFileCommand("abc.txt"))
    assertEqual("ADD FILE 'abc.txt'", AddFileCommand("abc.txt"))
    assertEqual("ADD FILE \"/path/to/abc.txt\"", AddFileCommand("/path/to/abc.txt"))
    assertEqual("LIST FILE abc.txt", ListFilesCommand(Array("abc.txt")))
    assertEqual("LIST FILE '/path//abc.txt'", ListFilesCommand(Array("/path//abc.txt")))
    assertEqual("LIST FILE \"/path2/abc.txt\"", ListFilesCommand(Array("/path2/abc.txt")))
    assertEqual("ADD JAR /path2/_2/abc.jar", AddJarCommand("/path2/_2/abc.jar"))
    assertEqual("ADD JAR '/test/path_2/jar/abc.jar'", AddJarCommand("/test/path_2/jar/abc.jar"))
    assertEqual("ADD JAR \"abc.jar\"", AddJarCommand("abc.jar"))
    assertEqual("LIST JAR /path-with-dash/abc.jar",
      ListJarsCommand(Array("/path-with-dash/abc.jar")))
    assertEqual("LIST JAR 'abc.jar'", ListJarsCommand(Array("abc.jar")))
    assertEqual("LIST JAR \"abc.jar\"", ListJarsCommand(Array("abc.jar")))
    assertEqual("ADD FILE /path with space/abc.txt", AddFileCommand("/path with space/abc.txt"))
    assertEqual("ADD JAR /path with space/abc.jar", AddJarCommand("/path with space/abc.jar"))
  }
}
