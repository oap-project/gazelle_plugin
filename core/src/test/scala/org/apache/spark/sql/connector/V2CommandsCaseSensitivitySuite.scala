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

package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, TestRelation2}
import org.apache.spark.sql.catalyst.analysis.CreateTablePartitioningValidationSuite
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.datasources.PreprocessTableCreation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}

class V2CommandsCaseSensitivitySuite extends SharedSparkSession with AnalysisTest {
  import CreateTablePartitioningValidationSuite._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

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

  override protected def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = {
    Seq(PreprocessTableCreation(spark))
  }

  test("CreateTableAsSelect: using top level field for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("ID", "iD").foreach { ref =>
          val plan = CreateTableAsSelect(
            catalog,
            Identifier.of(Array(), "table_name"),
            Expressions.identity(ref) :: Nil,
            TestRelation2,
            Map.empty,
            Map.empty,
            ignoreIfExists = false)

          if (caseSensitive) {
            assertAnalysisError(plan, Seq("Couldn't find column", ref), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("CreateTableAsSelect: using nested column for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("POINT.X", "point.X", "poInt.x", "poInt.X").foreach { ref =>
          val plan = CreateTableAsSelect(
            catalog,
            Identifier.of(Array(), "table_name"),
            Expressions.bucket(4, ref) :: Nil,
            TestRelation2,
            Map.empty,
            Map.empty,
            ignoreIfExists = false)

          if (caseSensitive) {
            val field = ref.split("\\.")
            assertAnalysisError(plan, Seq("Couldn't find column", field.head), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("ReplaceTableAsSelect: using top level field for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("ID", "iD").foreach { ref =>
          val plan = ReplaceTableAsSelect(
            catalog,
            Identifier.of(Array(), "table_name"),
            Expressions.identity(ref) :: Nil,
            TestRelation2,
            Map.empty,
            Map.empty,
            orCreate = true)

          if (caseSensitive) {
            assertAnalysisError(plan, Seq("Couldn't find column", ref), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("ReplaceTableAsSelect: using nested column for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("POINT.X", "point.X", "poInt.x", "poInt.X").foreach { ref =>
          val plan = ReplaceTableAsSelect(
            catalog,
            Identifier.of(Array(), "table_name"),
            Expressions.bucket(4, ref) :: Nil,
            TestRelation2,
            Map.empty,
            Map.empty,
            orCreate = true)

          if (caseSensitive) {
            val field = ref.split("\\.")
            assertAnalysisError(plan, Seq("Couldn't find column", field.head), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("AlterTable: add column - nested") {
    Seq("POINT.Z", "poInt.z", "poInt.Z").foreach { ref =>
      val field = ref.split("\\.")
      alterTableTest(
        TableChange.addColumn(field, LongType),
        Seq("add", field.head)
      )
    }
  }

  test("AlterTable: add column resolution - positional") {
    Seq("ID", "iD").foreach { ref =>
      alterTableTest(
        TableChange.addColumn(
          Array("f"), LongType, true, null, ColumnPosition.after(ref)),
        Seq("reference column", ref)
      )
    }
  }

  test("AlterTable: add column resolution - column position referencing new column") {
    alterTableTest(
      Seq(
        TableChange.addColumn(
          Array("x"), LongType, true, null, ColumnPosition.after("id")),
        TableChange.addColumn(
          Array("y"), LongType, true, null, ColumnPosition.after("X"))),
      Seq("Couldn't find the reference column for AFTER X at root")
    )
  }

  test("AlterTable: add column resolution - nested positional") {
    Seq("X", "Y").foreach { ref =>
      alterTableTest(
        TableChange.addColumn(
          Array("point", "z"), LongType, true, null, ColumnPosition.after(ref)),
        Seq("reference column", ref)
      )
    }
  }

  test("AlterTable: add column resolution - column position referencing new nested column") {
    alterTableTest(
      Seq(
        TableChange.addColumn(
          Array("point", "z"), LongType, true, null),
        TableChange.addColumn(
          Array("point", "zz"), LongType, true, null, ColumnPosition.after("Z"))),
      Seq("Couldn't find the reference column for AFTER Z at point")
    )
  }

  test("AlterTable: drop column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        TableChange.deleteColumn(ref),
        Seq("Cannot delete missing field", ref.quoted)
      )
    }
  }

  test("AlterTable: rename column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        TableChange.renameColumn(ref, "newName"),
        Seq("Cannot rename missing field", ref.quoted)
      )
    }
  }

  test("AlterTable: drop column nullability resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        TableChange.updateColumnNullability(ref, true),
        Seq("Cannot update missing field", ref.quoted)
      )
    }
  }

  test("AlterTable: change column type resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        TableChange.updateColumnType(ref, StringType),
        Seq("Cannot update missing field", ref.quoted)
      )
    }
  }

  test("AlterTable: change column comment resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        TableChange.updateColumnComment(ref, "Here's a comment for ya"),
        Seq("Cannot update missing field", ref.quoted)
      )
    }
  }

  private def alterTableTest(change: TableChange, error: Seq[String]): Unit = {
    alterTableTest(Seq(change), error)
  }

  private def alterTableTest(changes: Seq[TableChange], error: Seq[String]): Unit = {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val plan = AlterTable(
          catalog,
          Identifier.of(Array(), "table_name"),
          TestRelation2,
          changes
        )

        if (caseSensitive) {
          assertAnalysisError(plan, error, caseSensitive)
        } else {
          assertAnalysisSuccess(plan, caseSensitive)
        }
      }
    }
  }
}
