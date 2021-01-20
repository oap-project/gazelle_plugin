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
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}

class DataSourceV2SQLSessionCatalogSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with AlterTableTests
  with SessionCatalogTest[InMemoryTable, InMemoryTableSessionCatalog] {

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
      .set("spark.oap.sql.columnar.testing", "true")

  override protected val catalogAndNamespace = ""

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
    checkAnswer(sql(s"SELECT * FROM $tableName"), expected)
    checkAnswer(sql(s"SELECT * FROM default.$tableName"), expected)
    checkAnswer(sql(s"TABLE $tableName"), expected)
  }

  override def getTableMetadata(tableName: String): Table = {
    val v2Catalog = spark.sessionState.catalogManager.currentCatalog
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    v2Catalog.asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(nameParts.init.toArray, nameParts.last))
  }

  test("SPARK-30697: catalog.isView doesn't throw an error for specialized identifiers") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")

      def idResolver(id: Identifier): Identifier = Identifier.of(Array("default"), id.name())

      InMemoryTableSessionCatalog.withCustomIdentifierResolver(idResolver) {
        // The following should not throw AnalysisException.
        sql(s"DESCRIBE TABLE ignored.$t1")
      }
    }
  }

  test("SPARK-31624: SHOW TBLPROPERTIES working with V2 tables and the session catalog") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format TBLPROPERTIES " +
        "(key='v', key2='v2')")

      checkAnswer(sql(s"SHOW TBLPROPERTIES $t1"), Seq(Row("key", "v"), Row("key2", "v2")))

      checkAnswer(sql(s"SHOW TBLPROPERTIES $t1('key')"), Row("key", "v"))

      checkAnswer(
        sql(s"SHOW TBLPROPERTIES $t1('keyX')"),
        Row("keyX", s"Table default.$t1 does not have property: keyX"))
    }
  }
}
