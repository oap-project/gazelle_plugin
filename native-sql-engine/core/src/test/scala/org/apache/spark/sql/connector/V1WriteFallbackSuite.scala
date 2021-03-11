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

import java.util

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, LogicalWriteInfoImpl, SupportsOverwrite, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V1WriteFallbackSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import testImplicits._

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

  private val v2Format = classOf[InMemoryV1Provider].getName

  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryV1Provider.clear()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    InMemoryV1Provider.clear()
  }

  test("append fallback") {
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    df.write.mode("append").option("name", "t1").format(v2Format).save()

    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
    assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
    assert(InMemoryV1Provider.tables("t1").partitioning.isEmpty)

    df.write.mode("append").option("name", "t1").format(v2Format).save()
    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df.union(df))
  }

  test("overwrite by truncate fallback") {
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    df.write.mode("append").option("name", "t1").format(v2Format).save()

    val df2 = Seq((10, "k"), (20, "l"), (30, "m")).toDF("a", "b")
    df2.write.mode("overwrite").option("name", "t1").format(v2Format).save()
    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df2)
  }

  SaveMode.values().foreach { mode =>
    test(s"save: new table creations with partitioning for table - mode: $mode") {
      val format = classOf[InMemoryV1Provider].getName
      val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
      df.write.mode(mode).option("name", "t1").format(format).partitionBy("a").save()

      checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
      assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
      assert(InMemoryV1Provider.tables("t1").partitioning.sameElements(
        Array(IdentityTransform(FieldReference(Seq("a"))))))
    }
  }

  test("save: default mode is ErrorIfExists") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // default is ErrorIfExists, and since a table already exists we throw an exception
    val e = intercept[AnalysisException] {
      df.write.option("name", "t1").format(format).partitionBy("a").save()
    }
    assert(e.getMessage.contains("already exists"))
  }

  test("save: Ignore mode") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // no-op
    df.write.option("name", "t1").format(format).mode("ignore").partitionBy("a").save()

    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
  }

  test("save: tables can perform schema and partitioning checks if they already exist") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    val e2 = intercept[IllegalArgumentException] {
      df.write.mode("append").option("name", "t1").format(format).partitionBy("b").save()
    }
    assert(e2.getMessage.contains("partitioning"))

    val e3 = intercept[IllegalArgumentException] {
      Seq((1, "x")).toDF("c", "d").write.mode("append").option("name", "t1").format(format)
        .save()
    }
    assert(e3.getMessage.contains("schema"))
  }
}

class V1WriteFallbackSessionCatalogSuite
  extends InsertIntoTests(supportsDynamicOverwrite = false, includeSQLOnlyTests = true)
  with SessionCatalogTest[InMemoryTableWithV1Fallback, V1FallbackTableCatalog] {

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

  override protected val v2Format = classOf[InMemoryV1Provider].getName
  override protected val catalogClassName: String = classOf[V1FallbackTableCatalog].getName
  override protected val catalogAndNamespace: String = ""

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(InMemoryV1Provider.getTableData(spark, s"default.$tableName"), expected)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }
}

class V1FallbackTableCatalog extends TestV2SessionCatalogBase[InMemoryTableWithV1Fallback] {
  override def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): InMemoryTableWithV1Fallback = {
    val t = new InMemoryTableWithV1Fallback(name, schema, partitions, properties)
    InMemoryV1Provider.tables.put(name, t)
    t
  }
}

private object InMemoryV1Provider {
  val tables: mutable.Map[String, InMemoryTableWithV1Fallback] = mutable.Map.empty

  def getTableData(spark: SparkSession, name: String): DataFrame = {
    val t = tables.getOrElse(name, throw new IllegalArgumentException(s"Table $name doesn't exist"))
    spark.createDataFrame(t.getData.asJava, t.schema)
  }

  def clear(): Unit = {
    tables.clear()
  }
}

class InMemoryV1Provider
  extends SimpleTableProvider
  with DataSourceRegister
  with CreatableRelationProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {

    InMemoryV1Provider.tables.getOrElse(options.get("name"), {
      new InMemoryTableWithV1Fallback(
        "InMemoryTableWithV1Fallback",
        new StructType(),
        Array.empty,
        options.asCaseSensitiveMap()
      )
    })
  }

  override def shortName(): String = "in-memory"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val _sqlContext = sqlContext

    val partitioning = parameters.get(DataSourceUtils.PARTITIONING_COLUMNS_KEY).map { value =>
      DataSourceUtils.decodePartitioningColumns(value).map { partitioningColumn =>
        IdentityTransform(FieldReference(partitioningColumn))
      }
    }.getOrElse(Nil)

    val tableName = parameters("name")
    val tableOpt = InMemoryV1Provider.tables.get(tableName)
    val table = tableOpt.getOrElse(new InMemoryTableWithV1Fallback(
      "InMemoryTableWithV1Fallback",
      data.schema.asNullable,
      partitioning.toArray,
      Map.empty[String, String].asJava
    ))
    if (tableOpt.isEmpty) {
      InMemoryV1Provider.tables.put(tableName, table)
    } else {
      if (data.schema.asNullable != table.schema) {
        throw new IllegalArgumentException("Wrong schema provided")
      }
      if (!partitioning.sameElements(table.partitioning)) {
        throw new IllegalArgumentException("Wrong partitioning provided")
      }
    }

    def getRelation: BaseRelation = new BaseRelation {
      override def sqlContext: SQLContext = _sqlContext
      override def schema: StructType = table.schema
    }

    if (mode == SaveMode.ErrorIfExists && tableOpt.isDefined) {
      throw new AnalysisException("Table already exists")
    } else if (mode == SaveMode.Ignore && tableOpt.isDefined) {
      // do nothing
      return getRelation
    }
    val writer = table.newWriteBuilder(
      LogicalWriteInfoImpl(
        "", StructType(Seq.empty), new CaseInsensitiveStringMap(parameters.asJava)))
    if (mode == SaveMode.Overwrite) {
      writer.asInstanceOf[SupportsTruncate].truncate()
    }
    writer.asInstanceOf[V1WriteBuilder].buildForV1Write().insert(data, overwrite = false)
    getRelation
  }
}

class InMemoryTableWithV1Fallback(
    override val name: String,
    override val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String])
  extends Table
  with SupportsWrite {

  partitioning.foreach { t =>
    if (!t.isInstanceOf[IdentityTransform]) {
      throw new IllegalArgumentException(s"Transform $t must be IdentityTransform")
    }
  }

  override def capabilities: util.Set[TableCapability] = Set(
    TableCapability.V1_BATCH_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.TRUNCATE).asJava

  @volatile private var dataMap: mutable.Map[Seq[Any], Seq[Row]] = mutable.Map.empty
  private val partFieldNames = partitioning.flatMap(_.references).toSeq.flatMap(_.fieldNames)
  private val partIndexes = partFieldNames.map(schema.fieldIndex(_))

  def getData: Seq[Row] = dataMap.values.flatten.toSeq

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new FallbackWriteBuilder(info.options)
  }

  private class FallbackWriteBuilder(options: CaseInsensitiveStringMap)
    extends WriteBuilder
    with V1WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite {

    private var mode = "append"

    override def truncate(): WriteBuilder = {
      dataMap.clear()
      mode = "truncate"
      this
    }

    override def overwrite(filters: Array[Filter]): WriteBuilder = {
      val keys = InMemoryTable.filtersToKeys(dataMap.keys, partFieldNames, filters)
      dataMap --= keys
      mode = "overwrite"
      this
    }

    private def getPartitionValues(row: Row): Seq[Any] = {
      partIndexes.map(row.get)
    }

    override def buildForV1Write(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          assert(!overwrite, "V1 write fallbacks cannot be called with overwrite=true")
          val rows = data.collect()
          rows.groupBy(getPartitionValues).foreach { case (partition, elements) =>
            if (dataMap.contains(partition) && mode == "append") {
              dataMap.put(partition, dataMap(partition) ++ elements)
            } else if (dataMap.contains(partition)) {
              throw new IllegalStateException("Partition was not removed properly")
            } else {
              dataMap.put(partition, elements)
            }
          }
        }
      }
    }
  }
}
