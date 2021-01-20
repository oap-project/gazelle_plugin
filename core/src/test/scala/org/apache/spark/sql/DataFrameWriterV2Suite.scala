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

import java.sql.Timestamp

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.connector.{InMemoryTable, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{BucketTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, YearsTransform}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class DataFrameWriterV2Suite extends QueryTest with SharedSparkSession with BeforeAndAfter {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.functions._
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
      .set("spark.oap.sql.columnar.testing", "true")

  private def catalog(name: String): TableCatalog = {
    spark.sessionState.catalogManager.catalog(name).asTableCatalog
  }

  private val defaultOwnership = Map(TableCatalog.PROP_OWNER -> Utils.getCurrentUserName())

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  test("DataFrameWriteV2 encode identifiers correctly") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed

      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)

    spark.table("source").writeTo("testcat.table_name").append()
    sparkContext.listenerBus.waitUntilEmpty()
    assert(plan.isInstanceOf[AppendData])
    checkV2Identifiers(plan.asInstanceOf[AppendData].table)

    spark.table("source").writeTo("testcat.table_name").overwrite(lit(true))
    sparkContext.listenerBus.waitUntilEmpty()
    assert(plan.isInstanceOf[OverwriteByExpression])
    checkV2Identifiers(plan.asInstanceOf[OverwriteByExpression].table)

    spark.table("source").writeTo("testcat.table_name").overwritePartitions()
    sparkContext.listenerBus.waitUntilEmpty()
    assert(plan.isInstanceOf[OverwritePartitionsDynamic])
    checkV2Identifiers(plan.asInstanceOf[OverwritePartitionsDynamic].table)
  }

  private def checkV2Identifiers(
      plan: LogicalPlan,
      identifier: String = "table_name",
      catalogPlugin: TableCatalog = catalog("testcat")): Unit = {
    assert(plan.isInstanceOf[DataSourceV2Relation])
    val v2 = plan.asInstanceOf[DataSourceV2Relation]
    assert(v2.identifier.exists(_.name() == identifier))
    assert(v2.catalog.exists(_ == catalogPlugin))
  }

  test("Append: basic append") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    spark.table("source").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Append: by name not position") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d").writeTo("testcat.table_name").append()
    }

    assert(exc.getMessage.contains("Cannot find data for output column"))
    assert(exc.getMessage.contains("'data'"))

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq())
  }

  test("Append: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("testcat.table_name").append()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Overwrite: overwrite by expression: true") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    spark.table("source").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("testcat.table_name").overwrite(lit(true))

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Overwrite: overwrite by expression: id = 3") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    spark.table("source").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("testcat.table_name").overwrite($"id" === 3)

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Overwrite: by name not position") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
          .writeTo("testcat.table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("Cannot find data for output column"))
    assert(exc.getMessage.contains("'data'"))

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq())
  }

  test("Overwrite: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("testcat.table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("OverwritePartitions: overwrite conflicting partitions") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    spark.table("source").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").withColumn("id", $"id" - 2)
        .writeTo("testcat.table_name").overwritePartitions()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "d"), Row(3L, "e"), Row(4L, "f")))
  }

  test("OverwritePartitions: overwrite all rows if not partitioned") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    spark.table("source").writeTo("testcat.table_name").append()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("testcat.table_name").overwritePartitions()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("OverwritePartitions: by name not position") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
          .writeTo("testcat.table_name").overwritePartitions()
    }

    assert(exc.getMessage.contains("Cannot find data for output column"))
    assert(exc.getMessage.contains("'data'"))

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq())
  }

  test("OverwritePartitions: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("testcat.table_name").overwritePartitions()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Create: basic behavior") {
    spark.table("source").writeTo("testcat.table_name").create()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties == defaultOwnership.asJava)
  }

  test("Create: with using") {
    spark.table("source").writeTo("testcat.table_name").using("foo").create()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties === (Map("provider" -> "foo") ++ defaultOwnership).asJava)
  }

  test("Create: with property") {
    spark.table("source").writeTo("testcat.table_name").tableProperty("prop", "value").create()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties === (Map("prop" -> "value") ++ defaultOwnership).asJava)
  }

  test("Create: identity partitioned table") {
    spark.table("source").writeTo("testcat.table_name").partitionedBy($"id").create()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties == defaultOwnership.asJava)
  }

  test("Create: partitioned by years(ts)") {
    spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("testcat.table_name")
        .tableProperty("allow-unsupported-transforms", "true")
        .partitionedBy(years($"ts"))
        .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(YearsTransform(FieldReference("ts"))))
  }

  test("Create: partitioned by months(ts)") {
    spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("testcat.table_name")
        .tableProperty("allow-unsupported-transforms", "true")
        .partitionedBy(months($"ts"))
        .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(MonthsTransform(FieldReference("ts"))))
  }

  test("Create: partitioned by days(ts)") {
    spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("testcat.table_name")
        .tableProperty("allow-unsupported-transforms", "true")
        .partitionedBy(days($"ts"))
        .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(DaysTransform(FieldReference("ts"))))
  }

  test("Create: partitioned by hours(ts)") {
    spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("testcat.table_name")
        .tableProperty("allow-unsupported-transforms", "true")
        .partitionedBy(hours($"ts"))
        .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(HoursTransform(FieldReference("ts"))))
  }

  test("Create: partitioned by bucket(4, id)") {
    spark.table("source")
        .writeTo("testcat.table_name")
        .tableProperty("allow-unsupported-transforms", "true")
        .partitionedBy(bucket(4, $"id"))
        .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning ===
        Seq(BucketTransform(LiteralValue(4, IntegerType), Seq(FieldReference("id")))))
  }

  test("Create: fail if table already exists") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")

    val exc = intercept[TableAlreadyExistsException] {
      spark.table("source").writeTo("testcat.table_name").create()
    }

    assert(exc.getMessage.contains("table_name"))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // table should not have been changed
    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties === (Map("provider" -> "foo") ++ defaultOwnership).asJava)
  }

  test("Replace: basic behavior") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE testcat.table_name SELECT * FROM source")

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties === (Map("provider" -> "foo") ++ defaultOwnership).asJava)

    spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("testcat.table_name").replace()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "testcat.table_name")
    assert(replaced.schema === new StructType()
        .add("id", LongType)
        .add("data", StringType)
        .add("even_or_odd", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(replaced.properties === defaultOwnership.asJava)
  }

  test("Replace: partitioned table") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")
    spark.sql("INSERT INTO TABLE testcat.table_name SELECT * FROM source")

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties === (Map("provider" -> "foo") ++ defaultOwnership).asJava)

    spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("testcat.table_name").partitionedBy($"id").replace()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "testcat.table_name")
    assert(replaced.schema === new StructType()
        .add("id", LongType)
        .add("data", StringType)
        .add("even_or_odd", StringType))
    assert(replaced.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(replaced.properties === defaultOwnership.asJava)
  }

  test("Replace: fail if table does not exist") {
    val exc = intercept[CannotReplaceMissingTableException] {
      spark.table("source").writeTo("testcat.table_name").replace()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("CreateOrReplace: table does not exist") {
    spark.table("source2").writeTo("testcat.table_name").createOrReplace()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))

    val replaced = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "testcat.table_name")
    assert(replaced.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(replaced.properties === defaultOwnership.asJava)
  }

  test("CreateOrReplace: table exists") {
    spark.sql(
      "CREATE TABLE testcat.table_name (id bigint, data string) USING foo PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE testcat.table_name SELECT * FROM source")

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "testcat.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties === (Map("provider" -> "foo") ++ defaultOwnership).asJava)

    spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("testcat.table_name").createOrReplace()

    checkAnswer(
      spark.table("testcat.table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "testcat.table_name")
    assert(replaced.schema === new StructType()
        .add("id", LongType)
        .add("data", StringType)
        .add("even_or_odd", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(replaced.properties === defaultOwnership.asJava)
  }

  test("SPARK-30289 Create: partitioned by nested column") {
    val schema = new StructType().add("ts", new StructType()
      .add("created", TimestampType)
      .add("modified", TimestampType)
      .add("timezone", StringType))

    val data = Seq(
      Row(Row(Timestamp.valueOf("2019-06-01 10:00:00"), Timestamp.valueOf("2019-09-02 07:00:00"),
        "America/Los_Angeles")),
      Row(Row(Timestamp.valueOf("2019-08-26 18:00:00"), Timestamp.valueOf("2019-09-26 18:00:00"),
        "America/Los_Angeles")),
      Row(Row(Timestamp.valueOf("2018-11-23 18:00:00"), Timestamp.valueOf("2018-12-22 18:00:00"),
        "America/New_York")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)

    df.writeTo("testcat.table_name")
      .partitionedBy($"ts.timezone")
      .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))
      .asInstanceOf[InMemoryTable]

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(IdentityTransform(FieldReference(Array("ts", "timezone")))))
    checkAnswer(spark.table(table.name), data)
    assert(table.dataMap.toArray.length == 2)
    assert(table.dataMap(Seq(UTF8String.fromString("America/Los_Angeles"))).rows.size == 2)
    assert(table.dataMap(Seq(UTF8String.fromString("America/New_York"))).rows.size == 1)

    // TODO: `DataSourceV2Strategy` can not translate nested fields into source filter yet
    // so the following sql will fail.
    // sql("DELETE FROM testcat.table_name WHERE ts.timezone = \"America/Los_Angeles\"")
  }

  test("SPARK-30289 Create: partitioned by multiple transforms on nested columns") {
    spark.table("source")
      .withColumn("ts", struct(
        lit("2019-06-01 10:00:00.000000").cast("timestamp") as "created",
        lit("2019-09-02 07:00:00.000000").cast("timestamp") as "modified",
        lit("America/Los_Angeles") as "timezone"))
      .writeTo("testcat.table_name")
      .tableProperty("allow-unsupported-transforms", "true")
      .partitionedBy(
        years($"ts.created"), months($"ts.created"), days($"ts.created"), hours($"ts.created"),
        years($"ts.modified"), months($"ts.modified"), days($"ts.modified"), hours($"ts.modified")
      )
      .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(
      YearsTransform(FieldReference(Array("ts", "created"))),
      MonthsTransform(FieldReference(Array("ts", "created"))),
      DaysTransform(FieldReference(Array("ts", "created"))),
      HoursTransform(FieldReference(Array("ts", "created"))),
      YearsTransform(FieldReference(Array("ts", "modified"))),
      MonthsTransform(FieldReference(Array("ts", "modified"))),
      DaysTransform(FieldReference(Array("ts", "modified"))),
      HoursTransform(FieldReference(Array("ts", "modified")))))
  }

  test("SPARK-30289 Create: partitioned by bucket(4, ts.timezone)") {
    spark.table("source")
      .withColumn("ts", struct(
        lit("2019-06-01 10:00:00.000000").cast("timestamp") as "created",
        lit("2019-09-02 07:00:00.000000").cast("timestamp") as "modified",
        lit("America/Los_Angeles") as "timezone"))
      .writeTo("testcat.table_name")
      .tableProperty("allow-unsupported-transforms", "true")
      .partitionedBy(bucket(4, $"ts.timezone"))
      .create()

    val table = catalog("testcat").loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "testcat.table_name")
    assert(table.partitioning === Seq(BucketTransform(LiteralValue(4, IntegerType),
      Seq(FieldReference(Seq("ts", "timezone"))))))
  }
}
