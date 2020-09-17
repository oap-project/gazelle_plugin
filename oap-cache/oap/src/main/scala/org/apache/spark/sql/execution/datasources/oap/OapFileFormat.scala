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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.index.{IndexContext, IndexScanners, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

abstract class OapFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  // exposed for test
  private[oap] lazy val oapMetrics = OapRuntime.getOrCreate.oapMetricsManager

  private var initialized = false
  @transient protected var options: Map[String, String] = _
  @transient protected var sparkSession: SparkSession = _
  @transient protected var files: Seq[FileStatus] = _

  def init(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): FileFormat = {
    this.sparkSession = sparkSession
    this.options = options
    this.files = files

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    // TODO
    // 1. Make the scanning etc. as lazy loading, as inferSchema probably not be called
    // 2. We need to pass down the oap meta file and its associated partition path

    val parents = files.map(file => file.getPath.getParent)

    // TODO we support partitions, but this only read meta from one of the partitions
    val partition2Meta = parents.distinct.reverse.map { parent =>
      new Path(parent, OapFileFormat.OAP_META_FILE)
    }.find(metaPath => metaPath.getFileSystem(hadoopConf).exists(metaPath))

    meta = partition2Meta.map {
      DataSourceMeta.initialize(_, hadoopConf)
    }

    // OapFileFormat.serializeDataSourceMeta(hadoopConf, meta)
    inferSchema = meta.map(_.schema)
    initialized = true

    this
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    if (!initialized) {
      init(sparkSession, options, files)
    }
    inferSchema
  }

  // TODO inferSchema could be lazy computed
  var inferSchema: Option[StructType] = _
  var meta: Option[DataSourceMeta] = _
  // map of columns->IndexType
  protected var hitIndexColumns: Map[String, IndexType] = _

  def initMetrics(metrics: Map[String, SQLMetric]): Unit = oapMetrics.initMetrics(metrics)

  def getHitIndexColumns: Map[String, IndexType] = {
    if (this.hitIndexColumns == null) {
      logWarning("Trigger buildReaderWithPartitionValues before getHitIndexColumns")
      Map.empty
    } else {
      this.hitIndexColumns
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  /**
   * Check if index satisfies strategies' requirements.
   *
   * @param expressions: Index expressions.
   * @param requiredTypes: Required index metrics by optimization strategies.
   * @return
   */
  def hasAvailableIndex(
      expressions: Seq[Expression],
      requiredTypes: Seq[IndexType] = Nil): Boolean = {
    if (expressions.nonEmpty && sparkSession.conf.get(OapConf.OAP_ENABLE_OINDEX)) {
      meta match {
        case Some(m) if requiredTypes.isEmpty =>
          expressions.exists(m.isSupportedByIndex(_, None))
        case Some(m) if requiredTypes.length == expressions.length =>
          expressions.zip(requiredTypes).exists{ x =>
            val expression = x._1
            val requirement = Some(x._2)
            m.isSupportedByIndex(expression, requirement)
          }
        case _ => false
      }
    } else {
      false
    }
  }

  protected def indexScanners(m: DataSourceMeta, filters: Seq[Filter]): Option[IndexScanners] = {

    // Check whether this filter conforms to certain patterns that could benefit from index
    def canTriggerIndex(filter: Filter): Boolean = {
      var attr: String = null
      def checkAttribute(filter: Filter): Boolean = filter match {
        case Or(left, right) =>
          checkAttribute(left) && checkAttribute(right)
        case And(left, right) =>
          checkAttribute(left) && checkAttribute(right)
        case EqualTo(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case LessThan(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case LessThanOrEqual(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case GreaterThan(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case GreaterThanOrEqual(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case In(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case IsNull(attribute) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case IsNotNull(attribute) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case StringStartsWith(attribute, _) =>
          if (attr ==  null || attr == attribute) {attr = attribute; true} else false
        case _ => false
      }

      checkAttribute(filter)
    }

    val ic = new IndexContext(m)

    if (m.indexMetas.nonEmpty) { // check and use index
      logDebug("Supported Filters by Oap:")
      // filter out the "filters" on which we can use index
      val supportFilters = filters.toArray.filter(canTriggerIndex)
      // After filtered, supportFilter only contains:
      // 1. Or predicate that contains only one attribute internally;
      // 2. Some atomic predicates, such as LessThan, EqualTo, etc.
      if (supportFilters.nonEmpty) {
        // determine whether we can use index
        supportFilters.foreach(filter => logDebug("\t" + filter.toString))
        // get index options such as limit, order, etc.
        val indexOptions = options.filterKeys(OapFileFormat.oapOptimizationKeySeq.contains(_))
        val maxChooseSize = sparkSession.conf.get(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE)
        val indexDisableList = sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST)
        ScannerBuilder.build(supportFilters, ic, indexOptions, maxChooseSize, indexDisableList)
      }
    }
    ic.getScanners
  }
}

private[oap] object INDEX_STAT extends Enumeration {
  type INDEX_STAT = Value
  val MISS_INDEX, HIT_INDEX, IGNORE_INDEX = Value
}

private[sql] object OapFileFormat {
  val OAP_INDEX_EXTENSION = ".index"
  val OAP_META_FILE = ".oap.meta"

  val PARQUET_DATA_FILE_CLASSNAME = classOf[ParquetDataFile].getCanonicalName
  val ORC_DATA_FILE_CLASSNAME = classOf[OrcDataFile].getCanonicalName

  val COMPRESSION = "oap.compression"
  val DEFAULT_COMPRESSION = OapConf.OAP_COMPRESSION.defaultValueString
  val ROW_GROUP_SIZE = "oap.rowgroup.size"
  val DEFAULT_ROW_GROUP_SIZE = OapConf.OAP_ROW_GROUP_SIZE.defaultValueString

  /**
   * Oap Optimization Options.
   */
  val OAP_QUERY_ORDER_OPTION_KEY = "oap.scan.file.order"
  val OAP_QUERY_LIMIT_OPTION_KEY = "oap.scan.file.limit"
  val OAP_INDEX_SCAN_NUM_OPTION_KEY = "oap.scan.index.limit"
  val OAP_INDEX_GROUP_BY_OPTION_KEY = "oap.scan.index.group"

  val oapOptimizationKeySeq : Seq[String] = {
    OAP_QUERY_ORDER_OPTION_KEY ::
    OAP_QUERY_LIMIT_OPTION_KEY ::
    OAP_INDEX_SCAN_NUM_OPTION_KEY ::
    OAP_INDEX_GROUP_BY_OPTION_KEY :: Nil
  }
}
