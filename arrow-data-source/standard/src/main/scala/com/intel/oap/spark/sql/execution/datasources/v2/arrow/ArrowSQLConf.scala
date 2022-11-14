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

package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import org.apache.spark.sql.internal.SQLConf

object ArrowSQLConf {
  val ARROW_FILTER_PUSHDOWN_ENABLED = SQLConf.buildConf("spark.oap.sql.arrow.filterPushdown")
    .doc("Enables Arrow filter push-down optimization when set to true.")
    .booleanConf
    .createWithDefault(true)

  val FILES_DYNAMIC_MERGE_ENABLED = SQLConf.buildConf("spark.oap.sql.files.dynamicMergeEnabled")
    .doc("Whether to merge file partition dynamically. If true, It will use the total size, " +
      "file count and expectPartitionNum to dynamic merge filePartition. This is better to set " +
      "true if there are many small files in the read path. This configuration is effective " +
      "only when using file-based sources such as Parquet, JSON and ORC.")
    .booleanConf
    .createWithDefault(false)

  val FILES_EXPECTED_PARTITION_NUM = SQLConf.buildConf("spark.oap.sql.files.expectedPartitionNum")
    .doc("The expected number of File partitions. It will automatically merge file splits to " +
      "provide the best concurrency when the file partitions after split exceed the " +
      "expected num and the size of file partition is less than maxSplitSize. If not set, " +
      "the default value is the number of concurrent tasks configured by user. " +
      "This configuration is effective only when using file-based sources such as " +
      "Parquet, JSON and ORC.")
    .intConf
    .checkValue(v => v > 0, "The expected partition number must be a positive integer.")
    .createOptional

  val FILES_MAX_NUM_IN_PARTITION = SQLConf.buildConf("spark.oap.sql.files.maxNumInPartition")
    .doc("The max number of files in one filePartition. If set, it will limit the max file num " +
      "in FilePartition while merging files. This can avoid too many little io in one task. " +
      "This configuration is effective only when using file-based sources such as Parquet, " +
      "JSON and ORC.")
    .intConf
    .checkValue(v => v > 0, "The max file number in partition must be a positive integer.")
    .createOptional

  implicit def fromSQLConf(c: SQLConf): ArrowSQLConf = {
    new ArrowSQLConf(c)
  }
}

class ArrowSQLConf(c: SQLConf) {
  def arrowFilterPushDown: Boolean = c.getConf(ArrowSQLConf.ARROW_FILTER_PUSHDOWN_ENABLED)

  def filesDynamicMergeEnabled: Boolean = c.getConf(ArrowSQLConf.FILES_DYNAMIC_MERGE_ENABLED)

  def filesExpectedPartitionNum: Option[Int] = c.getConf(ArrowSQLConf.FILES_EXPECTED_PARTITION_NUM)

  def filesMaxNumInPartition: Option[Int] = c.getConf(ArrowSQLConf.FILES_MAX_NUM_IN_PARTITION)
}
