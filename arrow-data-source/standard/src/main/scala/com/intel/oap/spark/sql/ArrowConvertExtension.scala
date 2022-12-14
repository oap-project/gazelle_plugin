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

package com.intel.oap.spark.sql

import java.util.Locale

import com.intel.oap.spark.sql.execution.datasources.arrow.ArrowFileFormat
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class ArrowConvertorExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPostHocResolutionRule(session => ArrowConvertorRule(session))
  }
}

case class ArrowConvertorRule(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // Write datasource path
      // TODO: support writing with partitioned/bucketed/sorted column
      case c: InsertIntoHadoopFsRelationCommand
        if c.fileFormat.isInstanceOf[ParquetFileFormat] &&
          c.partitionColumns.isEmpty && c.bucketSpec.isEmpty =>
        c.copy(fileFormat = new ArrowFileFormat)

      // Read path
      case l@ LogicalRelation(
        r@ HadoopFsRelation(_, _, _, _, _: ParquetFileFormat, _), _, _, _) =>
        l.copy(relation = r.copy(fileFormat = new ArrowFileFormat)(session))

      // INSERT DIR
      case c: InsertIntoDataSourceDirCommand if c.provider == "parquet" =>
        c.copy(provider = "arrow")
    }
  }
}
