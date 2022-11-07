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

import com.intel.oap.spark.sql.execution.datasources.arrow.ArrowFileFormat

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
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
      // Write hive path
      case s@ InsertIntoStatement(
      l@ LogicalRelation(r@HadoopFsRelation(_, _, _, _, _: ParquetFileFormat, _)
      , _, _, _), _, _, _, _, _) =>
        InsertIntoStatement(
          LogicalRelation(
            HadoopFsRelation(r.location, r.partitionSchema, r.dataSchema, r.bucketSpec,
              new ArrowFileFormat, r.options)(r.sparkSession),
            l.output, l.catalogTable, l.isStreaming),
          s.partitionSpec, s.userSpecifiedCols, s.query, s.overwrite, s.ifPartitionNotExists)

      // Write datasource path
      case s@ InsertIntoHadoopFsRelationCommand(
      _, _, _, _, _, _: ParquetFileFormat, _, _, _, _, _, _) =>
        InsertIntoHadoopFsRelationCommand(
          s.outputPath, s.staticPartitions, s.ifPartitionNotExists, s.partitionColumns,
          s.bucketSpec, new ArrowFileFormat, s.options, s.query, s.mode, s.catalogTable,
          s.fileIndex, s.outputColumnNames)

      // Read path
      case l@ LogicalRelation(
      r@ HadoopFsRelation(_, _, _, _, _: ParquetFileFormat, _), _, _, _) =>
        LogicalRelation(
          HadoopFsRelation(r.location, r.partitionSchema, r.dataSchema, r.bucketSpec,
            new ArrowFileFormat, r.options)(r.sparkSession),
          l.output, l.catalogTable, l.isStreaming)

      // INSERT HIVE DIR
      case c@ InsertIntoDataSourceDirCommand(_, provider, _, _) if provider == "parquet" =>
        InsertIntoDataSourceDirCommand(c.storage, "arrow", c.query, c.overwrite)
    }
  }
}
