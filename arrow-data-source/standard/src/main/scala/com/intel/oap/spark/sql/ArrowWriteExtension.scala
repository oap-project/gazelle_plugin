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

import com.intel.oap.spark.sql.ArrowWriteExtension.ArrowWritePostRule
import com.intel.oap.spark.sql.ArrowWriteExtension.DummyRule
import com.intel.oap.spark.sql.ArrowWriteExtension.SimpleColumnarRule
import com.intel.oap.spark.sql.ArrowWriteExtension.SimpleStrategy
import com.intel.oap.spark.sql.execution.datasources.arrow.ArrowFileFormat
import com.intel.oap.sql.execution.RowToArrowColumnarExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OrderPreservingUnaryNode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.execution.ColumnarToRowTransition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class ArrowWriteExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(session => SimpleColumnarRule(DummyRule, ArrowWritePostRule(session)))
    e.injectPlannerStrategy(session => SimpleStrategy())
  }
}

object ArrowWriteExtension {
  private object DummyRule extends Rule[SparkPlan] {
    def apply(p: SparkPlan): SparkPlan = p
  }

  private case class SimpleColumnarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan])
      extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = pre
    override def postColumnarTransitions: Rule[SparkPlan] = post
  }

  case class ArrowWritePostRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan match {
      case rc @ DataWritingCommandExec(cmd, ColumnarToRowExec(child)) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            // TODO: support writing parquet fileformat
            if (command.fileFormat.isInstanceOf[ArrowFileFormat] &&
                !command.fileFormat.isInstanceOf[ParquetFileFormat]) {
              rc.withNewChildren(Array(ColumnarToFakeRowAdaptor(child)))
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case rc @ DataWritingCommandExec(cmd, child) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            // TODO: support writing parquet fileformat
            if (command.fileFormat.isInstanceOf[ArrowFileFormat] &&
              !command.fileFormat.isInstanceOf[ParquetFileFormat]) {
              child match {
                case c: AdaptiveSparkPlanExec =>
                  rc.withNewChildren(
                    Array(
                      AdaptiveSparkPlanExec(
                        ColumnarToFakeRowAdaptor(c.inputPlan),
                        c.context,
                        c.preprocessingRules,
                        c.isSubquery)))
                case other =>
                  rc.withNewChildren(
                    Array(ColumnarToFakeRowAdaptor(RowToArrowColumnarExec(child))))
              }
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }
  }

  private case class ColumnarToFakeRowLogicAdaptor(child: LogicalPlan)
      extends OrderPreservingUnaryNode {
    override def output: Seq[Attribute] = child.output

    // For spark 3.2.
    protected def withNewChildInternal(newChild: LogicalPlan): ColumnarToFakeRowLogicAdaptor =
      copy(child = newChild)
  }

  private case class ColumnarToFakeRowAdaptor(child: SparkPlan) extends ColumnarToRowTransition {
    if (!child.logicalLink.isEmpty) {
      setLogicalLink(ColumnarToFakeRowLogicAdaptor(child.logicalLink.get))
    }

    override protected def doExecute(): RDD[InternalRow] = {
      child.executeColumnar().map { cb => new FakeRow(cb) }
    }

    override def output: Seq[Attribute] = child.output

    // For spark 3.2.
    protected def withNewChildInternal(newChild: SparkPlan): ColumnarToFakeRowAdaptor =
      copy(child = newChild)
  }

  case class SimpleStrategy() extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ColumnarToFakeRowLogicAdaptor(child: LogicalPlan) =>
        Seq(ColumnarToFakeRowAdaptor(planLater(child)))
      case other =>
        Nil
    }
  }

}
