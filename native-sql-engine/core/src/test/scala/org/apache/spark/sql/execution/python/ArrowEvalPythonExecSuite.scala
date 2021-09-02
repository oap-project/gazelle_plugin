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

package org.apache.spark.sql.execution.python

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.execution.{FilterExec, InputAdapter, SparkPlanTest, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.{IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, StringType, StructType, StructField, IntegerType}

case class CodegenFallbackExpr(child: Expression) extends Expression with CodegenFallback {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override lazy val resolved = true
  override def eval(input: InternalRow): Any = child.eval(input)
}

case class TestFallbackScalarPandasUDF(name: String) {
  lazy val udf = new UserDefinedPythonFunction(
    name = name,
    func = PythonFunction(
      command = IntegratedUDFTestUtils.pandasFunc,
      envVars = IntegratedUDFTestUtils.workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = IntegratedUDFTestUtils.pythonExec,
      pythonVer = IntegratedUDFTestUtils.pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = StringType,
    pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    udfDeterministic = true) {

    override def builder(e: Seq[Expression]): Expression = {
      assert(e.length == 1, "Defined UDF only has one column")
      val expr = e.head
      assert(expr.resolved, "column should be resolved to use the same type " +
        "as input. Try df(name) or df.col(name)")
      CodegenFallbackExpr(Cast(super.builder(Cast(expr, StringType) :: Nil), expr.dataType))
    }
  }

  def apply(exprs: Column*): Column = udf(exprs: _*)

  val prettyName: String = "Scalar Pandas UDF"
}


class ArrowEvalPythonExecSuite extends QueryTest with SharedSparkSession {

  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder
  import IntegratedUDFTestUtils._

  

  val pyarrowTestUDF = TestScalarPandasUDF(name = "pyarrowUDF")
  val pyarrowFallbackTestUDF = TestFallbackScalarPandasUDF(name = "pyarrowFallbackUDF")

  lazy val base = Seq(
    (1, 1), (1, 2), (2, 1), (2, 2),
    (3, 1), (3, 2), (0, 1), (3, 0)).toDF("a", "b")

  test("columnar arrow_udf test") {
    lazy val expected = Seq(
      (0, 1, 0), (1, 1, 1), (1, 2, 1), (2, 1, 2), (2, 2, 2), (3, 0, 3), (3, 1, 3), (3, 2, 3)
      ).toDF("a", "b", "p_a")

    val df2 = base.withColumn("p_a", pyarrowTestUDF(base("a")))
    checkAnswer(df2, expected)
  }

  test("fallback arrow_udf test") {
    lazy val expected = Seq(
      (0, 1, 0), (1, 1, 1), (1, 2, 1), (2, 1, 2), (2, 2, 2), (3, 0, 3), (3, 1, 3), (3, 2, 3)
      ).toDF("a", "b", "p_a")

    val df2 = base.withColumn("p_a", pyarrowFallbackTestUDF(base("a")))
    checkAnswer(df2, expected)
  }
}