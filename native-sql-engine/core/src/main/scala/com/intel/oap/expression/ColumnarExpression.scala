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

package com.intel.oap.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import scala.collection.mutable.ListBuffer

trait ColumnarExpression {

  val codegenFuncList: List[String] = List(
    "less_than",
    "less_than_with_nan",
    "greater_than",
    "greater_than_with_nan",
    "less_than_or_equal_to",
    "less_than_or_equal_to_with_nan",
    "greater_than_or_equal_to",
    "greater_than_or_equal_to_with_nan",
    "equal",
    "equal_with_nan",
    "not",
    "isnotnull",
    "isnull",
    "starts_with",
    "like",
    "get_json_object",
    "translate",
    "substr",
    "instr",
    "btrim",
    "ltrim",
    "rtrim",
    "upper",
    "lower",
    "castDATE",
    "castDECIMAL",
    "castDECIMALNullOnOverflow",
    "castINTOrNull",
    "castBIGINTOrNull",
    "castFLOAT4OrNull",
    "castFLOAT8OrNull",
    "rescaleDECIMAL",
    "extractYear",
    "round",
    "abs",
    "add",
    "subtract",
    "multiply",
    "divide",
    "shift_left",
    "shift_right",
    "bitwise_and",
    "bitwise_or",
    "normalize",
    "convertTimestampUnit",
    "micros_to_timestamp"
  )

  def supportColumnarCodegen(args: java.lang.Object): (Boolean) = {
    false
  }

  def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    throw new UnsupportedOperationException(s"Not support doColumnarCodeGen.")
  }
}
