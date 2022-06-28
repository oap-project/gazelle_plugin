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

import org.apache.spark.sql.catalyst.expressions._


class ColumnarMurmur3Hash(children: Seq[Expression])
  extends Murmur3Hash(children: Seq[Expression]) with ColumnarExpression {

  def buildCheck(): Unit = {
    for (expr <- children) {
      if (expr.)
    }
  }
}


object ColumnarHashExpression {

  def create(children: Seq[Expression], original: Expression): Expression = {
    original match {
      case _: Murmur3Hash =>
        new ColumnarMurmur3Hash(children)
    }
  }
}
