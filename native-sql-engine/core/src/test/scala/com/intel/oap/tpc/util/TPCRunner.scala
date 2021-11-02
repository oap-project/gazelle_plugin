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

package com.intel.oap.tpc.util

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

class TPCRunner(val spark: SparkSession, val resource: String) {
  val caseIds = TPCRunner.parseCaseIds(TPCRunner.locateResourcePath(resource), ".sql")

  def runTPCQuery(caseId: String, roundId: Int, explain: Boolean = false): Unit = {
    val path = "%s/%s.sql".format(resource, caseId);
    val absolute = TPCRunner.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    println("Running query %s (round %d)... ".format(caseId, roundId))
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = true)
    }
    df.show(100)
  }
}

object TPCRunner {
  
  private def parseCaseIds(dir: String, suffix: String): List[String] = {
    val folder = new File(dir)
    if (!folder.exists()) {
      throw new IllegalArgumentException("dir does not exist: " + dir)
    }
    folder
        .listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = name.endsWith(suffix)
        })
        .map(f => f.getName)
        .map(n => n.substring(0, n.lastIndexOf(suffix)))
        .sortBy(s => {
          // fill with leading zeros
          "%s%s".format(new String((0 until 16 - s.length).map(_ => '0').toArray), s)
        })
        .toList
  }

  private def locateResourcePath(resource: String): String = {
    classOf[TPCRunner].getClassLoader.getResource("")
        .getPath.concat(File.separator).concat(resource)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}
