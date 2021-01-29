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

package com.intel.oap

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

case class ColumnarNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class ColumnarPluginConfig(conf: SQLConf) {
  val enableColumnarSort: Boolean =
    conf.getConfString("spark.sql.columnar.sort", "false").toBoolean
  val enableColumnarCodegenSort: Boolean =
    conf.getConfString("spark.sql.columnar.codegen.sort", "true").toBoolean
  val enableColumnarNaNCheck: Boolean =
    conf.getConfString("spark.sql.columnar.nanCheck", "false").toBoolean
  val enableColumnarBroadcastJoin: Boolean =
    conf.getConfString("spark.sql.columnar.sort.broadcastJoin", "true").toBoolean
  val enableColumnarWindow: Boolean =
    conf.getConfString("spark.sql.columnar.window", "true").toBoolean
  val enableColumnarSortMergeJoin: Boolean =
    conf.getConfString("spark.oap.sql.columnar.sortmergejoin", "false").toBoolean
  val enablePreferColumnar: Boolean =
    conf.getConfString("spark.oap.sql.columnar.preferColumnar", "false").toBoolean
  val enableJoinOptimizationReplace: Boolean =
    conf.getConfString("spark.oap.sql.columnar.joinOptimizationReplace", "false").toBoolean
  val joinOptimizationThrottle: Integer =
    conf.getConfString("spark.oap.sql.columnar.joinOptimizationLevel", "6").toInt
  val enableColumnarWholeStageCodegen: Boolean =
    conf.getConfString("spark.oap.sql.columnar.wholestagecodegen", "true").toBoolean
  val enableColumnarShuffle: Boolean = conf
    .getConfString("spark.shuffle.manager", "sort")
    .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  val batchSize: Int =
    conf.getConfString("spark.sql.execution.arrow.maxRecordsPerBatch", "10000").toInt
  val enableMetricsTime: Boolean =
    conf.getConfString(
      "spark.oap.sql.columnar.wholestagecodegen.breakdownTime",
      "false").toBoolean
  val tmpFile: String =
    conf.getConfString("spark.sql.columnar.tmp_dir", null)
  @deprecated val broadcastCacheTimeout: Int =
    conf.getConfString("spark.sql.columnar.sort.broadcast.cache.timeout", "-1").toInt
  val hashCompare: Boolean =
    conf.getConfString("spark.oap.sql.columnar.hashCompare", "false").toBoolean
  // Whether to spill the partition buffers when buffers are full.
  // If false, the partition buffers will be cached in memory first,
  // and the cached buffers will be spilled when reach maximum memory.
  val columnarShufflePreferSpill: Boolean =
    conf.getConfString("spark.oap.sql.columnar.shuffle.preferSpill", "true").toBoolean
  val columnarShuffleUseCustomizedCompressionCodec: String =
    conf.getConfString("spark.oap.sql.columnar.shuffle.customizedCompression.codec", "lz4")
  val isTesting: Boolean =
    conf.getConfString("spark.oap.sql.columnar.testing", "false").toBoolean
  val numaBindingInfo: ColumnarNumaBindingInfo = {
    val enableNumaBinding: Boolean =
      conf.getConfString("spark.oap.sql.columnar.numaBinding", "false").toBoolean
    if (!enableNumaBinding) {
      ColumnarNumaBindingInfo(false)
    } else {
      val tmp = conf.getConfString("spark.oap.sql.columnar.coreRange", null)
      if (tmp == null) {
        ColumnarNumaBindingInfo(false)
      } else {
        val numCores = conf.getConfString("spark.executor.cores", "1").toInt
        val coreRangeList: Array[String] = tmp.split('|').map(_.trim)
        ColumnarNumaBindingInfo(true, coreRangeList, numCores)
      }

    }
  }
}

object ColumnarPluginConfig {
  var ins: ColumnarPluginConfig = null
  var random_temp_dir_path: String = null

  /**
   * @deprecated We should avoid caching this value in entire JVM. us
   */
  @Deprecated
  def getConf: ColumnarPluginConfig = synchronized {
    if (ins == null) {
      ins = getSessionConf
    }
    ins
  }

  def getSessionConf: ColumnarPluginConfig = {
    new ColumnarPluginConfig(SQLConf.get)
  }

  def getBatchSize: Int = synchronized {
    if (ins == null) {
      10000
    } else {
      ins.batchSize
    }
  }
  def getEnableMetricsTime: Boolean = synchronized {
    if (ins == null) {
      false
    } else {
      ins.enableMetricsTime
    }
  }
  def getTempFile: String = synchronized {
    if (ins != null && ins.tmpFile != null) {
      ins.tmpFile
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }
  def setRandomTempDir(path: String) = synchronized {
    random_temp_dir_path = path
  }
  def getRandomTempDir = synchronized {
    random_temp_dir_path
  }
}
