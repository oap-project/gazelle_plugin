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

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.atomic.LongAdder

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.IndexBuildResult
import org.apache.spark.sql.oap.adapter.InputFileNameHolderAdapter

case class IndexWriteTaskStats(writeStatus: Seq[IndexBuildResult]) extends WriteTaskStats

class OapIndexWriteTaskStatsTracker extends WriteTaskStatsTracker with Logging {

  private[this] var curInputFileName: String = _

  private[this] var statusMap: Map[String, LongAdder] = Map.empty[String, LongAdder]

  override def newPartition(partitionValues: InternalRow): Unit = {
    // currently unhandled
  }

  override def newBucket(bucketId: Int): Unit = {
    // currently unhandled
  }

  override def newFile(filePath: String): Unit = {
    // currently unhandled
  }

  override def newRow(row: InternalRow): Unit = {
    val inputFileName = InputFileNameHolderAdapter.getInputFileName().toString
    if (curInputFileName != inputFileName) {
      curInputFileName = inputFileName
      statusMap = statusMap + (inputFileName -> new LongAdder)
    }
    statusMap(curInputFileName).increment()
  }

  override def getFinalStats(): WriteTaskStats = {
    val results = statusMap.map {
      case (filePath, rowCount) =>
        val path = new Path(filePath)
        IndexBuildResult(path.getName, rowCount.longValue(), "", path.getParent.toString)
    }
    IndexWriteTaskStats(results.toSeq)
  }
}

class OapIndexWriteJobStatsTracker extends WriteJobStatsTracker with Logging {

  private[this] var indexBuildResultSeq: Seq[IndexBuildResult] = _

  override def newTaskInstance(): WriteTaskStatsTracker = new OapIndexWriteTaskStatsTracker

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    indexBuildResultSeq = stats.flatMap(_.asInstanceOf[IndexWriteTaskStats].writeStatus)
  }

  def indexBuildResults: Seq[IndexBuildResult] = indexBuildResultSeq
}
