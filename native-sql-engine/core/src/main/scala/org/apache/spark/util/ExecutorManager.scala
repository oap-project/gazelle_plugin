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
package org.apache.spark.util

import java.lang.management.ManagementFactory

import scala.util.Random

import com.intel.oap._

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging

object ExecutorManager extends Logging {
  def getExecutorIds(sc: SparkContext): Seq[String] = sc.getExecutorIds
  var isTaskSet: Boolean = false
  def tryTaskSet(numaInfo: GazelleNumaBindingInfo): Unit = synchronized {
    if (numaInfo.enableNumaBinding && !isTaskSet) {
      val cmd_output =
        Utils.executeAndGetOutput(
          Seq("bash", "-c", "ps -ef | grep YarnCoarseGrainedExecutorBackend"))
      val getExecutorId = """--executor-id (\d+)""".r
      val executorIdOnLocalNode = {
        val tmp = for (m <- getExecutorId.findAllMatchIn(cmd_output)) yield m.group(1)
        tmp.toList.distinct
      }
      val executorId = SparkEnv.get.executorId
      val coreRange = numaInfo.totalCoreRange
      val shouldBindNumaIdx = if (executorIdOnLocalNode.isEmpty) {
        // support run with out yarn, such as local
        Random.nextInt(coreRange.length - 1)
      } else {
        executorIdOnLocalNode.indexOf(executorId) % coreRange.length
      }
      logInfo(s"executorId is $executorId, executorIdOnLocalNode is $executorIdOnLocalNode")
      val taskSetCmd = s"taskset -cpa ${coreRange(shouldBindNumaIdx)} ${getProcessId()}"
      logInfo(s"taskSetCmd is $taskSetCmd")
      isTaskSet = true
      Utils.executeCommand(Seq("bash", "-c", taskSetCmd))
    }
  }

  def getProcessId(): Int = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean()
    runtimeMXBean.getName().split("@")(0).toInt
  }

}
