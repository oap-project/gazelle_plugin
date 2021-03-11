package org.apache.spark.util

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.io.{InputStreamReader, BufferedReader}
import scala.collection.mutable.ListBuffer
import java.lang.management.ManagementFactory
import com.intel.oap._

object ExecutorManager {
  def getExecutorIds(sc: SparkContext): Seq[String] = sc.getExecutorIds
  var isTaskSet: Boolean = false
  def tryTaskSet(numaInfo: ColumnarNumaBindingInfo) = synchronized {
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
      val numCorePerExecutor = numaInfo.numCoresPerExecutor
      val coreRange = numaInfo.totalCoreRange
      val shouldBindNumaIdx = executorIdOnLocalNode.indexOf(executorId) % coreRange.size
      //val coreStartIdx = coreRange(shouldBindNumaIdx)._1
      //val coreEndIdx = coreRange(shouldBindNumaIdx)._2
      System.out.println(
        s"executorId is ${executorId}, executorIdOnLocalNode is ${executorIdOnLocalNode}")
      val taskSetCmd = s"taskset -cpa ${coreRange(shouldBindNumaIdx)} ${getProcessId()}"
      System.out.println(taskSetCmd)
      isTaskSet = true
      Utils.executeCommand(Seq("bash", "-c", taskSetCmd))
    }
  }
  def getProcessId(): Int = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean()
    runtimeMXBean.getName().split("@")(0).toInt
  }

}
