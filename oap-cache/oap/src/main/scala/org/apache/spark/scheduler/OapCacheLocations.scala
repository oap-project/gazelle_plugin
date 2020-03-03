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

package org.apache.spark.scheduler

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object OapCacheLocations extends Logging {

  private[scheduler] def getOapCacheLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    val locations = rdd.preferredLocations(rdd.partitions(partition))

    // here we check if the splits was cached, if the splits is cached, there will be hosts with
    // format "OAP_HOST_host_OAP_EXECUTOR_exec", so we check the host list to filter out
    // the cached hosts, so that the tasks' locality level will be PROCESS_LOCAL
    if (locations.nonEmpty) {
      // TODO use constant value for prefixes, these prefixes should be the same with that in
      // [[org.apache.spark.sql.execution.datasources.oap.FiberSensor]]
      val cacheLocs = locations.filter(_.startsWith("OAP_HOST_"))
      val oapPrefs = cacheLocs.map { cacheLoc =>
        val host = cacheLoc.split("_OAP_EXECUTOR_")(0).stripPrefix("OAP_HOST_")
        val execId = cacheLoc.split("_OAP_EXECUTOR_")(1)
        (host, execId)
      }
      if (oapPrefs.nonEmpty) {
        logDebug(s"got oap prefer location value oapPrefs is ${oapPrefs}")
        return oapPrefs.map(loc => TaskLocation(loc._1, loc._2))
      }
    }
    Seq.empty
  }
}
