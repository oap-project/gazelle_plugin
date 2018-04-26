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

package org.apache.spark.sql.oap.rpc

import scala.collection.immutable.HashSet

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.oap.filecache.{CacheStats, FiberCacheManager}
import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfo
import org.apache.spark.sql.oap.rpc.OapMessages._
import org.apache.spark.storage.BlockManager

trait OapHeartbeatMaterialsInterface {
  def get: HashSet[() => Heartbeat]
}

private[rpc] class OapHeartbeatMaterials(
    executorId: String,
    blockManager: BlockManager,
    conf: SparkConf) extends OapHeartbeatMaterialsInterface {

  val blockManagerId = blockManager.blockManagerId

  val fiberCacheHeartbeat =
    () => FiberCacheHeartbeat(executorId, blockManagerId, FiberCacheManager.status())
  val indexHeartbeat = () => IndexHeartbeat(executorId, blockManagerId, OapIndexInfo.status)
  val fiberCacheMetricsHeartbeat =
    () => FiberCacheMetricsHeartbeat(
      executorId, blockManagerId, CacheStats.status(FiberCacheManager.cacheStats, conf))

  private val materialsSet = HashSet[() => Heartbeat] (
    fiberCacheHeartbeat,
    indexHeartbeat,
    fiberCacheMetricsHeartbeat
  )

  def get: HashSet[() => Heartbeat] = materialsSet
}
