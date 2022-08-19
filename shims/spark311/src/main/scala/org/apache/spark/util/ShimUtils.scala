/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.File

object ShimUtils {

  /**
    * Only applicable to IndexShuffleBlockResolver. We move the implementation here, because
    * IndexShuffleBlockResolver's access modifier is private[spark].
    */
  def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                         shuffleId: Int, mapId: Long,
                                         partitionLengths: Array[Long], dataTmp: File): Unit = {
    shuffleBlockResolver match {
      case resolver: IndexShuffleBlockResolver =>
        resolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, dataTmp)
      case _ => throw new RuntimeException ("IndexShuffleBlockResolver is expected!")
    }
  }

  def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
    mapId: Long, context: TaskContext,
    shuffleExecutorComponents: ShuffleExecutorComponents): AnyRef = {
    resolver match {
      case indexShuffleBlockResolver: IndexShuffleBlockResolver =>
        shuffleHandle match {
          case baseShuffleHandle: BaseShuffleHandle[_, _, _] =>
            new SortShuffleWriter(
              indexShuffleBlockResolver,
              baseShuffleHandle,
              mapId,
              context,
              shuffleExecutorComponents)
          case _ => throw new RuntimeException("BaseShuffleHandle is expected!")
        }
      case _ => throw new RuntimeException("IndexShuffleBlockResolver is expected!")
    }
  }

  /**
    * We move the implementation into this package because Utils has private[spark]
    * access modifier.
    */
  def doFetchFile(urlString: String, targetDirHandler: File,
                  targetFileName: String, sparkConf: SparkConf): Unit = {
    Utils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf, null, null)
  }
}