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

package org.apache.spark.shuffle

import java.io.File

import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleWriter

object ShuffleUtil {

  /**
    * Only applicable to IndexShuffleBlockResolver. We move the implementation here, because
    * IndexShuffleBlockResolver's access modifier is private[spark].
    */
  def shuffleBlockResolverWriteAndCommit(shuffleBlockResolver: MigratableResolver,
                                         shuffleId: Int, mapId: Long, partitionLengths: Array[Long], dataTmp: File): Unit =
    shuffleBlockResolver match {
      case resolver: IndexShuffleBlockResolver =>
        resolver.writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, null, dataTmp)
      case _: throw new RuntimeException("IndexShuffleBlockResolver is expected!")
    }

  def newSortShuffleWriter(resolver: MigratableResolver, shuffleHandle: ShuffleHandle,
                           mapId: Long, context: TaskContext,
                           shuffleExecutorComponents: ShuffleExecutorComponents): AnyRef = {

    shuffleHandle match {
      case baseShuffleHandle: BaseShuffleHandle =>
        new SortShuffleWriter(
          baseShuffleHandle,
          mapId,
          context,
          shuffleExecutorComponents)
      case _ => throw new RuntimeException("BaseShuffleHandle is expected!")
    }
  }
}