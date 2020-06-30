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

package org.apache.spark.shuffle.remote

import org.apache.spark.{Aggregator, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.RemoteAppendOnlyMap

/**
  * NOTE:
  *
  * :: DeveloperApi ::
  * A set of functions used to aggregate data.
  *
  * @param createCombiner function to create the initial value of the aggregation.
  * @param mergeValue function to merge a new value into the aggregation result.
  * @param mergeCombiners function to merge outputs from multiple mergeValue function.
  */
@DeveloperApi
class RemoteAggregator[K, V, C](agg: Aggregator[K, V, C], resolver: RemoteShuffleBlockResolver)
    extends Aggregator[K, V, C](agg.createCombiner, agg.mergeValue, agg.mergeCombiners) {

  override def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new RemoteAppendOnlyMap[K, V, C](
      createCombiner, mergeValue, mergeCombiners, resolver = resolver)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  override def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new RemoteAppendOnlyMap[K, C, C](
      identity, mergeCombiners, mergeCombiners, resolver = resolver)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: RemoteAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}
