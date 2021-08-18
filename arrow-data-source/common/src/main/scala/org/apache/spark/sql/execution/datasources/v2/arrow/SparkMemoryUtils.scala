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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

import com.intel.oap.spark.sql.execution.datasources.v2.arrow._
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.memory.BufferAllocator

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.util.TaskCompletionListener

object SparkMemoryUtils extends Logging {
  private val DEBUG: Boolean = false

  class TaskMemoryResources {
    if (!inSparkTask()) {
      throw new IllegalStateException("Creating TaskMemoryResources instance out of Spark task")
    }

    val sharedMetrics = new NativeSQLMemoryMetrics()
    val defaultAllocator: BufferAllocator = {

      val globalAlloc = globalAllocator()
      val al = new SparkManagedAllocationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), Spiller.NO_OP),
        sharedMetrics)
      val parent = globalAlloc
      parent.newChildAllocator("Spark Managed Allocator - " +
        UUID.randomUUID().toString, al, 0, parent.getLimit)
    }

    val defaultMemoryPool: NativeMemoryPoolWrapper = {
      val rl = new SparkManagedReservationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), Spiller.NO_OP),
        sharedMetrics)
      NativeMemoryPoolWrapper(NativeMemoryPool.createListenable(rl), rl)
    }

    private val allocators = new util.ArrayList[BufferAllocator]()
    allocators.add(defaultAllocator)

    private val memoryPools = new util.ArrayList[NativeMemoryPoolWrapper]()
    memoryPools.add(defaultMemoryPool)

    def createSpillableMemoryPool(spiller: Spiller): NativeMemoryPool = {
      val rl = new SparkManagedReservationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), spiller),
        sharedMetrics)
      val pool = NativeMemoryPool.createListenable(rl)
      memoryPools.add(NativeMemoryPoolWrapper(pool, rl))
      pool
    }

    def createSpillableAllocator(spiller: Spiller): BufferAllocator = {
      val al = new SparkManagedAllocationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), spiller),
        sharedMetrics)
      val parent = globalAllocator()
      val alloc = parent.newChildAllocator("Spark Managed Allocator - " +
        UUID.randomUUID().toString, al, 0, parent.getLimit).asInstanceOf[BufferAllocator]
      allocators.add(alloc)
      alloc
    }

    private def close(allocator: BufferAllocator): Unit = {
      allocator.getChildAllocators.forEach(close(_))
      allocator.close()
    }

    /**
     * Close the allocator quietly without having any OOM errors thrown. We rely on Spark's memory
     * management system to detect possible memory leaks after the task get successfully down. Any
     * leak shown right here is possibly not actual because buffers may be cleaned up after
     * this check code is executed. Having said that developers should manage to make sure
     * the specific clean up logic of operators is registered at last of the program which means
     * it will be executed earlier.
     *
     * @see org.apache.spark.executor.Executor.TaskRunner#run()
     */
    private def softClose(allocator: BufferAllocator): Unit = {
      // move to leaked list
      logWarning(s"Detected leaked allocator, size: ${allocator.getAllocatedMemory}...")
      if (DEBUG) {
        leakedAllocators.add(allocator)
      }
    }

    private def close(pool: NativeMemoryPoolWrapper): Unit = {
      pool.pool.close()
    }

    private def softClose(pool: NativeMemoryPoolWrapper): Unit = {
      // move to leaked list
      logWarning(s"Detected leaked memory pool, size: ${pool.pool.getBytesAllocated}...")
      pool.listener.inactivate()
      if (DEBUG) {
        leakedMemoryPools.add(pool)
      }
    }

    def release(): Unit = {
      for (allocator <- allocators.asScala) {
        val allocated = allocator.getAllocatedMemory
        if (allocated == 0L) {
          close(allocator)
        } else {
          softClose(allocator)
        }
      }
      for (pool <- memoryPools.asScala) {
        val allocated = pool.pool.getBytesAllocated
        if (allocated == 0L) {
          close(pool)
        } else {
          softClose(pool)
        }
      }
    }
  }

  private val taskToResourcesMap = new java.util.IdentityHashMap[TaskContext, TaskMemoryResources]()

  private val leakedAllocators = new java.util.Vector[BufferAllocator]()
  private val leakedMemoryPools = new java.util.Vector[NativeMemoryPoolWrapper]()

  private def getLocalTaskContext: TaskContext = TaskContext.get()

  private def getTaskMemoryManager(): TaskMemoryManager = {
    getLocalTaskContext.taskMemoryManager()
  }

  private def inSparkTask(): Boolean = {
    getLocalTaskContext != null
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    getTaskMemoryResources() // initialize cleaners
    getLocalTaskContext.addTaskCompletionListener(f)
  }

  def getTaskMemoryResources(): TaskMemoryResources = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext
    taskToResourcesMap.synchronized {

      if (!taskToResourcesMap.containsKey(tc)) {
        val resources = new TaskMemoryResources
        getLocalTaskContext.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToResourcesMap.synchronized {
                val resources = taskToResourcesMap.remove(context)
                resources.release()
                context.taskMetrics().incPeakExecutionMemory(resources.sharedMetrics.peak())
              }
            }
          })
        taskToResourcesMap.put(tc, resources)
      }

      return taskToResourcesMap.get(tc)
    }
  }

  def globalAllocator(): BufferAllocator = {
    org.apache.spark.sql.util.ArrowUtils.rootAllocator
  }

  def globalMemoryPool(): NativeMemoryPool = {
    NativeMemoryPool.getDefault
  }

  def createSpillableAllocator(spiller: Spiller): BufferAllocator = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task")
    }
    getTaskMemoryResources().createSpillableAllocator(spiller)
  }

  def createSpillableMemoryPool(spiller: Spiller): NativeMemoryPool = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task")
    }
    getTaskMemoryResources().createSpillableMemoryPool(spiller)
  }

  def contextAllocator(): BufferAllocator = {
    val globalAlloc = globalAllocator()
    if (!inSparkTask()) {
      return globalAlloc
    }
    getTaskMemoryResources().defaultAllocator
  }

  def contextMemoryPool(): NativeMemoryPool = {
    if (!inSparkTask()) {
      return globalMemoryPool()
    }
    getTaskMemoryResources().defaultMemoryPool.pool
  }

  def getLeakedAllocators(): List[BufferAllocator] = {
    val list = new util.ArrayList[BufferAllocator](leakedAllocators)
    list.asScala.toList
  }

  def getLeakedMemoryPools(): List[NativeMemoryPoolWrapper] = {
    val list = new util.ArrayList[NativeMemoryPoolWrapper](leakedMemoryPools)
    list.asScala.toList
  }

  class UnsafeItr[T <: AutoCloseable](delegate: Iterator[T])
      extends Iterator[T] {
    val holder = new GenericRetainer[T]()

    SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((_: TaskContext) => {
      holder.release()
    })

    override def hasNext: Boolean = {
      holder.release()
      val hasNext = delegate.hasNext
      hasNext
    }

    override def next(): T = {
      val b = delegate.next()
      holder.retain(b)
      b
    }
  }

  class GenericRetainer[T <: AutoCloseable] {
    private var retained: Option[T] = None

    def retain(batch: T): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }

  case class NativeMemoryPoolWrapper(pool: NativeMemoryPool,
      listener: SparkManagedReservationListener)
}
