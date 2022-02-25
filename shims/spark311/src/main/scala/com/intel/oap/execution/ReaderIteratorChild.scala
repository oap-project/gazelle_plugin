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

package com.intel.oap.execution

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.api.python.BasePythonRunner


/**
  * pid: Option[Int] is not instroduced for ReaderIterator in spark 3.1.
  * This class plays a role to help fix compatibility isues.
  */
abstract class ReaderIteratorChild(stream: DataInputStream,
                                    writerThread: WriterThread,
                                    startTime: Long,
                                    env: SparkEnv,
                                    worker: Socket,
                                    pid: Option[Int],
                                    releasedOrClosed: AtomicBoolean,
                                    context: TaskContext)
  extends ReaderIterator(stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext) {

}