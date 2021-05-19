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

package com.intel.oap.spark.sql

import java.lang
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

import scala.util.control.Breaks._

import com.intel.oap.spark.sql.ArrowWriteQueue.EOS_BATCH
import com.intel.oap.spark.sql.ArrowWriteQueue.ScannerImpl
import org.apache.arrow.dataset.file.DatasetFileWriter
import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.scanner.Scanner
import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema

class ArrowWriteQueue(schema: Schema, fileFormat: FileFormat, outputFileURI: String)
    extends AutoCloseable {
  private val scanner = new ScannerImpl(schema)

  private val writeThread = new Thread(() => {
    DatasetFileWriter.write(scanner, fileFormat, outputFileURI)
  })

  writeThread.start()

  def enqueue(batch: ArrowRecordBatch): Unit = {
    scanner.enqueue(batch)
  }

  override def close(): Unit = {
    scanner.enqueue(EOS_BATCH)
    writeThread.join()
  }
}

object ArrowWriteQueue {
  val EOS_BATCH = new ArrowRecordBatch(0, Collections.emptyList(), Collections.emptyList())

  class ScannerImpl(schema: Schema) extends Scanner {
    private val writeQueue = new ArrayBlockingQueue[ArrowRecordBatch](64)

    def enqueue(batch: ArrowRecordBatch): Unit = {
      writeQueue.put(batch)
    }

    override def scan(): lang.Iterable[_ <: ScanTask] = {
      Collections.singleton(new ScanTask {
        override def execute(): ScanTask.BatchIterator = {
          new ScanTask.BatchIterator {
            private var currentBatch: Option[ArrowRecordBatch] = None

            override def hasNext: Boolean = {
              if (currentBatch.isDefined) {
                return true
              }
              val batch = try {
                writeQueue.poll(30L, TimeUnit.MINUTES)
              } catch {
                case _: InterruptedException =>
                  Thread.currentThread().interrupt()
                  EOS_BATCH
              }
              if (batch == null) {
                throw new RuntimeException("ArrowWriter: Timeout waiting for data")
              }
              if (batch == EOS_BATCH) {
                return false
              }
              currentBatch = Some(batch)
              true
            }

            override def next(): ArrowRecordBatch = {
              if (currentBatch.isEmpty) {
                throw new IllegalStateException()
              }
              try {
                currentBatch.get
              } finally {
                currentBatch = None
              }
            }

            override def close(): Unit = {

            }
          }
        }

        override def close(): Unit = {

        }
      })
    }

    override def schema(): Schema = {
      schema
    }

    override def close(): Unit = {

    }
  }
}