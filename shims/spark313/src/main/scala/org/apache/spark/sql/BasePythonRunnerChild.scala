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

package org.apache.spark.sql

import scala.collection.JavaConverters._
import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, SpecialLengths}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
  * We put this class in this package in order to access ArrowUtils, which has
  * private[sql] access modifier.
  */
abstract class BasePythonRunnerChild(funcs: Seq[ChainedPythonFunctions],
                                     evalType: Int,
                                     argOffsets: Array[Array[Int]])
  extends BasePythonRunner[ColumnarBatch, ColumnarBatch](funcs, evalType, argOffsets) {

  /**
    * The implementation is completely as same as that for spark 3.1 except the arguments for
    * newReaderIterator & ReaderIterator. The pid: Option[Int] is introduced from spark 3.2.
    * TODO: put the implementation into some common place.
    */
  protected def newReaderIterator(
                                   stream: DataInputStream,
                                   writerThread: WriterThread,
                                   startTime: Long,
                                   env: SparkEnv,
                                   worker: Socket,
                                   releasedOrClosed: AtomicBoolean,
                                   context: TaskContext): Iterator[ColumnarBatch] = {

    // The pid argument is added for ReaderIterator since spark3.2. So we introduce
    // ReaderIteratorChild to help fix the compatibility issues.
    new ReaderIterator(stream, writerThread, startTime, env, worker, releasedOrClosed, context) {
      private val allocator = SparkMemoryUtils.contextAllocator().newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = ArrowUtils.fromArrowSchema(root.getSchema())
                vectors = ArrowWritableColumnVector.loadColumns(root.getRowCount, root.getFieldVectors).toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }

}