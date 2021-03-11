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

package com.intel.oap.vectorized

import java.io._
import java.nio.ByteBuffer

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.expression.ConverterUtils
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.{
  BaseFixedWidthVector,
  BaseVariableWidthVector,
  VectorLoader,
  VectorSchemaRoot
}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{
  DeserializationStream,
  SerializationStream,
  Serializer,
  SerializerInstance
}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class ArrowColumnarBatchSerializer(readBatchNumRows: SQLMetric, numOutputRows: SQLMetric)
    extends Serializer
    with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance =
    new ArrowColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows)
}

private class ArrowColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric)
    extends SerializerInstance
    with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private val compressionEnabled =
        SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)

      private val allocator: BufferAllocator = SparkMemoryUtils.contextAllocator()
        .newChildAllocator("ArrowColumnarBatch deserialize", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var vectors: Array[ColumnVector] = _
      private var cb: ColumnarBatch = _
      private var batchLoaded = true

      private var jniWrapper: ShuffleDecompressionJniWrapper = _
      private var schemaHolderId: Long = 0
      private var vectorLoader: VectorLoader = _

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      private var isClosed: Boolean = false

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        if (reader != null && batchLoaded) {
          root.clear()
          if (cb != null) {
            cb.close()
            cb = null
          }

          try {
            batchLoaded = reader.loadNextBatch()
          } catch {
            case ioe: IOException =>
              this.close()
              logError("Failed to load next RecordBatch", ioe)
              throw ioe
          }
          if (batchLoaded) {
            val numRows = root.getRowCount
            logDebug(s"Read ColumnarBatch of ${numRows} rows")

            numBatchesTotal += 1
            numRowsTotal += numRows

            // jni call to decompress buffers
            if (compressionEnabled) {
              try {
                decompressVectors()
              } catch {
                case e: UnsupportedOperationException =>
                  this.close()
                  throw e
              }
            }

            val newFieldVectors = root.getFieldVectors.asScala.map { vector =>
              val newVector = vector.getField.createVector(allocator)
              vector.makeTransferPair(newVector).transfer()
              newVector
            }.asJava

            vectors = ArrowWritableColumnVector
              .loadColumns(numRows, newFieldVectors)
              .toArray[ColumnVector]

            cb = new ColumnarBatch(vectors, numRows)
            cb.asInstanceOf[T]
          } else {
            this.close()
            throw new EOFException
          }
        } else {
          if (compressionEnabled) {
            reader = new ArrowCompressedStreamReader(in, allocator)
          } else {
            reader = new ArrowStreamReader(in, allocator)
          }
          try {
            root = reader.getVectorSchemaRoot
          } catch {
            case _: IOException =>
              this.close()
              throw new EOFException
          }
          readValue()
        }
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (!isClosed) {
          if (numBatchesTotal > 0) {
            readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
          numOutputRows += numRowsTotal
          if (cb != null) cb.close()
          if (reader != null) reader.close(true)
          if (jniWrapper != null) jniWrapper.close(schemaHolderId)
          isClosed = true
        }
      }

      private def decompressVectors(): Unit = {
        if (jniWrapper == null) {
          jniWrapper = new ShuffleDecompressionJniWrapper
          schemaHolderId = jniWrapper.make(ConverterUtils.getSchemaBytesBuf(root.getSchema))
        }
        if (vectorLoader == null) {
          vectorLoader = new VectorLoader(root)
        }
        val bufAddrs = new ListBuffer[Long]()
        val bufSizes = new ListBuffer[Long]()
        val bufBS = mutable.BitSet()
        var bufIdx = 0

        root.getFieldVectors.asScala.foreach { vector =>
          val validityBuf = vector.getValidityBuffer
          if (validityBuf
                .capacity() <= 8 || java.lang.Long.bitCount(validityBuf.getLong(0)) == 64 ||
              java.lang.Long.bitCount(validityBuf.getLong(0)) == 0) {
            bufBS.add(bufIdx)
          }
          // don't call vector.getBuffers to avoid extra check
          val buffers = vector match {
            case fixed: BaseFixedWidthVector =>
              fixed.getValidityBuffer :: fixed.getDataBuffer :: Nil
            case variable: BaseVariableWidthVector =>
              variable.getValidityBuffer :: variable.getOffsetBuffer :: variable.getDataBuffer :: Nil
            case _ =>
              throw new UnsupportedOperationException(
                s"Could not decompress vector of class ${vector.getClass}")
          }
          buffers.foreach { buffer =>
            bufAddrs += buffer.memoryAddress()
            // buffer.readableBytes() will return wrong readable length here since it is initialized by
            // data stored in IPC message header, which is not the actual compressed length
            bufSizes += buffer.capacity()
            bufIdx += 1
          }
        }

        val builder = jniWrapper.decompress(
          schemaHolderId,
          reader.asInstanceOf[ArrowCompressedStreamReader].GetCompressType(),
          root.getRowCount,
          bufAddrs.toArray,
          bufSizes.toArray,
          bufBS.toBitMask)
        val builerImpl = new ArrowRecordBatchBuilderImpl(builder)
        val decompressedRecordBatch = builerImpl.build

        root.clear()
        if (decompressedRecordBatch != null) {
          vectorLoader.load(decompressedRecordBatch)
          decompressedRecordBatch.close()
        }
      }
    }
  }

  // Columnar shuffle write process don't need this.
  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
