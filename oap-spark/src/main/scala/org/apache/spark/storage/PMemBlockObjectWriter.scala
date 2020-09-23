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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, OutputStream}

import com.intel.oap.common.storage.stream.{ChunkOutputStream, DataStore}

import org.apache.spark.internal.Logging
import org.apache.spark.memory.PMemManagerInitializer
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class PMemBlockObjectWriter(
    file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetricsReporter,
    blockId: BlockId = null)
  extends DiskBlockObjectWriter(file, serializerManager,
    serializerInstance, bufferSize, syncWrites, writeMetrics, blockId)
  with Logging {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  // private var dataStore: DataStore = dataStore
  /** The file channel, used for repositioning / truncating the file. */
  // private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: ChunkOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false
  private var dataStore: DataStore = null

  def getFOS(): ChunkOutputStream = {
   fos
  }

  // for tmp usage in UT
  def getDataStore(): DataStore = {
    dataStore
  }

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|
   *           ^          ^
   *           |          |
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * same as chunkoutputstream.position()
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  // private var committedPosition = file.length()
  private var committedPosition = 0;
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    dataStore = new DataStore(PMemManagerInitializer.getPMemManager(),
      PMemManagerInitializer.getProperties());
    fos = ChunkOutputStream.getChunkOutputStreamInstance(file.toString, dataStore)
    committedPosition = fos.position().toInt
    reportedPosition = committedPosition
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  override def open(): PMemBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) Utils.tryWithSafeFinally {
      mcs.manualClose()
    } {
      mcs = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      streamOpen = false
      hasBeenClosed = true
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  override def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }
      val pos = fos.position().toInt
      val previousCommitedPosition = committedPosition
      val length = pos - committedPosition
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
      new FileSegment(file, previousCommitedPosition, length)
    } else {
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  override def revertPartialWritesAndClose(): File = {
    var cos: ChunkOutputStream = null
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      cos = ChunkOutputStream.getChunkOutputStreamInstance(file.toString, dataStore)
      cos.truncate(committedPosition)
    }
    new File(file.toString)
  }


  /**
    * Writes a key-value pair.
    */
  override def write(key: Any, value: Any): Unit = {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
    * Notify the writer that a record worth of bytes has been written with OutputStream#write.
    */
  override def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  // For testing
  private[spark] override def flush(): Unit = {
    objOut.flush()
    bs.flush()
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    // val pos = channel.position()
    // in high level, sometimes 16384 records written so this function called
    // but the data is not actually wrote to PMem because writeBuffer isn't full
    // and spill not finished
    if (fos != null) {
      val pos = fos.position().toInt
      writeMetrics.incBytesWritten(pos - reportedPosition)
      reportedPosition = pos
    }
  }
}
