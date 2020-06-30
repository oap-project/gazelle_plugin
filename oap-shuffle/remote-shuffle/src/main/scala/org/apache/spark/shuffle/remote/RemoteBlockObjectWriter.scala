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

import java.io.{BufferedOutputStream, OutputStream}

import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.storage.{BlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

/**
  * References a particular segment of a Hadoop file (potentially the entire file),
  * based off an offset and a length.
  */
private[spark] class HadoopFileSegment(val file: Path, val offset: Long, val length: Long) {
  require(offset >= 0, s"File segment offset cannot be negative (got $offset)")
  require(length >= 0, s"File segment length cannot be negative (got $length)")
  override def toString: String = {
    "(name=%s, offset=%d, length=%d)".format(file.getName, offset, length)
  }
}

/**
  * NOTE: Most of the code is copied from DiskBlockObjectWriter, as the only difference is that this
  * class performs a block object writing to remote storage
  *
  * A class for writing JVM objects directly to a file on remote storage. This class allows data to
  * be appended to an existing block. For efficiency, it retains the underlying file channel across
  * multiple commits. This channel is kept open until close() is called. In case of faults,
  * callers should instead close with revertPartialWritesAndClose() to atomically revert the
  * uncommitted partial writes.
  *
  * This class does not support concurrent writes. Also, once the writer has been opened it
  * cannot be reopened again.
  */
private[spark] class RemoteBlockObjectWriter(
    val file: Path,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetricsReporter,
    val blockId: BlockId = null)
    extends OutputStream
        with Logging {

  private lazy val fs = RemoteShuffleManager.getFileSystem

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

  // No need to use a channel, instead call FSDataOutputStream.getPos
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fsdos: FSDataOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  /**
    * Cursors used to represent positions in the file.
    *
    * xxxxxxxxxx|----------|-----|
    *           ^          ^     ^
    *           |          |    channel.position()
    *           |        reportedPosition
    *         committedPosition
    *
    * reportedPosition: Position at the time of the last update to the write metrics.
    * committedPosition: Offset after last committed write.
    * -----: Current writes to the underlying file.
    * xxxxx: Committed contents of the file.
    */
  private var committedPosition = 0L
  private var reportedPosition = committedPosition

  /**
    * Keep track of number of records written and also use this to periodically
    * output bytes written since the latter is expensive to do for each record.
    * And we reset it after every commitAndGet called.
    */
  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    fsdos = fs.create(file)
    ts = new TimeTrackingOutputStream(writeMetrics, fsdos)
    class ManualCloseBufferedOutputStream
        extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def open(): RemoteBlockObjectWriter = {
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
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        mcs = null
        bs = null
        fsdos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
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
  def commitAndGet(): HadoopFileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      /* NOTE by Chenzhao: It doesn't work for local file system */
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fsdos.hsync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = fsdos.getPos
      val fileSegment = new HadoopFileSegment(file, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
      fileSegment
    } else {
      new HadoopFileSegment(file, committedPosition, 0)
    }
  }


  /**
    * Reverts writes that haven't been committed yet. Callers should invoke this function
    * when there are runtime exceptions. This method will not throw, though it may be
    * unsuccessful in truncating written data.
    *
    * @return the file that this DiskBlockObjectWriter wrote to.
    */
  def revertPartialWritesAndClose(): Path = {
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
      try {
        close()
        fs.truncate(file, committedPosition)
      } catch {
        case _: UnsupportedOperationException => logInfo("This filesystem doesn't support" +
            "truncate")
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
      }
    }
    file
  }

  /**
    * Writes a key-value pair.
    */
  def write(key: Any, value: Any) {
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
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
    * Report the number of bytes written in this writer's shuffle write metrics.
    * Note that this is only valid before the underlying streams are closed.
    */
  private def updateBytesWritten() {
    val pos = fsdos.getPos
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
