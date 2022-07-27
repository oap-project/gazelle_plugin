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

package com.intel.oap.vectorized;


import com.intel.oap.expression.ConverterUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

public class SplitIterator implements Iterator<ColumnarBatch>{

  public static class IteratorOptions implements Serializable {
    private static final long serialVersionUID = -1L;

    private int partitionNum;

    private String name;

    private long offheapPerTask;

    private int bufferSize;

    private String expr;

    public NativePartitioning getNativePartitioning() {
      return nativePartitioning;
    }

    public void setNativePartitioning(NativePartitioning nativePartitioning) {
      this.nativePartitioning = nativePartitioning;
    }

    NativePartitioning nativePartitioning;

    public int getPartitionNum() {
      return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
      this.partitionNum = partitionNum;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getOffheapPerTask() {
      return offheapPerTask;
    }

    public void setOffheapPerTask(long offheapPerTask) {
      this.offheapPerTask = offheapPerTask;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
    }

    public String getExpr() {
      return expr;
    }

    public void setExpr(String expr) {
      this.expr = expr;
    }

  }

  ShuffleSplitterJniWrapper jniWrapper;

  private long nativeSplitter = 0;
  private final Iterator<ColumnarBatch> iterator;
  private final IteratorOptions options;

  private ColumnarBatch cb = null;

  public SplitIterator(ShuffleSplitterJniWrapper jniWrapper,
                       Iterator<ColumnarBatch> iterator, IteratorOptions options)  {
    this.jniWrapper = jniWrapper;
    this.iterator = iterator;
    this.options = options;
  }

  private void nativeCreateInstance() {
    ArrowRecordBatch recordBatch = ConverterUtils.createArrowRecordBatch(cb);
    try {
      nativeSplitter = jniWrapper.make(
              options.getNativePartitioning(),
              options.getOffheapPerTask(),
              options.getBufferSize());
      int len = recordBatch.getBuffers().size();
      long[] bufAddrs = new long[len];
      long[] bufSizes = new long[len];
      int i = 0, j = 0;
      for (ArrowBuf buffer: recordBatch.getBuffers()) {
        bufAddrs[i++] = buffer.memoryAddress();
      }
      for (ArrowBuffer buffer: recordBatch.getBuffersLayout()) {
        bufSizes[j++] = buffer.getSize();
      }
      jniWrapper.split(nativeSplitter, cb.numRows(), bufAddrs, bufSizes, false);
      jniWrapper.collect(nativeSplitter, cb.numRows());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private native boolean nativeHasNext(long instance);

  public boolean hasRecordBatch(){
    while (iterator.hasNext()) {
      cb = iterator.next();
      if (cb.numRows() != 0 && cb.numCols() != 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    // 1. Init the native splitter
    if (nativeSplitter == 0) {
      boolean flag = hasRecordBatch();
      if (!flag) {
        return false;
      } else {
        nativeCreateInstance();
      }
    }
    // 2. Call native hasNext
    if (nativeHasNext(nativeSplitter)) {
      return true;
    } else {
      boolean flag = hasRecordBatch();
      if (!flag) {
        return false;
      } else {
        nativeCreateInstance();
      }
    }
    return nativeHasNext(nativeSplitter);
  }

  private native byte[] nativeNext(long instance);

  @Override
  public ColumnarBatch next() {
    byte[] serializedRecordBatch = nativeNext(nativeSplitter);
    ColumnarBatch cb = ConverterUtils.createRecordBatch(serializedRecordBatch,
            options.getNativePartitioning().getSchema());
    return cb;
  }

  private native int nativeNextPartitionId(long nativeSplitter);

  public int nextPartitionId() {
    return nativeNextPartitionId(nativeSplitter);
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      jniWrapper.clear(nativeSplitter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
