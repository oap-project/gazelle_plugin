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

import org.apache.arrow.memory.ArrowBuf;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.*;
import org.apache.arrow.util.Preconditions;

import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple reference manager implementation for memory allocated by native code. The underlying
 * memory will be released when reference count reach zero.
 *
 * Jul/13/2020, Hongze Zhang: TO BE DEPRECATED: Only nativeRelease is being called at this time.
 */
@Deprecated
public class AdaptorReferenceManager implements ReferenceManager {
  public static final AdaptorReferenceManager DEFAULT = new AdaptorReferenceManager(-1L, -1);

  public native void nativeRelease(long nativeMemoryHolder);

  private static final Logger LOG = LoggerFactory.getLogger(AdaptorReferenceManager.class);
  private final AtomicInteger bufRefCnt = new AtomicInteger(0);
  private long nativeMemoryHolder;
  private int size = 0;

  AdaptorReferenceManager(long nativeMemoryHolder, int size) {
    try {
      JniUtils.getInstance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.nativeMemoryHolder = nativeMemoryHolder;
    this.size = size;
  }
  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(
        decrement >= 1, "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt;
    synchronized (this) {
      refCnt = bufRefCnt.addAndGet(-decrement);
      if (refCnt == 0) {
        // refcount of this reference manager has dropped to 0
        // release the underlying memory
        nativeRelease(nativeMemoryHolder);
      }
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);
    bufRefCnt.addAndGet(increment);
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();
    return srcBuffer;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(this, null, length, derivedBufferAddress);

    return derivedBuf;
  }

  @Override
  public OwnershipTransferResult transferOwnership(
      ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    return NO_OP.transferOwnership(sourceBuffer, targetAllocator);
  }

  @Override
  public BufferAllocator getAllocator() {
    throw new UnsupportedOperationException("No Allocator is retained");
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public long getAccountedSize() {
    return 0;
  }
}
