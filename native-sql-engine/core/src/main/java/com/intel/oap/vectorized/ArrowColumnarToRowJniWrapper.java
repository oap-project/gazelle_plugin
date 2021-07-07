package com.intel.oap.vectorized;


import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import java.io.IOException;

public class ArrowColumnarToRowJniWrapper {

  public ArrowColumnarToRowJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  public native long nativeConvertColumnarToRow(
    byte[] schema, int numRows, long[] bufAddrs, long[] bufSizes, long memory_pool_id) throws RuntimeException;

  public native boolean nativeHasNext(long instanceID);

  public native UnsafeRow nativeNext(long instanceID);
}
