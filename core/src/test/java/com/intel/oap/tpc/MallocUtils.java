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

package com.intel.oap.tpc;

import com.intel.oap.vectorized.JniUtils;

import java.io.IOException;

public class MallocUtils {

  static {
    try {
      JniUtils.getInstance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Visible for testing: Try turning back allocated native memory to OS. This might have no effect
   * when using Jemalloc.
   */
  public static native void mallocTrim();

  /**
   * Visible for testing: Print malloc statistics.
   */
  public static native void mallocStats();
}
