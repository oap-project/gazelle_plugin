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

package org.apache.spark.sql.execution.datasources;

import java.time.ZoneId;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;

/**
 * A class to help fix compatibility issues for class who extends VectorizedParquetRecordReader.
 */
public class VectorizedParquetRecordReaderChild extends VectorizedParquetRecordReader {

  public VectorizedParquetRecordReaderChild(ZoneId convertTz,
    String datetimeRebaseMode,
    String datetimeRebaseTz,
    String int96RebaseMode,
    String int96RebaseTz,
    boolean useOffHeap,
    int capacity) {
    super(convertTz, datetimeRebaseMode, int96RebaseMode, useOffHeap, capacity);
  }
}