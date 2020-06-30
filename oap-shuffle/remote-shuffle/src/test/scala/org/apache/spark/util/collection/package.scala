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

package org.apache.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.remote.RemoteShuffleManager

package object collection {
  def createDefaultConf(loadDefaults: Boolean = true): SparkConf = {
    new SparkConf(loadDefaults)
      .set("spark.shuffle.manager", classOf[RemoteShuffleManager].getCanonicalName)
      // Unit tests should not rely on external systems, using local file system as storage
      .set("spark.shuffle.remote.storageMasterUri", "file://")
      .set("spark.shuffle.remote.filesRootDirectory", "/tmp")
      .set("spark.shuffle.sync", "true")
  }
}
