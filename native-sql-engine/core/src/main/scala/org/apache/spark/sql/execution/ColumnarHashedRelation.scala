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

package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.{ArrowWritableColumnVector, SerializableObject}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ColumnarHashedRelation.Deallocator
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{KnownSizeEstimation, Utils}

class ColumnarHashedRelation(
    var hashRelationObj: SerializableObject,
    var arrowColumnarBatch: Array[ColumnarBatch],
    var arrowColumnarBatchSize: Int)
  extends Externalizable
    with KryoSerializable
    with KnownSizeEstimation
    with Logging {

  // The implementation of Cleaner changed from JDK 8 to 9
  // --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED should be set to JVM while using jdk11
  val cleanerClassName = {
    val majorVersion = System.getProperty("java.version").split("\\D+")(0).toInt
    if (majorVersion < 9) {
      "sun.misc.Cleaner"
    } else {
      "jdk.internal.ref.Cleaner"
    }
  }

  createCleaner(hashRelationObj, arrowColumnarBatch)

  def this() = {
    this(null, null, 0)
  }

  private def createCleaner(obj: SerializableObject, batch: Array[ColumnarBatch]): Unit = {
    if (obj == null && batch == null) {
      // no need to clean up
      return
    }
    val cleanerClass = Utils.classForName(cleanerClassName)
    val createMethod = cleanerClass.getMethod(
      "create", classOf[Object], classOf[Runnable])
    // Accessing jdk.internal.ref.Cleaner should actually fail by default in JDK 9+,
    // unfortunately, unless the user has allowed access with something like
    // --add-opens java.base/java.lang=ALL-UNNAMED  If not, we can't really use the Cleaner
    // hack below. It doesn't break, just means the user might run into the default JVM limit
    // on off-heap memory and increase it or set the flag above. This tests whether it's
    // available:
    try {
      createMethod.invoke(null, this, new Deallocator(obj, batch))
    } catch {
      case e: IllegalAccessException =>
        // Don't throw an exception, but can't log here?
        logError("failed to run Cleaner.create", e)
    }
  }


  def asReadOnlyCopy(): ColumnarHashedRelation = {
    this
  }

  override def estimatedSize: Long = 0

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    out.writeObject(rawArrowData)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    kryo.writeObject(out, hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    kryo.writeObject(out, rawArrowData)
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashRelationObj = in.readObject().asInstanceOf[SerializableObject]
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    createCleaner(hashRelationObj, arrowColumnarBatch)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    hashRelationObj =
      kryo.readObject(in, classOf[SerializableObject]).asInstanceOf[SerializableObject]
    val rawArrowData = kryo.readObject(in, classOf[Array[Byte]]).asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    createCleaner(hashRelationObj, arrowColumnarBatch)
  }

  def size(): Int = {
    hashRelationObj.total_size + arrowColumnarBatchSize
  }

  def getColumnarBatchAsIter: Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var idx = 0
      val total_len = arrowColumnarBatch.length
      override def hasNext: Boolean = idx < total_len
      override def next(): ColumnarBatch = {
        val tmp_idx = idx
        idx += 1
        val cb = arrowColumnarBatch(tmp_idx)
        // retain all cols
        (0 until cb.numCols).toList.foreach(i =>
          cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
        cb
      }
    }
  }
}
object ColumnarHashedRelation extends Logging {

  private class Deallocator (
      var hashRelationObj: SerializableObject,
      var arrowColumnarBatch: Array[ColumnarBatch]) extends Runnable {

    override def run(): Unit = {
      try {
        Option(hashRelationObj).foreach(_.close())
        Option(arrowColumnarBatch).foreach(_.foreach(_.close))
      } catch {
        case e: Exception =>
          // We should suppress all possible errors in Cleaner to prevent JVM from being shut down
          logError("ColumnarHashedRelation: Error running deallocator", e)
      }
    }
  }
}
