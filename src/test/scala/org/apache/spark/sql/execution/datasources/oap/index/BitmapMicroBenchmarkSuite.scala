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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, FileInputStream,
  FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.io.File
import java.util
import org.roaringbitmap.RoaringBitmap
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.collection
import org.apache.spark.util.Utils

import scala.collection.mutable


/**
 * Microbenchmark for Bitmap index with different bitmap implementations.
 */
class BitmapMicroBenchmarkSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._
  private var dir: File = null
  private var path: String = null
  private val intArray: Array[Int] =
    Array(100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1)
  private val sparkBs = new collection.BitSet(100000001)
  private val scalaBs = new mutable.BitSet()
  private val javaBs = new util.BitSet()
  private val rb = new RoaringBitmap()

  override def beforeEach(): Unit = {
    dir = Utils.createTempDir()
    path = dir.getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path')""".stripMargin)

    intArray.foreach(sparkBs.set)
    intArray.foreach(scalaBs.add)
    intArray.foreach(javaBs.set)
    intArray.foreach(rb.add)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    dir.delete()
  }

  test("Bitmap size microbenchmark for different implementations") {
    val sparkBos = new ByteArrayOutputStream()
    val sparkOos = new ObjectOutputStream(sparkBos)
    sparkOos.writeObject(sparkBs)
    sparkOos.flush()
    sparkBos.close()
    sparkOos.close()

    val scalaBos = new ByteArrayOutputStream()
    val scalaOos = new ObjectOutputStream(scalaBos)
    scalaOos.writeObject(scalaBs)
    scalaOos.flush()
    scalaBos.close()
    scalaOos.close()

    val javaBos = new ByteArrayOutputStream()
    val javaOos = new ObjectOutputStream(javaBos)
    javaOos.writeObject(javaBs)
    javaOos.flush()
    javaBos.close()
    javaOos.close()

    rb.runOptimize()
    val rbSeBytes = rb.serializedSizeInBytes()
    val rbBos = new ByteArrayOutputStream()
    val rbOos = new ObjectOutputStream(rbBos)
    rbOos.writeObject(rb)
    rbOos.flush()
    rbBos.close()
    rbOos.close()

    val rbBos2 = new ByteArrayOutputStream()
    val rbOos2 = new ObjectOutputStream(rbBos2)
    rb.writeExternal(rbOos2)
    rbOos2.flush()
    rbBos2.close()
    rbOos2.close()

    // The configuration is my local dev machine(12 cores of Core i7 3.47GHz and 12GB memory)
    /* The result is below:
     * sparkBos.size is 12.5 MB.
     * scalaBos.size is 16.8 MB.
     * javaBos.size is 12.5 MB.
     * rbSeBytes is 66 B.
     * rbBos.size is 121 B.
     * rbBos2.size is 72 B.
     */
  }

  test("Bitmap r/w speed microbenchmark for different implementations") {
    val fileHeader = 4

    val scalaStartTime = System.nanoTime
    val scalaFile = path + "scalaBitSet.bin";
    val scalaFos = new FileOutputStream(scalaFile)
    scalaFos.write(fileHeader)
    val scalaBos = new ByteArrayOutputStream()
    val scalaOos = new ObjectOutputStream(scalaBos);
    scalaOos.writeObject(scalaBs)
    scalaFos.write(scalaBos.toByteArray)
    scalaBos.close()
    scalaOos.close()
    scalaFos.close()
    val scalaByteArraySize = scalaBos.toByteArray.size
    val scalaFis = new FileInputStream(scalaFile)
    val scalaHeaderRead = scalaFis.read()
    assert(scalaHeaderRead == fileHeader)
    val scalaByteArrayRead = new Array[Byte](scalaByteArraySize)
    scalaFis.read(scalaByteArrayRead)
    val scalaBis = new ByteArrayInputStream(scalaByteArrayRead)
    val scalaOis = new ObjectInputStream(scalaBis)
    val scalaBsRead = scalaOis.readObject().asInstanceOf[mutable.BitSet]
    scalaBis.close()
    scalaOis.close()
    scalaFis.close()
    val scalaEndTime = System.nanoTime
    val scalaTime = (scalaEndTime - scalaStartTime) / 1000
    assert(scalaBsRead == scalaBs)

    val javaStartTime = System.nanoTime
    val javaFile = path + "javaBitSet.bin";
    val javaFos = new FileOutputStream(javaFile)
    javaFos.write(fileHeader)
    javaFos.write(javaBs.toByteArray())
    javaFos.close()
    val javaByteArraySize = javaBs.toByteArray.size
    val javaFis = new FileInputStream(javaFile)
    val javaHeaderRead = javaFis.read()
    assert(javaHeaderRead == fileHeader)
    val javaByteArrayRead = new Array[Byte](javaByteArraySize)
    javaFis.read(javaByteArrayRead)
    javaFis.close()
    val javaBsRead = util.BitSet.valueOf(javaByteArrayRead)
    val javaEndTime = System.nanoTime
    val javaTime = (javaEndTime - javaStartTime) / 1000
    assert(javaBsRead == javaBs)

    val sparkStartTime = System.nanoTime
    val sparkFile = path + "sparkBitSet.bin";
    val sparkBos = new ByteArrayOutputStream()
    val sparkFos = new FileOutputStream(sparkFile)
    sparkFos.write(fileHeader)
    val sparkOos = new ObjectOutputStream(sparkBos)
    sparkOos.writeObject(sparkBs)
    sparkFos.write(sparkBos.toByteArray)
    sparkBos.close()
    sparkOos.close()
    sparkFos.close()
    val sparkByteArraySize = sparkBos.toByteArray.size
    val sparkFis = new FileInputStream(sparkFile)
    val sparkHeaderRead = sparkFis.read()
    assert(sparkHeaderRead == fileHeader)
    val sparkByteArrayRead = new Array[Byte](sparkByteArraySize)
    sparkFis.read(sparkByteArrayRead)
    val sparkBis = new ByteArrayInputStream(sparkByteArrayRead)
    val sparkOis = new ObjectInputStream(sparkBis)
    val sparkBsRead = sparkOis.readObject().asInstanceOf[collection.BitSet]
    sparkBis.close()
    sparkOis.close()
    sparkFis.close()
    val sparkEndTime = System.nanoTime
    val sparkTime = (sparkEndTime - sparkStartTime) / 1000

    val rbStartTime = System.nanoTime
    val rbFile = path + "roaringbitmaps.bin";
    rb.runOptimize();
    val rbFos = new FileOutputStream(rbFile)
    rbFos.write(fileHeader)
    val rbBos = new ByteArrayOutputStream()
    val rbDos = new DataOutputStream(rbBos)
    rb.serialize(rbDos)
    rbBos.writeTo(rbFos)
    rbBos.close()
    rbDos.close()
    rbFos.close();
    val rbFis = new FileInputStream(rbFile)
    val rbHeaderRead = rbFis.read()
    assert(rbHeaderRead == fileHeader)
    val rbByteArrayRead = new Array[Byte](rbBos.size)
    rbFis.read(rbByteArrayRead)
    val rbBis = new ByteArrayInputStream(rbByteArrayRead)
    val rbDis = new DataInputStream(rbBis);
    val rbRead = new RoaringBitmap();
    rbRead.deserialize(rbDis);
    rbBis.close()
    rbDis.close()
    rbFis.close();
    val rbEndTime = System.nanoTime
    val rbTime = (rbEndTime - rbStartTime) / 1000
    if(!rbRead.equals(rb)) throw new RuntimeException("rb r/w is not equal!");

    /* The result is below. The unit is us. The configuration is the same as the above.
     * spark r/w time is 57412.
     * scala r/w time is 85720.
     * java r/w time is 94570.
     * rb r/w time is 321.
     */
  }

  test("test the bitmap index file size with BitSet and Roaring Bitmap") {
    val data: Seq[(Int, String)] = (1 to 30000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex index_bm on oap_test (a) USING BITMAP")
    val fileNameIterator = dir.listFiles()
    for (fileName <- fileNameIterator) {
      if (fileName.toString().endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
        val fileSize = fileName.length
      }
    }
    val startTime = System.nanoTime
    checkAnswer(sql("SELECT * FROM oap_test WHERE a = 15000"),
      Row(15000, "this is test 15000") :: Nil)
    val endTime = System.nanoTime
    // The unit is ms.
    val elapsedTime = (endTime - startTime) / 1000000
    sql("drop oindex index_bm on oap_test")
    // The microbenchmark result will be added after the bitmap partial loading patch is merged.
  }

}
