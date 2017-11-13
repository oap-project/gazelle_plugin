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

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.{IndexMeta, TrieIndex}
import org.apache.spark.sql.execution.datasources.oap.filecache.CacheResult
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

class PermutermScannerSuite extends SparkFunSuite {
  private def testTrie(): InMemoryTrie = {
    val row1 = UTF8String.fromString("Alpha")
    val row2 = UTF8String.fromString("AlphaHello")
    val row3 = UTF8String.fromString("Alphabeta")
    val row4 = UTF8String.fromString("Beta")
    val row5 = UTF8String.fromString("Zero")

    val uniqueList = new java.util.LinkedList[UTF8String]()
    val offsetMap = new java.util.HashMap[UTF8String, Int]()

    val list = List(row1, row2, row3, row4, row5)
    list.foreach(uniqueList.add)
    list.zipWithIndex.foreach(i => offsetMap.put(i._1, 8 * (i._2 + 1)))
    val trie = InMemoryTrie()
    PermutermUtils.generatePermuterm(uniqueList, offsetMap, trie)
    trie
  }

  class TestPermutermScanner
      extends PermutermScanner(new IndexMeta("index1", "ABC", TrieIndex(0))) {
    private var unsafeTrie: UnsafeTrie = _
    def allPointersString: String = allPointers.mkString(",")
    override def initialize(path: Path, configuration: Configuration): TestPermutermScanner = {
      val fs = path.getFileSystem(configuration)
      val fileSize = fs.getFileStatus(path).getLen
      val buffer = new Array[Byte](fileSize.toInt)
      fs.open(path).readFully(0, buffer)
      val cbbos = new ChunkedByteBufferOutputStream(buffer.length, ByteBuffer.allocate)
      cbbos.write(buffer)
      cbbos.close()
      val baseOffset = Platform.BYTE_ARRAY_OFFSET
      val dataEnd = Platform.getInt(buffer, baseOffset + fileSize - 8)
      val rootOffset = Platform.getInt(buffer, baseOffset + fileSize - 16)
      val cbb = cbbos.toChunkedByteBuffer
      permutermDataCache = new CacheResult(true, cbb)
      unsafeTrie = UnsafeTrie(cbb, rootOffset, dataEnd, _ => cbb)
      this
    }
    def scan(): Unit = {
      _init(unsafeTrie)
    }
  }

  private def testScanUnsafeTrie(): TestPermutermScanner = {
    val row1 = UTF8String.fromString("Alpha")
    val row2 = UTF8String.fromString("AlphaHello")
    val row3 = UTF8String.fromString("Alphabeta")
    val row4 = UTF8String.fromString("Beta")
    val row5 = UTF8String.fromString("Zero")
    val list = List(row1, row2, row3, row4, row5)
    val path = new Path(Utils.createTempDir().getAbsolutePath, "index")
    val configuration = new Configuration()
    val fs = path.getFileSystem(configuration)
    val fileWriter = fs.create(path, true)
    val indexWriter = new PermutermIndexRecordWriter(
      configuration, fileWriter, new StructType().add("s", StringType))

    list.foreach(s => indexWriter.write(null, InternalRow(s)))
    indexWriter.flushToFile()
    fileWriter.close()

    val scanner = new TestPermutermScanner()
    scanner.initialize(path, configuration)
  }

  test("scan in memory permuterm trie") {
    val trie = testTrie()
    val scanner = new PermutermScanner(new IndexMeta("index1", "ABC", TrieIndex(0)))
    scanner.pattern = UTF8String.fromString("Alpha\u0003").getBytes
    scanner._init(trie)
    assert(scanner.matchRoot != null)
  }

  test("scan unsafe permuterm trie #1") {
    val scanner = testScanUnsafeTrie()
    scanner.pattern = UTF8String.fromString("Alpha\u0003").getBytes
    scanner.scan()
    assert(scanner.matchRoot != null)
    assert(scanner.allPointersString.equals("8"))
  }

  test("scan unsafe permuterm trie #2") {
    val scanner = testScanUnsafeTrie()
    scanner.pattern = UTF8String.fromString("ALPHA\u0003").getBytes
    scanner.scan()
    assert(scanner.matchRoot == null)
  }

  test("scan unsafe permuterm trie #3") {
    val scanner = testScanUnsafeTrie()
    scanner.pattern = UTF8String.fromString("Alpha").getBytes
    scanner.scan()
    assert(scanner.matchRoot != null)
    assert(scanner.allPointersString.equals("8,16,24"))
  }

  test("scan unsafe permuterm trie #4") {
    val scanner = testScanUnsafeTrie()
    scanner.pattern = UTF8String.fromString("eta\u0003").getBytes
    scanner.scan()
    assert(scanner.matchRoot != null)
    assert(scanner.allPointersString.equals("24,32"))
  }

  test("scan unsafe permuterm trie #5") {
    val scanner = testScanUnsafeTrie()
    // A%eta
    scanner.pattern = UTF8String.fromString("eta\u0003A").getBytes
    scanner.scan()
    assert(scanner.matchRoot != null)
    assert(scanner.allPointersString.equals("24"))
  }

  test("scan unsafe permuterm trie #6") {
    val scanner = testScanUnsafeTrie()
    // %ll%
    scanner.pattern = UTF8String.fromString("ll").getBytes
    scanner.scan()
    assert(scanner.matchRoot != null)
    assert(scanner.allPointersString.equals("16"))
  }
}
