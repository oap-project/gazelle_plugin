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

package org.apache.spark.sql.execution.datasources.oap.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.InMemoryTrie
import org.apache.spark.unsafe.types.UTF8String

private[oap] object PermutermUtils extends Logging {
  def generatePermuterm(
      uniqueList: java.util.LinkedList[InternalRow],
      offsetMap: java.util.HashMap[InternalRow, Int],
      root: InMemoryTrie = InMemoryTrie()): Long = {
    val it = uniqueList.iterator()
    var count = 0L
    while (it.hasNext) {
      val row = it.next()
      count += addWordToPermutermTree(row, root, offsetMap)
    }
    count
  }

  private def addWordToPermutermTree(
      row: InternalRow,
      root: InMemoryTrie,
      offsetMap: java.util.HashMap[InternalRow, Int]): Int = {
    val utf8String = row.getUTF8String(0)
    val bytes = utf8String.getBytes
    assert(offsetMap.containsKey(row))
    val endMark = UTF8String.fromString("\3").getBytes
    val offset = offsetMap.get(row)
    // including "\3abc" and "abc\3" and "bc\3a" and "c\3ab"
    (0 to bytes.length).map(i => {
      val token = bytes.slice(i, bytes.length) ++ endMark ++ bytes.slice(0, i)
      addArrayByteToTrie(token, offset, root)
    }).sum
  }

  private def addArrayByteToTrie(
      bytes: Seq[Byte], offset: Int, root: InMemoryTrie): Int = {
    bytes match {
      case Nil =>
        root.setPointer(offset)
        0
      case letter +: Nil =>
        assert(root.children.forall(c => c.nodeKey != letter || !c.canTerminate))
        root.children.find(_.nodeKey == letter) match {
          case Some(tn: InMemoryTrie) =>
            tn.setPointer(offset)
            0
          case _ =>
            root.addChild(InMemoryTrie(letter, offset))
            1
        }
      case letter +: tail =>
        root.children.find(_.nodeKey == letter) match {
          case Some(tn: InMemoryTrie) =>
            addArrayByteToTrie(tail, offset, tn)
          case _ =>
            val parent = InMemoryTrie(letter)
            root.addChild(parent)
            addArrayByteToTrie(tail, offset, parent) + 1
        }
    }
  }
}
