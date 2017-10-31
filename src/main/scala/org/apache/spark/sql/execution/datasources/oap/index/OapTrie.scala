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

import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.collection.mutable.ArrayBuffer

// Trie Node
private[oap] trait TrieNode {
  def nodeKey: Byte
  def childCount: Int
  def childAt(idx: Int): TrieNode
  def children: Seq[TrieNode]
  def canTerminate: Boolean
  def rowIdsPointer: Int
  def allPointers: Seq[Int]
  // for debug
  override def toString: String = {
    def disp(key: Byte): String =
      if (key >= 32 && key <= 126) key.toChar.toString else "\\" + key.toLong.toHexString
    if (childCount > 0) {
      s"[Trie(${disp(nodeKey)},$rowIdsPointer) ${children.mkString(" ")}]"
    } else {
      s"Trie(${disp(nodeKey)},$rowIdsPointer)"
    }
  }
}

case class TriePointer(page: Int, offset: Int)

private[oap] case class InMemoryTrie(
    nodeKey: Byte = 0,
    var rowIdsPointer: Int = -1) extends TrieNode {
  val innerChildren: ArrayBuffer[TrieNode] = new ArrayBuffer()
  def childAt(idx: Int): TrieNode = children(idx)
  def allPointers: Seq[Int] = Nil
  def addChild(node: TrieNode): InMemoryTrie = {
    innerChildren += node
    this
  }
  def children: Seq[TrieNode] = {
    innerChildren.sortBy(_.nodeKey)
  }
  def setPointer(value: Int): InMemoryTrie = {
    rowIdsPointer = value
    this
  }
  def canTerminate: Boolean = rowIdsPointer >= 0
  def childCount: Int = innerChildren.size
}
