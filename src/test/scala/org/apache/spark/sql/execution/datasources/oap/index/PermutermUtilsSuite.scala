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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils
import org.apache.spark.unsafe.types.UTF8String

class PermutermUtilsSuite extends SparkFunSuite {
  test("split pages") {
    assert(PermutermUtils.generatePages(10, 3) == Seq(3, 3, 2, 2))
    assert(PermutermUtils.generatePages(3, 3) == Seq(3))
    assert(PermutermUtils.generatePages(3, 4) == Seq(3))
    assert(PermutermUtils.generatePages(8, 4) == Seq(4, 4))
    assert(PermutermUtils.generatePages(8, 3) == Seq(3, 3, 2))
    assert(PermutermUtils.generatePages(7, 3) == Seq(3, 2, 2))
  }

  test("generate permuterm") {
    val row1 = UTF8String.fromString("Alpha")
    val row2 = UTF8String.fromString("Alphabeta")
    val row3 = UTF8String.fromString("AlphaHello")
    val row4 = UTF8String.fromString("Beta")
    val row5 = UTF8String.fromString("Zero")

    val uniqueList = new java.util.LinkedList[UTF8String]()
    val offsetMap = new java.util.HashMap[UTF8String, Int]()

    val list1 = List(row1, row2)
    list1.foreach(uniqueList.add)
    list1.zipWithIndex.foreach(i => offsetMap.put(i._1, i._2))
    val trie1 = InMemoryTrie()
    val trieLength1 = PermutermUtils.generatePermuterm(uniqueList, offsetMap, trie1)
    assert(trieLength1 == 109)
    assert(trie1.toString.equals(
      "[Trie(\\0,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,0) " +
        "[Trie(b,-1) [Trie(e,-1) [Trie(t,-1) Trie(a,1)]]]]]]]]] [Trie(A,-1) [Trie(l,-1) [Trie" +
        "(p,-1) [Trie(h,-1) [Trie(a,-1) Trie(\\3,0) [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(" +
        "a,-1) Trie(\\3,1)]]]]]]]]] [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p," +
        "-1) [Trie(h,0) [Trie(a,-1) [Trie(b,-1) [Trie(e,-1) Trie(t,1)]]]]]]]] [Trie(b,-1) [" +
        "Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) " +
        "Trie(h,1)]]]]]]]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [" +
        "Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) Trie(a,1)]]]]]]]]] [Trie(e,-1) [Trie(t" +
        ",-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(" +
        "a,-1) Trie(b,1)]]]]]]]]] [Trie(h,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1" +
        ") Trie(p,0)]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A," +
        "-1) [Trie(l,-1) Trie(p,1)]]]]]]]]] [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) [" +
        "Trie(\\3,-1) Trie(A,0)] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1)" +
        " Trie(A,1)]]]]]]]]] [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) Trie" +
        "(l,0)]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) Trie" +
        "(l,1)]]]]]]]]] [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p," +
        "-1) [Trie(h,-1) [Trie(a,-1) [Trie(b,-1) Trie(e,1)]]]]]]]]]]"))
    uniqueList.clear()
    offsetMap.clear()

    val list2 = List(row2, row3, row4, row5)
    list2.foreach(uniqueList.add)
    list2.zipWithIndex.foreach(i => offsetMap.put(i._1, i._2))
    val trie2 = InMemoryTrie()
    val trieLength2 = PermutermUtils.generatePermuterm(uniqueList, offsetMap, trie2)
    assert(trieLength2 == 232)
    assert(trie2.toString.equals(
      "[Trie(\\0,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1)" +
        " [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) Trie(o,1)]]]] [Trie(b,-1) [Trie(e,-1)" +
        " [Trie(t,-1) Trie(a,0)]]]]]]]] [Trie(B,-1) [Trie(e,-1) [Trie(t,-1) Trie(a,2)]]] [Trie(" +
        "Z,-1) [Trie(e,-1) [Trie(r,-1) Trie(o,3)]]]] [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(" +
        "h,-1) [Trie(a,-1) [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) [Trie(o,-1) Trie(\\3" +
        ",1)]]]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) Trie(\\3,0)]]]]]]]]] [Trie(B," +
        "-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) Trie(\\3,2)]]]] [Trie(H,-1) [Trie(e,-1) [Trie(" +
        "l,-1) [Trie(l,-1) [Trie(o,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(" +
        "h,-1) Trie(a,1)]]]]]]]]]] [Trie(Z,-1) [Trie(e,-1) [Trie(r,-1) [Trie(o,-1) Trie(\\3,3)]" +
        "]]] [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a," +
        "-1) [Trie(b,-1) [Trie(e,-1) Trie(t,0)]]]]]]] [Trie(B,-1) [Trie(e,-1) Trie(t,2)]]] [" +
        "Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) [Trie(o,-1) [Trie(\\3,-1) [Trie(A,-1) [" +
        "Trie(l,-1) [Trie(p,-1) Trie(h,1)]]]]]]]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a," +
        "-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) Trie(h,0)]]]]]]]]] [Trie(b,-1) [" +
        "Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [" +
        "Trie(h,-1) Trie(a,0)]]]]]]]]] [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) [Trie(o,-1) [Trie(" +
        "\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) Trie(H,1)]]]]]]]]]" +
        " [Trie(r,-1) [Trie(o,-1) [Trie(\\3,-1) Trie(Z,3)]]] [Trie(t,-1) [Trie(a,-1) [Trie(\\3," +
        "-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) Trie(b,0)]]]]] Trie(B," +
        "2)]]]] [Trie(h,-1) [Trie(a,-1) [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) [Trie(o" +
        ",-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) Trie(p,1)]]]]]]]] [Trie(b,-1) [Trie(e,-1) [" +
        "Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) Trie(p,0)]]]]]]]]] [Trie(" +
        "l,-1) [Trie(l,-1) [Trie(o,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [Trie(" +
        "h,-1) [Trie(a,-1) [Trie(H,-1) Trie(e,1)]]]]]]]]] [Trie(o,-1) [Trie(\\3,-1) [Trie(A,-1)" +
        " [Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) [Trie(H,-1) [Trie(e,-1) Trie(l,1)]]]]" +
        "]]]]] [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l," +
        "-1) [Trie(o,-1) [Trie(\\3,-1) Trie(A,1)]]]]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [" +
        "Trie(a,-1) [Trie(\\3,-1) Trie(A,0)]]]]]]]]] [Trie(o,-1) [Trie(\\3,-1) [Trie(A,-1) [" +
        "Trie(l,-1) [Trie(p,-1) [Trie(h,-1) [Trie(a,-1) [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) " +
        "Trie(l,1)]]]]]]]] [Trie(Z,-1) [Trie(e,-1) Trie(r,3)]]]] [Trie(p,-1) [Trie(h,-1) [Trie(" +
        "a,-1) [Trie(H,-1) [Trie(e,-1) [Trie(l,-1) [Trie(l,-1) [Trie(o,-1) [Trie(\\3,-1) [Trie(" +
        "A,-1) Trie(l,1)]]]]]]] [Trie(b,-1) [Trie(e,-1) [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [" +
        "Trie(A,-1) Trie(l,0)]]]]]]]]] [Trie(r,-1) [Trie(o,-1) [Trie(\\3,-1) [Trie(Z,-1) Trie(" +
        "e,3)]]]] [Trie(t,-1) [Trie(a,-1) [Trie(\\3,-1) [Trie(A,-1) [Trie(l,-1) [Trie(p,-1) [" +
        "Trie(h,-1) [Trie(a,-1) [Trie(b,-1) Trie(e,0)]]]]]] [Trie(B,-1) Trie(e,2)]]]]]"))
    uniqueList.clear()
    offsetMap.clear()
  }
}
