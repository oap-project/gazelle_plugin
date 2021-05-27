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

package org.apache.spark.sql.execution.datasources.v2.arrow

import scala.collection.JavaConverters._

import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.TypeLayout
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import org.apache.spark.sql.vectorized.ColumnarBatch

object SparkVectorUtils {

  def estimateSize(columnarBatch: ColumnarBatch): Long = {
    val cols = (0 until columnarBatch.numCols).toList.map(i =>
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector())
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    cols.foreach(vector => {
      appendNodes(vector.asInstanceOf[FieldVector], nodes, buffers);
    })
    buffers.asScala.map(_.getPossibleMemoryConsumed()).sum
  }

  def toArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val numRowsInBatch = columnarBatch.numRows()
    val cols = (0 until columnarBatch.numCols).toList.map(i =>
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector)
    toArrowRecordBatch(numRowsInBatch, cols)
  }

  def toArrowRecordBatch(numRows: Int,
      cols: List[ValueVector]): ArrowRecordBatch = {
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    cols.foreach(vector => {
      appendNodes(vector.asInstanceOf[FieldVector], nodes, buffers);
    })
    new ArrowRecordBatch(numRows, nodes, buffers);
  }

  private def appendNodes(
      vector: FieldVector,
      nodes: java.util.List[ArrowFieldNode],
      buffers: java.util.List[ArrowBuf]): Unit = {
    nodes.add(new ArrowFieldNode(vector.getValueCount, vector.getNullCount))
    val fieldBuffers = vector.getFieldBuffers
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField.getType)
    if (fieldBuffers.size != expectedBufferCount) {
      throw new IllegalArgumentException(
        s"Wrong number of buffers for field ${vector.getField} in vector " +
            s"${vector.getClass.getSimpleName}. found: ${fieldBuffers}")
    }
    buffers.addAll(fieldBuffers)
    vector.getChildrenFromFields.asScala.foreach(child => appendNodes(child, nodes, buffers))
  }
}
