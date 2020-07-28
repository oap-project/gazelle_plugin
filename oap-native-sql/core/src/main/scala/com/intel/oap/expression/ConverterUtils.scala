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

package com.intel.oap.expression

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import com.intel.oap.vectorized.ArrowWritableColumnVector
import io.netty.buffer.ArrowBuf
import org.apache.spark.rdd.RDD
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{
  ArrowFieldNode,
  ArrowRecordBatch,
  MessageSerializer,
  IpcOption
}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import io.netty.buffer.{ByteBuf, ByteBufAllocator, ByteBufOutputStream}
import java.nio.channels.{Channels, WritableByteChannel}

object ConverterUtils extends Logging {
  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val numRowsInBatch = columnarBatch.numRows()
    val cols = (0 until columnarBatch.numCols).toList.map(i =>
      columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector())
    createArrowRecordBatch(numRowsInBatch, cols)

    /*val fieldNodes = new ListBuffer[ArrowFieldNode]()
    val inputData = new ListBuffer[ArrowBuf]()
    for (i <- 0 until columnarBatch.numCols()) {
      val inputVector =
        columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector()
      fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      //FIXME for projection + in test
      //fieldNodes += new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount())
      inputData += inputVector.getValidityBuffer()
      if (inputVector.isInstanceOf[VarCharVector]) {
        inputData += inputVector.getOffsetBuffer()
      }
      inputData += inputVector.getDataBuffer()
      //FIXME for projection + in test
      //inputData += inputVector.getValidityBuffer()
    }
    new ArrowRecordBatch(numRowsInBatch, fieldNodes.toList.asJava, inputData.toList.asJava)*/
  }

  def createArrowRecordBatch(numRowsInBatch: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    cols.foreach(vector => {
      appendNodes(vector.asInstanceOf[FieldVector], nodes, buffers);
    })
    new ArrowRecordBatch(numRowsInBatch, nodes, buffers);
  }

  def appendNodes(
      vector: FieldVector,
      nodes: java.util.List[ArrowFieldNode],
      buffers: java.util.List[ArrowBuf]): Unit = {
    nodes.add(new ArrowFieldNode(vector.getValueCount, vector.getNullCount))
    val fieldBuffers = vector.getFieldBuffers
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType())
    if (fieldBuffers.size != expectedBufferCount) {
      throw new IllegalArgumentException(
        s"wrong number of buffers for field ${vector.getField} in vector ${vector.getClass.getSimpleName}. found: ${fieldBuffers}")
    }
    buffers.addAll(fieldBuffers)
    vector.getChildrenFromFields.asScala.foreach(child => appendNodes(child, nodes, buffers))
  }

  def convertToNetty(iter: Array[ColumnarBatch]): Array[Byte] = {
    val out = new ByteBufOutputStream(ByteBufAllocator.DEFAULT.buffer());
    val channel = new WriteChannel(Channels.newChannel(out))
    var schema: Schema = null
    val option = new IpcOption

    iter.foreach { columnarBatch =>
      val vectors = (0 until columnarBatch.numCols)
        .map(i => columnarBatch.column(i).asInstanceOf[ArrowWritableColumnVector])
        .toList
      if (schema == null) {
        schema = new Schema(vectors.map(_.getValueVector().getField).asJava)
        MessageSerializer.serialize(channel, schema, option)
      }
      MessageSerializer.serialize(
        channel,
        ConverterUtils
          .createArrowRecordBatch(columnarBatch.numRows, vectors.map(_.getValueVector)),
        option)
    }
    val buf = out.buffer
    val bytes = new Array[Byte](buf.readableBytes);
    buf.getBytes(buf.readerIndex, bytes);
    bytes
  }

  def convertFromNetty(data: Array[Array[Byte]]): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var array_id = 0
      var incorrectInput = false
      var input = new ByteArrayInputStream(data(array_id))
      var reader = new ArrowStreamReader(input, ArrowWritableColumnVector.getNewAllocator)
      var root = reader.getVectorSchemaRoot()

      override def hasNext: Boolean =
        (array_id < (data.size - 1) || input.available > 0) && (!incorrectInput)
      override def next(): ColumnarBatch = {
        try {
          if (input.available == 0) {
            array_id += 1
            input = new ByteArrayInputStream(data(array_id))
            reader = new ArrowStreamReader(input, ArrowWritableColumnVector.getNewAllocator)
            root = reader.getVectorSchemaRoot()
          }
          reader.loadNextBatch();
          val length = root.getRowCount
          val vectors = root
            .getFieldVectors()
            .asScala
            .zipWithIndex
            .map {
              case (vector, i) => {
                new ArrowWritableColumnVector(vector, i, length, false)
              }
            }
            .toArray[ColumnVector]
          val numCols = vectors.size
          /*logWarning(s"numRows is $length, data is ${(0 until length).map(i =>
          (0 until numCols).map(j =>
            vectors(j).asInstanceOf[ArrowWritableColumnVector].getUTF8String(i)))}")*/
          new ColumnarBatch(vectors, length)
        } catch {
          case e: java.io.IOException =>
            incorrectInput = true
            null
        }
      }
    }
  }

  def fromArrowRecordBatch(
      recordBatchSchema: Schema,
      recordBatch: ArrowRecordBatch): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength()
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch)
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null) {
      recordBatch.close()
    }
  }

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null)
        releaseArrowRecordBatch(recordBatch)
    })
  }

  def getAttrFromExpr(fieldExpr: Expression, skipAlias: Boolean = false): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children(0))
      case a: AttributeReference =>
        a
      case a: Alias =>
        if (skipAlias && a.child.isInstanceOf[AttributeReference])
          getAttrFromExpr(a.child)
        else
          a.toAttribute.asInstanceOf[AttributeReference]
      case a: KnownFloatingPointNormalized =>
        logInfo(s"$a")
        getAttrFromExpr(a.child)
      case a: NormalizeNaNAndZero =>
        getAttrFromExpr(a.child)
      case c: Coalesce =>
        getAttrFromExpr(c.children(0))
      case i: IsNull =>
        getAttrFromExpr(i.child)
      case a: Add =>
        getAttrFromExpr(a.left)
      case s: Subtract =>
        getAttrFromExpr(s.left)
      case u: Upper =>
        getAttrFromExpr(u.child)
      case ss: Substring =>
        getAttrFromExpr(ss.children(0))
      case other =>
        throw new UnsupportedOperationException(
          s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def getResultAttrFromExpr(
      fieldExpr: Expression,
      name: String = "None",
      dataType: Option[DataType] = None): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getResultAttrFromExpr(a.child, name, Some(a.dataType))
      case a: AttributeReference =>
        if (name != "None") {
          new AttributeReference(name, a.dataType, a.nullable)()
        } else {
          a
        }
      case a: Alias =>
        //TODO: a walkaround since we didn't support cast yet
        if (a.child.isInstanceOf[Cast]) {
          val tmp = if (name != "None") {
            new Alias(a.child.asInstanceOf[Cast].child, name)(
              a.exprId,
              a.qualifier,
              a.explicitMetadata)
          } else {
            new Alias(a.child.asInstanceOf[Cast].child, a.name)(
              a.exprId,
              a.qualifier,
              a.explicitMetadata)
          }
          tmp.toAttribute.asInstanceOf[AttributeReference]
        } else {
          if (name != "None") {
            val tmp = a.toAttribute.asInstanceOf[AttributeReference]
            new AttributeReference(name, tmp.dataType, tmp.nullable)()
          } else {
            a.toAttribute.asInstanceOf[AttributeReference]
          }
        }
      case d: ColumnarDivide =>
        new AttributeReference(name, DoubleType, d.nullable)()
      case m: ColumnarMultiply =>
        new AttributeReference(name, m.dataType, m.nullable)()
      case other =>
        val a = if (name != "None") {
          new Alias(other, name)()
        } else {
          new Alias(other, "res")()
        }
        val tmpAttr = a.toAttribute.asInstanceOf[AttributeReference]
        if (dataType.isDefined) {
          new AttributeReference(tmpAttr.name, dataType.getOrElse(null), tmpAttr.nullable)()
        } else {
          tmpAttr
        }
    }
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- 0 until left.size) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
    val numRows = rb1.getLength()
    val rb1_nodes = rb1.getNodes()
    val rb2_nodes = rb2.getNodes()
    val rb1_bufferlist = rb1.getBuffers()
    val rb2_bufferlist = rb2.getBuffers()

    val combined_nodes = rb1_nodes.addAll(rb2_nodes)
    val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
    new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    def fromAttributes(attributes: Seq[Attribute]): StructType =
      StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    ArrowUtils.toArrowSchema(fromAttributes(attributes), SQLConf.get.sessionLocalTimeZone)
  }

  override def toString(): String = {
    s"ConverterUtils"
  }

}
