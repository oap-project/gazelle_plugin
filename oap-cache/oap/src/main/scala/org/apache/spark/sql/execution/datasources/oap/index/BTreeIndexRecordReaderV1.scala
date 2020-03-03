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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.utils.NonNullKeyReader
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types._

private[index] case class BTreeIndexRecordReaderV1(
    configuration: Configuration,
    schema: StructType,
    fileReader: IndexFileReader)
  extends BTreeIndexRecordReader(configuration, schema, fileReader) {

  private val META_SIZE = FOOTER_LENGTH_SIZE + ROW_ID_LIST_LENGTH_SIZE

  protected var footer: BTreeFooter = _
  protected var meta: BTreeMeta = _

  protected[index] val rowIdListSizePerSection: Int =
    configuration.getInt(OapConf.OAP_BTREE_ROW_LIST_PART_SIZE.key, 1024 * 1024)

  protected[index] def initializeReader(): Unit = {
    val sectionLengthIndex = fileReader.getLen - FOOTER_LENGTH_SIZE - ROW_ID_LIST_LENGTH_SIZE
    val sectionLengthBuffer = new Array[Byte](FOOTER_LENGTH_SIZE + ROW_ID_LIST_LENGTH_SIZE)
    fileReader.readFully(sectionLengthIndex, sectionLengthBuffer)
    val rowIdListSize = getLongFromBuffer(sectionLengthBuffer, 0)
    val footerSize = getIntFromBuffer(sectionLengthBuffer, ROW_ID_LIST_LENGTH_SIZE)
    meta = BTreeMetaImpl(fileReader.getLen, footerSize, rowIdListSize)
    footer = readBTreeFooter()
  }

  override protected def readData(position: Long, length: Int): Array[Byte] = {
    assert(length <= Int.MaxValue, "Try to read too large index data")
    fileReader.read(position, length)
  }

  protected[index] def readBTreeFooter(): BTreeFooter = {
    val fiberCache = getBTreeFiberCache(meta.footerOffset, meta.footerLength, footerSectionId, 0)
    update(footerSectionId, fiberCache)
    BTreeFooterImpl(fiberCache, schema)
  }

  protected[index] def readBTreeRowIdList(footer: BTreeFooter, partIdx: Int): BTreeRowIdList = {
    val partSize = rowIdListSizePerSection.toLong * IndexUtils.INT_SIZE
    val readLength = if (partIdx * partSize + partSize > meta.rowIdListLength) {
      meta.rowIdListLength % partSize
    } else {
      partSize
    }
    val fiberCache = getBTreeFiberCache(
      meta.rowIdListOffset + partIdx * partSize,
      readLength.toInt,
      rowIdListSectionId, partIdx)

    update(rowIdListSectionId, fiberCache)
    BTreeRowIdListImpl(fiberCache)
  }


  protected[index] def readBTreeNodeData(footer: BTreeFooter, nodeIdx: Int): BTreeNode = {
    val offset = footer.getNodeOffset(nodeIdx)
    val length = footer.getNodeSize(nodeIdx)
    val fiberCache = getBTreeFiberCache(meta.nodeOffset + offset, length, nodeSectionId, nodeIdx)

    update(nodeSectionId, fiberCache)
    BTreeNodeImpl(fiberCache, schema)
  }

  private[index] case class BTreeMetaImpl(
      fileLength: Long,
      footerLength: Int,
      rowIdListLength: Long) extends BTreeMeta {
    def footerOffset: Long = fileLength - META_SIZE - footerLength
    def rowIdListOffset: Long = footerOffset - rowIdListLength
    def nodeOffset: Long = IndexFile.VERSION_LENGTH
  }

  private[index] case class BTreeFooterImpl(fiberCache: FiberCache, schema: StructType)
      extends BTreeFooter {
    // TODO move to companion object
    private val nodePosOffset = IndexUtils.INT_SIZE
    private val nodeSizeOffset = IndexUtils.INT_SIZE * 2
    private val minPosOffset = IndexUtils.INT_SIZE * 3
    private val maxPosOffset = IndexUtils.INT_SIZE * 4
    private val nodeMetaStart = IndexUtils.INT_SIZE * 4
    private val nodeMetaByteSize = IndexUtils.INT_SIZE * 5
    private val statsLengthSize = IndexUtils.INT_SIZE

    @transient protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(schema)

    def getVersionNum: Int = fiberCache.getInt(0)

    def getNonNullKeyRecordCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE)

    def getNullKeyRecordCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE * 2)

    def getNodesCount: Int = fiberCache.getInt(IndexUtils.INT_SIZE * 3)

    // get idx Node's max value
    def getMaxValue(idx: Int, schema: StructType): InternalRow =
      nnkr.readKey(fiberCache, getMaxValueOffset(idx))._1

    def getMinValue(idx: Int, schema: StructType): InternalRow =
      nnkr.readKey(fiberCache, getMinValueOffset(idx))._1

    def getMinValueOffset(idx: Int): Int = {
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + minPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    }

    def getMaxValueOffset(idx: Int): Int = {
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize + getStatsLength
    }

    def getRowCountOfNode(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize)

    def getNodeOffset(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)

    def getNodeSize(idx: Int): Int =
      fiberCache.getInt(nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)

    def getStatsOffset: Int = nodeMetaStart + nodeMetaByteSize * getNodesCount + statsLengthSize

    private def getStatsLength: Int =
      fiberCache.getInt(nodeMetaStart + nodeMetaByteSize * getNodesCount)

    def getRowIdListPartOffset(idx: Int): Int = throw new UnsupportedOperationException()

    def getRowIdListPartSize(idx: Int): Int = throw new UnsupportedOperationException()

    def getRowIdListPartSizePerSection: Int = throw new UnsupportedOperationException()
  }

  private[index] case class BTreeRowIdListImpl(fiberCache: FiberCache) extends BTreeRowIdList {
    def getRowId(idx: Int): Int = fiberCache.getInt(idx * IndexUtils.INT_SIZE)
  }

  private[index] case class BTreeNodeImpl(fiberCache: FiberCache, schema: StructType)
      extends BTreeNode {
    private val posSectionStart = IndexUtils.INT_SIZE
    private val posEntrySize = IndexUtils.INT_SIZE * 2
    private def valueSectionStart = posSectionStart + getKeyCount * posEntrySize

    @transient
    protected lazy val nnkr: NonNullKeyReader = new NonNullKeyReader(schema)

    def getKeyCount: Int = fiberCache.getInt(0)

    def getKey(idx: Int, schema: StructType): InternalRow = {
      val offset = valueSectionStart +
          fiberCache.getInt(posSectionStart + idx * posEntrySize)
      nnkr.readKey(fiberCache, offset)._1
    }

    def getRowIdPos(idx: Int): Int =
      fiberCache.getInt(posSectionStart + idx * posEntrySize + IndexUtils.INT_SIZE)
  }
}
