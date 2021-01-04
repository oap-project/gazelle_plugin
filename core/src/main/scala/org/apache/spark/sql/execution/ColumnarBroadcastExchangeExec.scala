package org.apache.spark.sql.execution

import com.google.common.collect.Lists
import com.intel.oap.expression._
import com.intel.oap.vectorized.{ArrowWritableColumnVector, BatchIterator, ExpressionEvaluator}
import io.netty.buffer.{ByteBuf, ByteBufAllocator, ByteBufOutputStream}
import java.io.{ObjectOutputStream, OutputStream}
import java.nio.ByteBuffer

import scala.concurrent.duration.NANOSECONDS
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.execution.{ColumnarHashedRelation, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext
import org.apache.spark.util.{SparkFatalException, ThreadUtils}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
    extends BroadcastExchangeExec(mode, child) {

  override def supportsColumnar = true
  override def output: Seq[Attribute] = child.output

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numRows" -> SQLMetrics.createMetric(sparkContext, "number of Rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_broadcastExchange"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  val buildKeyExprs: Seq[Expression] = mode match {
    case hashRelationMode: HashedRelationBroadcastMode =>
      hashRelationMode.key
    case _ =>
      throw new UnsupportedOperationException(
        s"ColumnarBroadcastExchange only support HashRelationMode")
  }
  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  private[sql] override lazy val relationFuture
      : java.util.concurrent.Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      sqlContext.sparkSession,
      BroadcastExchangeExec.executionContext) {
      var relation: Any = null
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val beforeCollect = System.nanoTime()

        ///////////////////// Collect Raw RecordBatches from all executors /////////////////
        val countsAndBytes = child
          .executeColumnar()
          .mapPartitions { iter =>
            var _numRows: Long = 0
            val _input = new ArrayBuffer[ColumnarBatch]()

            while (iter.hasNext) {
              val batch = iter.next
              (0 until batch.numCols).foreach(i =>
                batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              _numRows += batch.numRows
              _input += batch
            }
            val bytes = ConverterUtils.convertToNetty(_input.toArray)
            _input.foreach(_.close)

            Iterator((_numRows, bytes))
          }
          .collect
        ///////////////////////////////////////////////////////////////////////////
        val input = countsAndBytes.map(_._2)
        val size_raw = input.map(_.length).sum
        val hash_relation_schema = ConverterUtils.toArrowSchema(output)

        ///////////// After collect data to driver side, build hashmap here /////////////
        val beforeBuild = System.nanoTime()
        val hash_relation_function =
          ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, output, 1, true)
        val hash_relation_expr =
          TreeBuilder.makeExpression(
            hash_relation_function,
            Field.nullable("result", new ArrowType.Int(32, true)))
        val hashRelationKernel = new ExpressionEvaluator()
        hashRelationKernel.build(
          hash_relation_schema,
          Lists.newArrayList(hash_relation_expr),
          true
          )
        val iter = ConverterUtils.convertFromNetty(output, input)
        var numRows: Long = 0
        val _input = new ArrayBuffer[ColumnarBatch]()
        while (iter.hasNext) {
          val batch = iter.next
          if (batch.numRows > 0) {
            (0 until batch.numCols).foreach(i =>
              batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
            _input += batch
            numRows += batch.numRows
            val dep_rb = ConverterUtils.createArrowRecordBatch(batch)
            hashRelationKernel.evaluate(dep_rb)
            ConverterUtils.releaseArrowRecordBatch(dep_rb)
          }
        }
        val hashRelationResultIterator = hashRelationKernel.finishByIterator()

        val hashRelationObj = hashRelationResultIterator.nextHashRelationObject()
        relation =
          new ColumnarHashedRelation(hashRelationObj, _input.toArray, size_raw).asReadOnlyCopy
        val dataSize = relation.asInstanceOf[ColumnarHashedRelation].size

        longMetric("buildTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

        /////////////////////////////////////////////////////////////////////////////

        if (numRows >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_ROWS) {
          throw new SparkException(
            s"Cannot broadcast the table over ${BroadcastExchangeExec.MAX_BROADCAST_TABLE_ROWS} rows: $numRows rows")
        }

        longMetric("collectTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeCollect)

        longMetric("numRows") += numRows
        longMetric("dataSize") += dataSize
        if (dataSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
          throw new SparkException(
            s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
        }

        val beforeBroadcast = System.nanoTime()

        // Broadcast the relation
        val broadcasted = sparkContext.broadcast(relation)
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)
        longMetric("totalTime").merge(longMetric("collectTime"))
        longMetric("totalTime").merge(longMetric("broadcastTime"))
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.success(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            new OutOfMemoryError(
              "Not enough memory to build and broadcast the table to all " +
                "worker nodes. As a workaround, you can either disable broadcast by setting " +
                s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause))
          promise.failure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.failure(ex)
          throw ex
        case e: Throwable =>
          promise.failure(e)
          throw e
      } finally {
        val timeout: Int = SQLConf.get.broadcastTimeout.toInt
        relation.asInstanceOf[ColumnarHashedRelation].countDownClose(timeout)
      }
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarBroadcastExchangeExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarBroadcastExchangeExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

}
