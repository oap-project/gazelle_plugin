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

package org.apache.spark.network.netty

import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

import com.codahale.metrics.{Metric, MetricSet}

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.network.{BlockDataManager, BlockTransferService, TransportContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, MyOneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, BlockManagerId, StorageLevel}
import org.apache.spark.util.Utils

// This is to support index cache feature without overriding Spark source code, which may cause
// compatibility issue when upgrading Spark.
// Instead, RemoteShuffle self-maintain a Customized netty transfer service solely for shuffle index
// files transferring
private[spark] class RemoteShuffleTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    bindAddress: String,
    override val hostName: String,
    _port: Int,
    numCores: Int) extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  init(SparkEnv.get.blockManager)

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new MyNettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  val getShuffleServerId: BlockManagerId = {
    val id = SparkEnv.get.blockManager.blockManagerId
    BlockManagerId(id.executorId, id.host, port)
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  override def shuffleMetrics(): MetricSet = {
    require(
      server != null && clientFactory != null, "RemoteShuffleTransferService is not initialized")

    new MetricSet {
      val allMetrics = new JHashMap[String, Metric]()
      override def getMetrics: JMap[String, Metric] = {
        allMetrics.putAll(clientFactory.getAllMetrics.getMetrics)
        allMetrics.putAll(server.getAllMetrics.getMetrics)
        allMetrics
      }
    }
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new MyOneForOneBlockFetcher(client, appId, execId, blockIds, listener).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  // Make this an empty implementation, because remote shuffle only uses the `fetchBlocks` function
  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = ???

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }
}
