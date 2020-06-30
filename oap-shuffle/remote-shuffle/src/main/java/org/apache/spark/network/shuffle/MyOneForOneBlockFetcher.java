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

package org.apache.spark.network.shuffle;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;
import org.apache.spark.shuffle.remote.HadoopFileSegmentManagedBuffer;
import org.apache.spark.shuffle.remote.MessageForHadoopManagedBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import scala.Tuple2;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class MyOneForOneBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(MyOneForOneBlockFetcher.class);

  private final TransportClient client;
  private final OpenBlocks openMessage;
  private final String[] blockIds;
  private final BlockFetchingListener listener;

  public MyOneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener) {
    this.client = client;
    this.openMessage = new OpenBlocks(appId, execId, blockIds);
    this.blockIds = blockIds;
    this.listener = listener;
  }

  /**
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }

    client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          boolean isShuffleRequest =
              (response.get(0) == MessageForHadoopManagedBuffers.MAGIC_CODE());
          if (isShuffleRequest) {
            MessageForHadoopManagedBuffers message =
                MessageForHadoopManagedBuffers.fromByteBuffer(Unpooled.wrappedBuffer(response));
            for (Tuple2<String, HadoopFileSegmentManagedBuffer> entry: message.buffers()) {
              listener.onBlockFetchSuccess(entry._1, entry._2);
            }
          } else {
            throw new UnsupportedOperationException("MyNettyOneForOneBlockFetcher got an " +
                "unexpected response, which is not from RemoteShuffle dedicated TransferService");
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }
}
