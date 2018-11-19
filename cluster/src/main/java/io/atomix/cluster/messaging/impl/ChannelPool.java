/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.utils.net.Address;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Internal Netty channel pool.
 */
class ChannelPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelPool.class);

  private final Function<Address, CompletableFuture<Channel>> factory;
  private final int size;
  private final Map<Address, List<CompletableFuture<Channel>>> channels = Maps.newConcurrentMap();

  ChannelPool(Function<Address, CompletableFuture<Channel>> factory, int size) {
    this.factory = factory;
    this.size = size;
  }

  /**
   * Returns the channel pool for the given address.
   *
   * @param address the address for which to return the channel pool
   * @return the channel pool for the given address
   */
  private List<CompletableFuture<Channel>> getChannelPool(Address address) {
    List<CompletableFuture<Channel>> channelPool = channels.get(address);
    if (channelPool != null) {
      return channelPool;
    }
    return channels.computeIfAbsent(address, e -> {
      List<CompletableFuture<Channel>> defaultList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        defaultList.add(null);
      }
      return Lists.newCopyOnWriteArrayList(defaultList);
    });
  }

  /**
   * Returns the channel offset for the given message type.
   *
   * @param messageType the message type for which to return the channel offset
   * @return the channel offset for the given message type
   */
  private int getChannelOffset(String messageType) {
    return Math.abs(messageType.hashCode() % size);
  }

  /**
   * Gets or creates a pooled channel to the given address for the given message type.
   *
   * @param address     the address for which to get the channel
   * @param messageType the message type for which to get the channel
   * @return a future to be completed with a channel from the pool
   */
  CompletableFuture<Channel> getChannel(Address address, String messageType) {
    List<CompletableFuture<Channel>> channelPool = getChannelPool(address);
    int offset = getChannelOffset(messageType);

    CompletableFuture<Channel> channelFuture = channelPool.get(offset);
    if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
      synchronized (channelPool) {
        channelFuture = channelPool.get(offset);
        if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
          LOGGER.debug("Connecting to {}", address);
          channelFuture = factory.apply(address);
          channelFuture.whenComplete((channel, error) -> {
            if (error == null) {
              LOGGER.debug("Connected to {}", channel.remoteAddress());
            } else {
              LOGGER.debug("Failed to connect to {}", address, error);
            }
          });
          channelPool.set(offset, channelFuture);
        }
      }
    }

    final CompletableFuture<Channel> future = new CompletableFuture<>();
    final CompletableFuture<Channel> finalFuture = channelFuture;
    finalFuture.whenComplete((channel, error) -> {
      if (error == null) {
        if (!channel.isActive()) {
          CompletableFuture<Channel> currentFuture;
          synchronized (channelPool) {
            currentFuture = channelPool.get(offset);
            if (currentFuture == finalFuture) {
              channelPool.set(offset, null);
            } else if (currentFuture == null) {
              currentFuture = factory.apply(address);
              currentFuture.whenComplete((c, e) -> {
                if (e == null) {
                  LOGGER.debug("Connected to {}", channel.remoteAddress());
                } else {
                  LOGGER.debug("Failed to connect to {}", channel.remoteAddress(), e);
                }
              });
              channelPool.set(offset, currentFuture);
            }
          }

          if (currentFuture == finalFuture) {
            getChannel(address, messageType).whenComplete((recursiveResult, recursiveError) -> {
              if (recursiveError == null) {
                future.complete(recursiveResult);
              } else {
                future.completeExceptionally(recursiveError);
              }
            });
          } else {
            currentFuture.whenComplete((recursiveResult, recursiveError) -> {
              if (recursiveError == null) {
                future.complete(recursiveResult);
              } else {
                future.completeExceptionally(recursiveError);
              }
            });
          }
        } else {
          future.complete(channel);
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }
}
