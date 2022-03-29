// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.netty.channel.Channel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client-side Netty remote connection.
 */
final class RemoteClientConnection extends AbstractClientConnection {
  private final Channel channel;

  RemoteClientConnection(ScheduledExecutorService executorService, Channel channel) {
    super(executorService);
    this.channel = channel;
  }

  @Override
  public CompletableFuture<Void> sendAsync(ProtocolRequest message) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    channel.writeAndFlush(message).addListener(channelFuture -> {
      if (!channelFuture.isSuccess()) {
        future.completeExceptionally(channelFuture.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(ProtocolRequest message, Duration timeout) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    Callback callback = new Callback(message.id(), message.subject(), timeout, future);
    channel.writeAndFlush(message).addListener(channelFuture -> {
      if (!channelFuture.isSuccess()) {
        callback.completeExceptionally(channelFuture.cause());
      }
    });
    return future;
  }
}
