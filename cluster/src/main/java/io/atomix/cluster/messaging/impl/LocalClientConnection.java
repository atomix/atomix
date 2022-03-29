// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Local client-side connection.
 */
final class LocalClientConnection extends AbstractClientConnection {
  private final LocalServerConnection serverConnection;

  LocalClientConnection(ScheduledExecutorService executorService, HandlerRegistry handlers) {
    super(executorService);
    this.serverConnection = new LocalServerConnection(handlers, this);
  }

  @Override
  public CompletableFuture<Void> sendAsync(ProtocolRequest message) {
    serverConnection.dispatch(message);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(ProtocolRequest message, Duration timeout) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    new Callback(message.id(), message.subject(), timeout, future);
    serverConnection.dispatch(message);
    return future;
  }

  @Override
  public void close() {
    super.close();
    serverConnection.close();
  }
}
