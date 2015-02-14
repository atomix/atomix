/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolClient implements ProtocolClient {
  private final Vertx vertx;
  private final String host;
  private final int port;
  private final NetClient client;

  public VertxTcpProtocolClient(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.vertx = protocol.getVertx() != null ? protocol.getVertx() : Vertx.vertx();

    NetClientOptions options = new NetClientOptions()
      .setTcpKeepAlive(true)
      .setTcpNoDelay(true)
      .setSendBufferSize(protocol.getSendBufferSize())
      .setReceiveBufferSize(protocol.getReceiveBufferSize())
      .setSsl(protocol.isSsl())
      .setTrustAll(protocol.isClientTrustAll())
      .setUsePooledBuffers(true);
    client = vertx.createNetClient(options);
  }

  @Override
  public CompletableFuture<ProtocolConnection> connect() {
    final CompletableFuture<ProtocolConnection> future = new CompletableFuture<>();

    client.connect(port, host, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(new VertxTcpProtocolConnection(vertx, result.result()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (client != null) {
      client.close();
      future.complete(null);
    } else {
      future.complete(null);
    }
    return future;
  }

  @Override
  public String toString() {
    return String.format("%s[host=%s, port=%d]", getClass().getSimpleName(), host, port);
  }

}
