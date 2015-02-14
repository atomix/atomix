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
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolServer;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolServer implements ProtocolServer {
  private final Vertx vertx;
  private final String host;
  private final int port;
  private final NetServer server;
  private EventListener<ProtocolConnection> listener;

  public VertxTcpProtocolServer(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.vertx = protocol.getVertx() != null ? protocol.getVertx() : Vertx.vertx();
    NetServerOptions options = new NetServerOptions()
      .setTcpKeepAlive(true)
      .setTcpNoDelay(true)
      .setReuseAddress(true)
      .setAcceptBacklog(protocol.getAcceptBacklog())
      .setSendBufferSize(protocol.getSendBufferSize())
      .setReceiveBufferSize(protocol.getReceiveBufferSize())
      .setSsl(protocol.isSsl())
      .setClientAuthRequired(protocol.isClientAuthRequired())
      .setUsePooledBuffers(true);
    this.server = vertx.createNetServer(options);
  }

  @Override
  public ProtocolServer connectListener(EventListener<ProtocolConnection> listener) {
    this.listener = listener;
    return this;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    server.connectHandler(socket -> {
      if (listener != null) {
        listener.accept(new VertxTcpProtocolConnection(vertx, socket));
      }
    }).listen(port, host, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (server != null) {
      server.close(result -> {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      });
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
