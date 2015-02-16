/*
 * Copyright 2014 the original author or authors.
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

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolServer;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.net.NetServer;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolServer implements ProtocolServer {
  private Vertx vertx;
  private final String host;
  private final int port;
  private final VertxTcpProtocol protocol;
  private NetServer server;
  private EventListener<ProtocolConnection> listener;

  public VertxTcpProtocolServer(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  @Override
  public String address() {
    return String.format("tcp://%s:%d", server.host(), server.port());
  }

  @Override
  public ProtocolServer connectListener(EventListener<ProtocolConnection> listener) {
    this.listener = listener;
    return this;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (vertx == null)
      vertx = protocol.getVertx();
    if (vertx == null)
      vertx = new DefaultVertx();

    if (server == null) {
      server = vertx.createNetServer();
      server.setTCPKeepAlive(true);
      server.setTCPNoDelay(true);
      server.setReuseAddress(true);
      server.setAcceptBacklog(protocol.getAcceptBacklog());
      server.setSendBufferSize(protocol.getSendBufferSize());
      server.setReceiveBufferSize(protocol.getReceiveBufferSize());
      server.setSSL(protocol.isSsl());
      server.setKeyStorePath(protocol.getKeyStorePath());
      server.setKeyStorePassword(protocol.getKeyStorePassword());
      server.setTrustStorePath(protocol.getTrustStorePath());
      server.setTrustStorePassword(protocol.getTrustStorePassword());
      server.setClientAuthRequired(protocol.isClientAuthRequired());
      server.setUsePooledBuffers(true);
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
    } else {
      future.complete(null);
    }
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
