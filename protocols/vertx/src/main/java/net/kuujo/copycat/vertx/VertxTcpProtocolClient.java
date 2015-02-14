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

import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.net.NetClient;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolClient implements ProtocolClient {
  private Vertx vertx;
  private final String host;
  private final int port;
  private final VertxTcpProtocol protocol;
  private NetClient client;

  public VertxTcpProtocolClient(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  @Override
  public CompletableFuture<ProtocolConnection> connect() {
    final CompletableFuture<ProtocolConnection> future = new CompletableFuture<>();

    if (vertx == null)
      vertx = protocol.getVertx();
    if (vertx == null)
      vertx = new DefaultVertx();

    if (client == null) {
      client = vertx.createNetClient();
      client.setTCPKeepAlive(true);
      client.setTCPNoDelay(true);
      client.setSendBufferSize(protocol.getSendBufferSize());
      client.setReceiveBufferSize(protocol.getReceiveBufferSize());
      client.setSSL(protocol.isSsl());
      client.setKeyStorePath(protocol.getKeyStorePath());
      client.setKeyStorePassword(protocol.getKeyStorePassword());
      client.setTrustStorePath(protocol.getTrustStorePath());
      client.setTrustStorePassword(protocol.getTrustStorePassword());
      client.setTrustAll(protocol.isClientTrustAll());
      client.setUsePooledBuffers(true);
      client.connect(port, host, result -> {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(new VertxTcpProtocolConnection(vertx, result.result()));
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
    if (client != null) {
      client.close();
      client = null;
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
