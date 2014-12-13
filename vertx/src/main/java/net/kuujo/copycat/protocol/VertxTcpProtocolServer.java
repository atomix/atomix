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
package net.kuujo.copycat.protocol;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolServer implements ProtocolServer {
  private static final String DELIMITER = "\\x00";
  private Vertx vertx;
  private final String host;
  private final int port;
  private boolean clientAuthRequired;
  private final VertxTcpProtocol protocol;
  private NetServer server;
  private ProtocolHandler handler;

  public VertxTcpProtocolServer(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   */
  public void setClientAuthRequired(boolean required) {
    this.clientAuthRequired = required;
  }

  /**
   * Returns whether client authentication is required.
   *
   * @return Whether client authentication is required.
   */
  public boolean isClientAuthRequired() {
    return clientAuthRequired;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   * @return The TCP protocol.
   */
  public VertxTcpProtocolServer withClientAuthRequired(boolean required) {
    this.clientAuthRequired = required;
    return this;
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (vertx == null) {
      vertx = new DefaultVertx();
    }

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
      server.setClientAuthRequired(clientAuthRequired);
      server.setUsePooledBuffers(true);
      server.connectHandler(new Handler<NetSocket>() {
        @Override
        public void handle(final NetSocket socket) {
          socket.dataHandler(RecordParser.newDelimited(DELIMITER, new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
              JsonObject json = new JsonObject(buffer.toString());
              Object id = json.getValue("id");
              try {
                handleRequest(id, socket, ByteBuffer.wrap(json.getBinary("request")));
              } catch (Exception e) {
                respond(socket, id, null, e);
              }
            }
          }));
        }
      }).listen(port, host, new Handler<AsyncResult<NetServer>>() {
        @Override
        public void handle(AsyncResult<NetServer> result) {
          if (result.failed()) {
            future.completeExceptionally(result.cause());
          } else {
            future.complete(null);
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Handles a request.
   */
  private void handleRequest(final Object id, final NetSocket socket, ByteBuffer request) {
    if (handler != null) {
      handler.handle(request).whenComplete((response, error) -> respond(socket, id, response, error));
    }
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, Object id, ByteBuffer response, Throwable error) {
    if (error != null) {
      socket.write(new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()).encode() + DELIMITER);
    } else {
      socket.write(new JsonObject().putString("status", "ok").putValue("id", id).putBinary("response", response.array()).encode() + DELIMITER);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (server != null) {
      server.close(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            future.completeExceptionally(result.cause());
          } else {
            future.complete(null);
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

}
