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
package net.kuujo.copycat.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.AsyncCopycat;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

/**
 * Vert.x TCP protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpService extends AbstractAsyncService {
  private Vertx vertx = new DefaultVertx();
  private NetServer server;
  private String host;
  private int port;

  public TcpService(AsyncCopycat copycat) {
    super(copycat);
    this.vertx = new DefaultVertx();
  }

  public TcpService(AsyncCopycat copycat, Vertx vertx) {
    super(copycat);
    this.vertx = vertx;
  }

  public TcpService(AsyncCopycat copycat, String host, int port) {
    super(copycat);
    this.host = host;
    this.port = port;
  }

  /**
   * Sets the service host.
   *
   * @param host The TCP host.
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the service host.
   *
   * @return The service host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the service host, returning the service for method chaining.
   *
   * @param host The TCP host.
   * @return The TCP service.
   */
  public TcpService withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the service port.
   *
   * @param port The TCP port.
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the service port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the service port, returning the service for method chaining.
   *
   * @param port The TCP port.
   * @return The TCP service.
   */
  public TcpService withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (server == null) {
      server = vertx.createNetServer();
    }

    server.connectHandler(new Handler<NetSocket>() {
      @Override
      public void handle(final NetSocket socket) {
        socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
          @Override
          @SuppressWarnings({"unchecked", "rawtypes"})
          public void handle(Buffer buffer) {
            JsonObject json = new JsonObject(buffer.toString());
            submit(json.getString("command"), json.getArray("args").toArray()).whenComplete((result, error) -> {
              if (error == null) {
                if (result instanceof Map) {
                  socket.write(new JsonObject().putString("status", "ok").putObject("result", new JsonObject((Map) result)).encode() + '\00');
                } else if (result instanceof List) {
                  socket.write(new JsonObject().putString("status", "ok").putArray("result", new JsonArray((List) result)).encode() + '\00');
                } else {
                  socket.write(new JsonObject().putString("status", "ok").putValue("result", result).encode() + '\00');
                }
              } else {
                socket.write(new JsonObject().putString("status", "error").putString("message", error.getMessage()).encode() + '\00');
              }
            });
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
    return future;
  }

  @Override
  public CompletableFuture<Void> stop() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
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
    return future;
  }

}
