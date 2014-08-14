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
package net.kuujo.copycat.vertx.endpoint.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriPort;

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
public class TcpEndpoint implements Endpoint {
  private Vertx vertx = new DefaultVertx();
  private CopyCatContext context;
  private NetServer server;
  private String host;
  private int port;

  public TcpEndpoint() {
    this.vertx = new DefaultVertx();
  }

  public TcpEndpoint(Vertx vertx) {
    this.vertx = vertx;
  }

  public TcpEndpoint(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Sets the endpoint host.
   *
   * @param host The TCP host.
   */
  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the endpoint host.
   *
   * @return The endpoint host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the endpoint host, returning the endpoint for method chaining.
   *
   * @param host The TCP host.
   * @return The TCP endpoint.
   */
  public TcpEndpoint withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the endpoint port.
   *
   * @param port The TCP port.
   */
  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the endpoint port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the endpoint port, returning the endpoint for method chaining.
   *
   * @param port The TCP port.
   * @return The TCP endpoint.
   */
  public TcpEndpoint withPort(int port) {
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
            context.submitCommand(json.getString("command"), json.getArray("args").toArray()).whenComplete((result, error) -> {
              if (error == null) {
                if (result instanceof Map) {
                  socket.write(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putObject("result", new JsonObject((Map) result)).encode() + '\00');
                } else if (result instanceof List) {
                  socket.write(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putArray("result", new JsonArray((List) result)).encode() + '\00');
                } else {
                  socket.write(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putValue("result", result).encode() + '\00');
                }
              } else {
                socket.write(new JsonObject().putString("status", "error").putString("leader", context.leader()).putString("message", error.getMessage()).encode() + '\00');
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
