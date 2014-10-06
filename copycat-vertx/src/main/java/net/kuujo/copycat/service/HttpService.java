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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x HTTP service.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HttpService extends BaseService {
  private final Vertx vertx;
  private HttpServer server;
  private String host;
  private int port;

  public HttpService() {
    this.vertx = new DefaultVertx();
  }

  public HttpService(String host, int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

  public HttpService(String host, int port, Vertx vertx) {
    this.host = host;
    this.port = port;
    this.vertx = vertx;
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
  public HttpService withHost(String host) {
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
  public HttpService withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (server == null) {
      server = vertx.createHttpServer();
    }

    RouteMatcher routeMatcher = new RouteMatcher();
    routeMatcher.post("/:command", new Handler<HttpServerRequest>() {
      @Override
      public void handle(final HttpServerRequest request) {
        request.bodyHandler(new Handler<Buffer>() {
          @Override
          @SuppressWarnings({"unchecked", "rawtypes"})
          public void handle(Buffer buffer) {
            submit(request.params().get("command"), new JsonArray(buffer.toString()).toArray()).whenComplete((result, error) -> {
              if (error == null) {
                request.response().setStatusCode(200);
                if (result instanceof Map) {
                  request.response().end(new JsonObject().putString("status", "ok").putObject("result", new JsonObject((Map) result)).encode());
                } else if (result instanceof List) {
                  request.response().end(new JsonObject().putString("status", "ok").putArray("result", new JsonArray((List) result)).encode());
                } else {
                  request.response().end(new JsonObject().putString("status", "ok").putValue("result", result).encode());
                }
              } else {
                request.response().setStatusCode(400);
              }
            });
          }
        });
      }
    });

    server.requestHandler(routeMatcher);
    server.listen(port, host, new Handler<AsyncResult<HttpServer>>() {
      @Override
      public void handle(AsyncResult<HttpServer> result) {
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
