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

import net.kuujo.copycat.Arguments;
import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriArgument;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInject;
import net.kuujo.copycat.uri.UriPort;

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

/**
 * Vert.x HTTP endpoint.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HttpEndpoint implements Endpoint {
  private final Vertx vertx;
  private CopyCatContext context;
  private HttpServer server;
  private String host;
  private int port;

  public HttpEndpoint() {
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public HttpEndpoint(@UriArgument("vertx") Vertx vertx) {
    this.vertx = vertx;
  }

  @UriInject
  public HttpEndpoint(@UriHost String host) {
    this(host, 0);
  }

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port, @UriArgument("vertx") Vertx vertx) {
    this.host = host;
    this.port = port;
    this.vertx = vertx;
  }

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
    this.server = vertx.createHttpServer();
    RouteMatcher routeMatcher = new RouteMatcher();
    routeMatcher.post("/:command", new Handler<HttpServerRequest>() {
      @Override
      public void handle(final HttpServerRequest request) {
        request.bodyHandler(new Handler<Buffer>() {
          @Override
          public void handle(Buffer buffer) {
            HttpEndpoint.this.context.submitCommand(request.params().get("command"), new Arguments(new JsonObject(buffer.toString()).toMap()), new AsyncCallback<Object>() {
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public void call(net.kuujo.copycat.AsyncResult<Object> result) {
                if (result.succeeded()) {
                  request.response().setStatusCode(200);
                  if (result instanceof Map) {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putObject("result", new JsonObject((Map) result.value())).encode());                  
                  } else if (result instanceof List) {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putArray("result", new JsonArray((List) result.value())).encode());
                  } else {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putValue("result", result.value()).encode());
                  }
                } else {
                  request.response().setStatusCode(400);
                }
              }
            });
          }
        });
      }
    });
    server.requestHandler(routeMatcher);
  }

  /**
   * Sets the endpoint host.
   *
   * @param host The TCP host.
   * @return The TCP endpoint.
   */
  public HttpEndpoint setHost(String host) {
    this.host = host;
    return this;
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
   * Sets the endpoint port.
   *
   * @param port The TCP port.
   * @return The TCP endpoint.
   */
  public HttpEndpoint setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Returns the endpoint port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    server.listen(port, host, new Handler<AsyncResult<HttpServer>>() {
      @Override
      public void handle(AsyncResult<HttpServer> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
        } else {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
        }
      }
    });
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    server.close(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
        } else {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
        }
      }
    });
  }

}
