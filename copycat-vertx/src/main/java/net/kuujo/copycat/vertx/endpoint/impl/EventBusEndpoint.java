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
import net.kuujo.copycat.uri.UriPath;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Event bus endpoint implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusEndpoint implements Endpoint {
  private String address;
  private CopyCatContext context;
  private String host = "localhost";
  private int port;
  private Vertx vertx;

  public EventBusEndpoint() {
  }

  public EventBusEndpoint(String host, int port, String address) {
    this.host = host;
    this.port = port;
    this.address = address;
  }

  public EventBusEndpoint(Vertx vertx, String address) {
    this.vertx = vertx;
    this.address = address;
  }

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void handle(final Message<JsonObject> message) {
      String command = message.body().getString("command");
      if (command != null) {
        JsonArray args = message.body().getArray("args");
        context.submitCommand(command, args.toArray()).whenComplete((result, error) -> {
          if (error == null) {
            if (result instanceof Map) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putObject("result", new JsonObject((Map) result)));
            } else if (result instanceof List) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putArray("result", new JsonArray((List) result)));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putValue("result", result));
            }
          }
        });
      }
    }
  };

  public void setContext(CopyCatContext context) {
    this.context = context;
  }

  public CopyCatContext getContext() {
    return context;
  }

  /**
   * Sets the event bus address.
   *
   * @param address The event bus address.
   */
  @UriPath
  public void setAddress(String address) {
    this.address = address;
  }

  /**
   * Returns the event bus address.
   *
   * @return The event bus address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the event bus address, returning the endpoint for method chaining.
   *
   * @param address The event bus address.
   * @return The event bus endpoint.
   */
  public EventBusEndpoint withAddress(String address) {
    this.address = address;
    return this;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (vertx == null) {
      vertx = new DefaultVertx(port >= 0 ? port : 0, host, (vertxResult) -> {
        if (vertxResult.failed()) {
          future.completeExceptionally(vertxResult.cause());
        } else {
          vertx.eventBus().registerHandler(address, messageHandler, (eventBusResult) -> {
            if (eventBusResult.failed()) {
              future.completeExceptionally(eventBusResult.cause());
            } else {
              future.complete(null);
            }
          });
        }
      });
    } else {
      vertx.eventBus().registerHandler(address, messageHandler, (result) -> {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      });
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> stop() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().unregisterHandler(address, messageHandler, (result) -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

}
