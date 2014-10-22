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

import net.kuujo.copycat.Copycat;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Event bus service implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusService extends AbstractService {
  private String address;
  private String host = "localhost";
  private int port;
  private Vertx vertx;

  public VertxEventBusService(Copycat copycat) {
    super(copycat);
  }

  public VertxEventBusService(Copycat copycat, String host, int port, String address) {
    super(copycat);
    this.host = host;
    this.port = port;
    this.address = address;
  }

  public VertxEventBusService(Copycat copycat, Vertx vertx, String address) {
    super(copycat);
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
        submit(command, args.toArray()).whenComplete((result, error) -> {
          if (error == null) {
            if (result instanceof Map) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", copycat.leader()).putObject("result", new JsonObject((Map) result)));
            } else if (result instanceof List) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", copycat.leader()).putArray("result", new JsonArray((List) result)));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", copycat.leader()).putValue("result", result));
            }
          }
        });
      }
    }
  };

  /**
   * Sets the event bus address.
   *
   * @param address The event bus address.
   */
  public void setAddress(String address) {
    this.address = address != null && !address.isEmpty() ? address : this.address;
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
   * Sets the event bus address, returning the service for method chaining.
   *
   * @param address The event bus address.
   * @return The event bus service.
   */
  public VertxEventBusService withAddress(String address) {
    this.address = address;
    return this;
  }

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Returns the Vert.x instance.
   *
   * @return The Vert.x instance.
   */
  public Vertx getVertx() {
    return vertx;
  }

  /**
   * Sets the Vert.x instance, returning the service for method chaining.
   *
   * @param vertx The Vert.x instance.
   * @return The event bus service.
   */
  public VertxEventBusService withVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Sets the Vert.x host.
   *
   * @param host The Vert.x host.
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the Vert.x host.
   *
   * @return The Vert.x host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the Vert.x host, returning the event bus service for method chaining.
   *
   * @param host The Vert.x host.
   * @return The event bus service.
   */
  public VertxEventBusService withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the Vert.x port.
   *
   * @param port The Vert.x port.
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the Vert.x port.
   *
   * @return The Vert.x port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the Vert.x port, returning the service for method chaining.
   *
   * @param port The Vert.x port.
   * @return The event bus service.
   */
  public VertxEventBusService withPort(int port) {
    this.port = port;
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
    if (vertx != null) {
      vertx.eventBus().unregisterHandler(address, messageHandler, (result) -> {
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

}
