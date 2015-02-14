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
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolServer implements ProtocolServer, Handler<Message<JsonObject>> {
  private final String address;
  private final Vertx vertx;
  private final Context context;
  private EventListener<ProtocolConnection> listener;
  private final Map<String, ProtocolConnection> connections = new HashMap<>();

  public VertxEventBusProtocolServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
    this.context = vertx.currentContext();
  }

  @Override
  public ProtocolServer connectListener(EventListener<ProtocolConnection> listener) {
    this.listener = listener;
    return this;
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");
    if (action != null) {
      switch (action) {
        case "connect":
          handleConnect(message);
          break;
        case "disconnect":
          handleDisconnect(message);
          break;
      }
    }
  }

  /**
   * Handles a connect message.
   */
  private void handleConnect(Message<JsonObject> message) {
    String address = message.body().getString("address");
    if (address != null) {
      String id = UUID.randomUUID().toString();
      new VertxEventBusProtocolConnection(id, address, vertx, result -> {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          connections.put(address, result.result());
          message.reply(new JsonObject().putString("status", "ok").putString("address", id));
          if (listener != null) {
            listener.accept(result.result());
          }
        }
      });
    }
  }

  /**
   * Handles a disconnect message.
   */
  private void handleDisconnect(Message<JsonObject> message) {
    String address = message.body().getString("address");
    ProtocolConnection connection = connections.remove(address);
    if (connection != null) {
      connection.close().whenComplete((result, error) -> {
        if (error == null) {
          message.reply(new JsonObject().putString("status", "ok"));
        } else {
          message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
        }
      });
    } else {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Connection already closed"));
    }
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().registerHandler(address, this, result -> {
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
    vertx.eventBus().unregisterHandler(address, this, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public String toString() {
    return String.format("%s[address=%s]", getClass().getSimpleName(), address);
  }

}
