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
package net.kuujo.copycat.vertx.protocol.impl;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolServer implements ProtocolServer {
  private final Serializer serializer = SerializerFactory.getSerializer();
  private final String address;
  private Vertx vertx;
  private ProtocolHandler requestHandler;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      switch (action) {
        case "appendEntries":
          handleAppendEntries(message);
          break;
        case "requestVote":
          handleRequestVote(message);
          break;
        case "submitCommand":
          handleSubmitCommand(message);
          break;
      }
    }
  };

  public EventBusProtocolServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  /**
   * Handles an append entries request.
   */
  private void handleAppendEntries(final Message<JsonObject> message) {
    if (requestHandler != null) {
      AppendEntriesRequest request = serializer.readValue(message.body().getBinary("request"), AppendEntriesRequest.class);
      requestHandler.appendEntries(request).whenComplete((response, error) -> {
        if (error == null) {
          message.reply(new JsonObject().putString("status", "ok").putBinary("response", serializer.writeValue(response)));
        } else {
          message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a request vote request.
   */
  private void handleRequestVote(final Message<JsonObject> message) {
    if (requestHandler != null) {
      RequestVoteRequest request = serializer.readValue(message.body().getBinary("request"), RequestVoteRequest.class);
      requestHandler.requestVote(request).whenComplete((response, error) -> {
        if (error == null) {
          message.reply(new JsonObject().putString("status", "ok").putBinary("response", serializer.writeValue(response)));
        } else {
          message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a submit command request.
   */
  private void handleSubmitCommand(final Message<JsonObject> message) {
    if (requestHandler != null) {
      SubmitCommandRequest request = serializer.readValue(message.body().getBinary("request"), SubmitCommandRequest.class);
      requestHandler.submitCommand(request).whenComplete((response, error) -> {
        if (error == null) {
          message.reply(new JsonObject().putString("status", "ok").putBinary("response", serializer.writeValue(response)));
        } else {
          message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
        }
      });
    }
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().registerHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
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

  @Override
  public CompletableFuture<Void> stop() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().unregisterHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
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
