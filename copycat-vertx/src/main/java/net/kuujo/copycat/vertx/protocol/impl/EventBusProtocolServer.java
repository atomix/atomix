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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolServer implements ProtocolServer {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private final String address;
  private Vertx vertx;
  private ProtocolHandler requestHandler;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      switch (action) {
        case "appendEntries":
          doAppendEntries(message);
          break;
        case "requestVote":
          doRequestVote(message);
          break;
        case "submitCommand":
          doSubmitCommand(message);
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

  private void doAppendEntries(final Message<JsonObject> message) {
    if (requestHandler != null) {
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = message.body().getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      final Object id = message.body().getValue("id");
      AppendEntriesRequest request = new AppendEntriesRequest(id, message.body().getLong("term"), message.body().getString("leader"), message.body().getLong("prevIndex"), message.body().getLong("prevTerm"), entries, message.body().getLong("commit"));
      requestHandler.appendEntries(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()).putNumber("lastIndex", response.lastLogIndex()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
        }
      });
    }
  }

  private void doRequestVote(final Message<JsonObject> message) {
    if (requestHandler != null) {
      final Object id = message.body().getValue("id");
      RequestVoteRequest request = new RequestVoteRequest(id, message.body().getLong("term"), message.body().getString("candidate"), message.body().getLong("lastIndex"), message.body().getLong("lastTerm"));
      requestHandler.requestVote(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
        }
      });
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void doSubmitCommand(final Message<JsonObject> message) {
    if (requestHandler != null) {
      final Object id = message.body().getValue("id");
      SubmitCommandRequest request = new SubmitCommandRequest(id, message.body().getString("command"), message.body().getArray("args").toList());
      requestHandler.submitCommand(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            if (response.result() instanceof Map) {
              message.reply(new JsonObject().putString("status", "ok").putValue("id", id).putObject("result", new JsonObject((Map) response.result())));
            } else if (response.result() instanceof List) {
              message.reply(new JsonObject().putString("status", "ok").putValue("id", id).putArray("result", new JsonArray((List) response.result())));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putValue("id", id).putValue("result", response.result()));
            }
          } else {
            message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          message.reply(new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
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
