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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;
import net.kuujo.copycat.util.AsyncCallback;

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
  private final Vertx vertx;
  private ProtocolHandler requestHandler;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      switch (action) {
        case "ping":
          doPing(message);
          break;
        case "sync":
          doSync(message);
          break;
        case "install":
          doInstall(message);
          break;
        case "poll":
          doPoll(message);
          break;
        case "submit":
          doSubmit(message);
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

  private void doPing(final Message<JsonObject> message) {
    if (requestHandler != null) {
      PingRequest request = new PingRequest(message.body().getLong("term"), message.body().getString("leader"));
      requestHandler.ping(request, new AsyncCallback<PingResponse>() {
        @Override
        public void complete(PingResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putNumber("term", response.term()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          message.reply(new JsonObject().putString("status", "error").putString("message", t.getMessage()));
        }
      });
    }
  }

  private void doSync(final Message<JsonObject> message) {
    if (requestHandler != null) {
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = message.body().getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      SyncRequest request = new SyncRequest(message.body().getLong("term"), message.body().getString("leader"), message.body().getLong("prevIndex"), message.body().getLong("prevTerm"), entries, message.body().getLong("commit"));
      requestHandler.sync(request, new AsyncCallback<SyncResponse>() {
        @Override
        public void complete(SyncResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          message.reply(new JsonObject().putString("status", "error").putString("message", t.getMessage()));
        }
      });
    }
  }

  private void doInstall(final Message<JsonObject> message) {
    if (requestHandler != null) {
      Set<String> members = new HashSet<>();
      JsonArray jsonMembers = message.body().getArray("members");
      if (jsonMembers != null) {
        for (Object jsonMember : jsonMembers) {
          members.add((String) jsonMember);
        }
      }
      InstallRequest request = new InstallRequest(message.body().getLong("term"), message.body().getString("leader"), message.body().getLong("snapshotIndex"), message.body().getLong("snapshotTerm"), members, message.body().getBinary("data"), message.body().getBoolean("complete"));
      requestHandler.install(request, new AsyncCallback<InstallResponse>() {
        @Override
        public void complete(InstallResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          message.reply(new JsonObject().putString("status", "error").putString("message", t.getMessage()));
        }
      });
    }
  }

  private void doPoll(final Message<JsonObject> message) {
    if (requestHandler != null) {
      PollRequest request = new PollRequest(message.body().getLong("term"), message.body().getString("candidate"), message.body().getLong("lastIndex"), message.body().getLong("lastTerm"));
      requestHandler.poll(request, new AsyncCallback<PollResponse>() {
        @Override
        public void complete(PollResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
          } else {
            message.reply(new JsonObject().putString("status", "error").putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          message.reply(new JsonObject().putString("status", "error").putString("message", t.getMessage()));
        }
      });
    }
  }

  private void doSubmit(final Message<JsonObject> message) {
    if (requestHandler != null) {
      SubmitRequest request = new SubmitRequest(message.body().getString("command"), message.body().getObject("args").toMap());
      requestHandler.submit(request, new AsyncCallback<SubmitResponse>() {
        @Override
        public void complete(SubmitResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            message.reply(new JsonObject().putString("status", "ok").putObject("result", new JsonObject(response.result())));
          } else {
            message.reply(new JsonObject().putString("status", "error").putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          message.reply(new JsonObject().putString("status", "error").putString("message", t.getMessage()));
        }
      });
    }
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    vertx.eventBus().registerHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          callback.complete(null);
        }
      }
    });
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    vertx.eventBus().unregisterHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          callback.complete(null);
        }
      }
    });
  }

}
