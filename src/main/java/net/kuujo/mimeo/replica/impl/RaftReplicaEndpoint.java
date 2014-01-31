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
package net.kuujo.mimeo.replica.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxException;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.eventbus.Message;

import net.kuujo.mimeo.protocol.PingRequest;
import net.kuujo.mimeo.protocol.PingResponse;
import net.kuujo.mimeo.protocol.PollRequest;
import net.kuujo.mimeo.protocol.PollResponse;
import net.kuujo.mimeo.protocol.SubmitRequest;
import net.kuujo.mimeo.protocol.SubmitResponse;
import net.kuujo.mimeo.protocol.SyncRequest;
import net.kuujo.mimeo.protocol.SyncResponse;
import net.kuujo.mimeo.replica.ReplicaEndpoint;

/**
 * A default service.
 * 
 * @author Jordan Halterman
 */
public class RaftReplicaEndpoint implements ReplicaEndpoint {
  private static final long DEFAULT_REPLY_TIMEOUT = 5000;
  private final Vertx vertx;
  private String address;
  private Handler<PingRequest> pingHandler;
  private Handler<SyncRequest> syncHandler;
  private Handler<PollRequest> pollHandler;
  private Handler<SubmitRequest> submitHandler;
  private boolean running;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      JsonObject request = message.body().getObject("request");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action specified."));
      }
      else if (request == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "Malformed request."));
      }
      else {
        switch (action) {
          case "ping":
            if (pingHandler != null) {
              pingHandler.handle(PingRequest.fromJson(request, message));
            }
          case "sync":
            if (syncHandler != null) {
              syncHandler.handle(SyncRequest.fromJson(request, message));
            }
            break;
          case "poll":
            if (pollHandler != null) {
              pollHandler.handle(PollRequest.fromJson(request, message));
            }
            break;
          case "submit":
            if (submitHandler != null) {
              submitHandler.handle(SubmitRequest.fromJson(request, message));
            }
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public RaftReplicaEndpoint(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public ReplicaEndpoint setAddress(String address) {
    if (running) throw new IllegalStateException("Cannot set endpoint address during operation.");
    this.address = address;
    return this;
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public ReplicaEndpoint ping(String address, PingRequest request, Handler<AsyncResult<PingResponse>> resultHandler) {
    return ping(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public ReplicaEndpoint ping(String address, PingRequest request, long timeout, Handler<AsyncResult<PingResponse>> resultHandler) {
    final Future<PingResponse> future = new DefaultFutureResult<PingResponse>().setHandler(resultHandler);
    vertx.eventBus().sendWithTimeout(address,
        new JsonObject().putString("action", "ping").putObject("request", PingRequest.toJson(request)), timeout,
        new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              String status = result.result().body().getString("status");
              if (status.equals("ok")) {
                future.setResult(PingResponse.fromJson(result.result().body().getObject("result")));
              }
              else {
                future.setFailure(new VertxException(result.result().body().getString("message", "")));
              }
            }
          }
        });
    return this;
  }

  @Override
  public ReplicaEndpoint pingHandler(Handler<PingRequest> handler) {
    pingHandler = handler;
    return this;
  }

  @Override
  public ReplicaEndpoint sync(String address, SyncRequest request, Handler<AsyncResult<SyncResponse>> resultHandler) {
    return sync(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public ReplicaEndpoint sync(String address, SyncRequest request, long timeout, Handler<AsyncResult<SyncResponse>> resultHandler) {
    final Future<SyncResponse> future = new DefaultFutureResult<SyncResponse>().setHandler(resultHandler);
    vertx.eventBus().sendWithTimeout(address,
        new JsonObject().putString("action", "sync").putObject("request", SyncRequest.toJson(request)), timeout,
        new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              String status = result.result().body().getString("status");
              if (status.equals("ok")) {
                future.setResult(SyncResponse.fromJson(result.result().body().getObject("result")));
              }
              else {
                future.setFailure(new VertxException(result.result().body().getString("message", "")));
              }
            }
          }
        });
    return this;
  }

  @Override
  public ReplicaEndpoint syncHandler(Handler<SyncRequest> handler) {
    syncHandler = handler;
    return this;
  }

  @Override
  public ReplicaEndpoint poll(String address, PollRequest request, Handler<AsyncResult<PollResponse>> resultHandler) {
    return poll(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public ReplicaEndpoint poll(String address, PollRequest request, long timeout, Handler<AsyncResult<PollResponse>> resultHandler) {
    final Future<PollResponse> future = new DefaultFutureResult<PollResponse>().setHandler(resultHandler);
    vertx.eventBus().sendWithTimeout(address,
        new JsonObject().putString("action", "poll").putObject("request", PollRequest.toJson(request)), timeout,
        new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              String status = result.result().body().getString("status");
              if (status.equals("ok")) {
                future.setResult(PollResponse.fromJson(result.result().body().getObject("result")));
              }
              else {
                future.setFailure(new VertxException(result.result().body().getString("message", "")));
              }
            }
          }
        });
    return this;
  }

  @Override
  public ReplicaEndpoint pollHandler(Handler<PollRequest> handler) {
    pollHandler = handler;
    return this;
  }

  @Override
  public ReplicaEndpoint submit(String address, SubmitRequest request, Handler<AsyncResult<SubmitResponse>> resultHandler) {
    return submit(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public ReplicaEndpoint submit(String address, SubmitRequest request, long timeout,
      Handler<AsyncResult<SubmitResponse>> resultHandler) {
    final Future<SubmitResponse> future = new DefaultFutureResult<SubmitResponse>().setHandler(resultHandler);
    vertx.eventBus().sendWithTimeout(address,
        new JsonObject().putString("action", "submit").putObject("request", SubmitRequest.toJson(request)), timeout,
        new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              String status = result.result().body().getString("status");
              if (status.equals("ok")) {
                future.setResult(SubmitResponse.fromJson(result.result().body().getObject("result")));
              }
              else {
                future.setFailure(new VertxException(result.result().body().getString("message", "")));
              }
            }
          }
        });
    return this;
  }

  @Override
  public ReplicaEndpoint submitHandler(Handler<SubmitRequest> handler) {
    submitHandler = handler;
    return this;
  }

  @Override
  public ReplicaEndpoint start() {
    start(null);
    return this;
  }

  @Override
  public ReplicaEndpoint start(Handler<AsyncResult<Void>> doneHandler) {
    running = true;
    vertx.eventBus().registerHandler(address, messageHandler, doneHandler);
    return this;
  }

  @Override
  public void stop() {
    stop(null);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().unregisterHandler(address, messageHandler, doneHandler);
    running = false;
  }

}
