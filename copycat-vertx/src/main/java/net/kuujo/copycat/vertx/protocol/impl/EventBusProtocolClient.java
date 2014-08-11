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

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolClient implements ProtocolClient {
  private final String address;
  private final Vertx vertx;

  public EventBusProtocolClient(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public void appendEntries(final AppendEntriesRequest request, final AsyncCallback<AppendEntriesResponse> callback) {
    JsonObject message = new JsonObject();
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(result.cause()));
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new AppendEntriesResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("succeeded"))));
          } else if (status.equals("error")) {
            callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new AppendEntriesResponse(request.id(), result.result().body().getString("message"))));
          }
        }
      }
    });
  }

  @Override
  public void requestVote(final RequestVoteRequest request, final AsyncCallback<RequestVoteResponse> callback) {
    JsonObject message = new JsonObject();
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(result.cause()));
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new RequestVoteResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("voteGranted"))));
          } else if (status.equals("error")) {
            callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new RequestVoteResponse(request.id(), result.result().body().getString("message"))));
          }
        }
      }
    });
  }

  @Override
  public void submitCommand(final SubmitCommandRequest request, final AsyncCallback<SubmitCommandResponse> callback) {
    JsonObject message = new JsonObject();
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(result.cause()));
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new SubmitCommandResponse(request.id(), result.result().body().getObject("result").toMap())));
          } else if (status.equals("error")) {
            callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new SubmitCommandResponse(request.id(), result.result().body().getString("message"))));
          }
        }
      }
    });
  }

  @Override
  public void connect() {
  }

  @Override
  public void connect(AsyncCallback<Void> callback) {
    callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
  }

  @Override
  public void close() {
  }

  @Override
  public void close(AsyncCallback<Void> callback) {
    callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
  }

}
