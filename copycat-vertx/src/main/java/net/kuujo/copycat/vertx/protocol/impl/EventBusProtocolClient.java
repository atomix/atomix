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
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.util.Args;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolClient implements ProtocolClient {
  private final String address;
  private Vertx vertx;

  public EventBusProtocolClient(String address, Vertx vertx) {
    this.address = Args.checkNotNull(address, "Vert.x event bus address cannot be null");
    this.vertx = Args.checkNotNull(vertx, "Vert.x instance cannot be null");
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    final CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
    JsonObject message = new JsonObject()
        .putString("action", "appendEntries")
        .putNumber("term", request.term())
        .putString("leader", request.leader())
        .putNumber("prevIndex", request.prevLogIndex())
        .putNumber("prevTerm", request.prevLogTerm())
        .putNumber("commit", request.commitIndex());
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            future.complete(new AppendEntriesResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("succeeded"), result.result().body().getLong("lastIndex")));
          } else if (status.equals("error")) {
            future.complete(new AppendEntriesResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(final RequestVoteRequest request) {
    final CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
    JsonObject message = new JsonObject()
        .putString("action", "requestVote")
        .putNumber("term", request.term())
        .putString("candidate", request.candidate())
        .putNumber("lastIndex", request.lastLogIndex())
        .putNumber("lastTerm", request.lastLogTerm());
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            future.complete(new RequestVoteResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("voteGranted")));
          } else if (status.equals("error")) {
            future.complete(new RequestVoteResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(final SubmitCommandRequest request) {
    final CompletableFuture<SubmitCommandResponse> future = new CompletableFuture<>();
    JsonObject message = new JsonObject()
        .putString("action", "submitCommand")
        .putString("command", request.command())
        .putArray("args", new JsonArray(request.args()));
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            future.complete(new SubmitCommandResponse(request.id(), result.result().body().getValue("result")));
          } else if (status.equals("error")) {
            future.complete(new SubmitCommandResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> connect() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

}
