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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolReader;
import net.kuujo.copycat.protocol.ProtocolWriter;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.util.Args;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolClient implements ProtocolClient {
  private final ProtocolReader reader = new ProtocolReader();
  private final ProtocolWriter writer = new ProtocolWriter();
  private final String address;
  private Vertx vertx;

  public EventBusProtocolClient(String address, Vertx vertx) {
    this.address = Args.checkNotNull(address, "Vert.x event bus address cannot be null");
    this.vertx = Args.checkNotNull(vertx, "Vert.x instance cannot be null");
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    final CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
    DeliveryOptions options = DeliveryOptions.options().setSendTimeout(5000);
    vertx.eventBus().send(address, writer.writeRequest(request), options, new Handler<AsyncResult<Message<byte[]>>>() {
      @Override
      public void handle(AsyncResult<Message<byte[]>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(reader.readResponse(result.result().body()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(final RequestVoteRequest request) {
    final CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
    DeliveryOptions options = DeliveryOptions.options().setSendTimeout(5000);
    vertx.eventBus().send(address, writer.writeRequest(request), options, new Handler<AsyncResult<Message<byte[]>>>() {
      @Override
      public void handle(AsyncResult<Message<byte[]>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(reader.readResponse(result.result().body()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(final SubmitCommandRequest request) {
    final CompletableFuture<SubmitCommandResponse> future = new CompletableFuture<>();
    DeliveryOptions options = DeliveryOptions.options().setSendTimeout(5000);
    vertx.eventBus().send(address, writer.writeRequest(request), options, new Handler<AsyncResult<Message<byte[]>>>() {
      @Override
      public void handle(AsyncResult<Message<byte[]>> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(reader.readResponse(result.result().body()));
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
