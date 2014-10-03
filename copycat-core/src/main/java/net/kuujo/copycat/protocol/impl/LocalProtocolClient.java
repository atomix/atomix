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
package net.kuujo.copycat.protocol.impl;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.registry.Registry;

/**
 * Local protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolClient implements ProtocolClient {
  private final String address;
  private final Registry registry;

  public LocalProtocolClient(String address, Registry registry) {
    this.address = address;
    this.registry = registry;
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    if (server == null) {
      CompletableFuture<SyncResponse> future = new CompletableFuture<>();
      future.completeExceptionally(new ProtocolException("Invalid server address"));
      return future;
    }
    return server.appendEntries(request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    if (server == null) {
      CompletableFuture<PollResponse> future = new CompletableFuture<>();
      future.completeExceptionally(new ProtocolException("Invalid server address"));
      return future;
    }
    return server.requestVote(request);
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    if (server == null) {
      CompletableFuture<SubmitResponse> future = new CompletableFuture<>();
      future.completeExceptionally(new ProtocolException("Invalid server address"));
      return future;
    }
    return server.submitCommand(request);
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
