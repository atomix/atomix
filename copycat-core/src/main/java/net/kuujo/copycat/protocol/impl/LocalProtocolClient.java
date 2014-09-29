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

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.util.Args;

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
  public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    Args.checkNotNull(server, () -> {
      throw new ProtocolException("Invalid server address");
    });
    return server.appendEntries(request);
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    Args.checkNotNull(server, () -> {
      throw new ProtocolException("Invalid server address");
    });
    return server.requestVote(request);
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    LocalProtocolServer server = registry.lookup(address);
    Args.checkNotNull(server, () -> {
      throw new ProtocolException("Invalid server address");
    });
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
