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

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.util.Args;

/**
 * Local protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolServer implements ProtocolServer {
  private final String address;
  private final CopyCatContext context;
  private ProtocolHandler requestHandler;

  public LocalProtocolServer(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.appendEntries(request);
  }

  CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.requestVote(request);
  }

  CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.submitCommand(request);
  }

  @Override
  public CompletableFuture<Void> start() {
    context.registry().bind(address, this);
    return CompletableFuture.completedFuture((Void) null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    context.registry().unbind(address);
    return CompletableFuture.completedFuture((Void) null);
  }

}
