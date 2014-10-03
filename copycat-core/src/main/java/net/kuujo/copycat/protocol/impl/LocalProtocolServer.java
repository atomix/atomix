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

import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.util.Args;

/**
 * Local protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolServer implements ProtocolServer {
  private final String address;
  private final Registry registry;
  private ProtocolHandler requestHandler;

  public LocalProtocolServer(String address, Registry registry) {
    this.address = address;
    this.registry = registry;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  CompletableFuture<PingResponse> ping(PingRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.ping(request);
  }

  CompletableFuture<SyncResponse> sync(SyncRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.sync(request);
  }

  CompletableFuture<PollResponse> poll(PollRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.poll(request);
  }

  CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    Args.checkNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.submit(request);
  }

  @Override
  public CompletableFuture<Void> start() {
    registry.bind(address, this);
    return CompletableFuture.completedFuture((Void) null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    registry.unbind(address);
    return CompletableFuture.completedFuture((Void) null);
  }

}
