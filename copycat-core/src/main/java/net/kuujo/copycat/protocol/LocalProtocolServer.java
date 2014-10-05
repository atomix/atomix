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
package net.kuujo.copycat.protocol;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.spi.protocol.ProtocolServer;
import net.kuujo.copycat.internal.util.Args;

/**
 * Local protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolServer implements ProtocolServer {
  private final String id;
  private final Map<String, LocalProtocolServer> registry;
  private RequestHandler requestHandler;

  public LocalProtocolServer(String id, Map<String, LocalProtocolServer> registry) {
    this.id = id;
    this.registry = registry;
  }

  @Override
  public void requestHandler(RequestHandler handler) {
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
    registry.put(id, this);
    return CompletableFuture.completedFuture((Void) null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    registry.remove(id);
    return CompletableFuture.completedFuture((Void) null);
  }

  @Override
  public String toString() {
    return String.format("LocalProtocolServer[id=%s]", id);
  }

}
