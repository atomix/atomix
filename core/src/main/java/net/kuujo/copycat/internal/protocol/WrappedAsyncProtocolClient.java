/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.protocol;

import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.protocol.AsyncProtocolClient;
import net.kuujo.copycat.spi.protocol.ProtocolClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Asynchronous protocol client wrapper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WrappedAsyncProtocolClient implements AsyncProtocolClient {
  private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("wrapped-async-protocol-client-%s");
  private final Executor executor = Executors.newCachedThreadPool(THREAD_FACTORY);
  private final ProtocolClient client;

  public WrappedAsyncProtocolClient(ProtocolClient client) {
    this.client = client;
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return CompletableFuture.supplyAsync(() -> client.ping(request), executor);
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return CompletableFuture.supplyAsync(() -> client.sync(request), executor);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return CompletableFuture.supplyAsync(() -> client.poll(request), executor);
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    return CompletableFuture.supplyAsync(() -> client.submit(request), executor);
  }

  @Override
  public CompletableFuture<Void> connect() {
    return CompletableFuture.runAsync(client::connect, executor);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.runAsync(client::close, executor);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
