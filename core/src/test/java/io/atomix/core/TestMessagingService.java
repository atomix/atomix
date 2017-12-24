/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core;

import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingException.NoRemoteHandler;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test messaging service.
 */
public class TestMessagingService implements ManagedMessagingService {
  private final Endpoint endpoint;
  private final Map<Endpoint, TestMessagingService> services;
  private final Map<String, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>>> handlers = new ConcurrentHashMap<>();
  private final AtomicBoolean started = new AtomicBoolean();

  public TestMessagingService(Endpoint endpoint, Map<Endpoint, TestMessagingService> services) {
    this.endpoint = endpoint;
    this.services = services;
  }

  /**
   * Returns the test service for the given endpoint or {@code null} if none has been created.
   */
  private TestMessagingService getService(Endpoint endpoint) {
    checkNotNull(endpoint);
    return services.get(endpoint);
  }

  /**
   * Returns the given handler for the given endpoint.
   */
  private BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> getHandler(Endpoint endpoint, String type) {
    TestMessagingService service = getService(endpoint);
    if (service == null) {
      return (e, p) -> Futures.exceptionalFuture(new NoRemoteHandler());
    }
    BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler = service.handlers.get(checkNotNull(type));
    if (handler == null) {
      return (e, p) -> Futures.exceptionalFuture(new NoRemoteHandler());
    }
    return handler;
  }

  @Override
  public Endpoint endpoint() {
    return endpoint;
  }

  @Override
  public CompletableFuture<Void> sendAsync(Endpoint ep, String type, byte[] payload) {
    return getHandler(ep, type).apply(endpoint, payload).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload) {
    return getHandler(ep, type).apply(endpoint, payload);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload, Executor executor) {
    ComposableFuture<byte[]> future = new ComposableFuture<>();
    sendAndReceive(ep, type, payload).whenCompleteAsync(future, executor);
    return future;
  }

  @Override
  public void registerHandler(String type, BiConsumer<Endpoint, byte[]> handler, Executor executor) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, (e, p) -> {
      try {
        executor.execute(() -> handler.accept(e, p));
        return CompletableFuture.completedFuture(new byte[0]);
      } catch (RejectedExecutionException e2) {
        return Futures.exceptionalFuture(e2);
      }
    });
  }

  @Override
  public void registerHandler(String type, BiFunction<Endpoint, byte[], byte[]> handler, Executor executor) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, (e, p) -> {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      try {
        executor.execute(() -> future.complete(handler.apply(e, p)));
      } catch (RejectedExecutionException e2) {
        future.completeExceptionally(e2);
      }
      return future;
    });
  }

  @Override
  public void registerHandler(String type, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, handler);
  }

  @Override
  public void unregisterHandler(String type) {
    handlers.remove(checkNotNull(type));
  }

  @Override
  public CompletableFuture<MessagingService> start() {
    services.put(endpoint, this);
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    services.remove(endpoint);
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
