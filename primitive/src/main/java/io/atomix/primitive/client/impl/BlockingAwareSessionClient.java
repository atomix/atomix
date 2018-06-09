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
package io.atomix.primitive.client.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.client.SessionClient;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.atomix.utils.concurrent.Futures.asyncFuture;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwareSessionClient extends DelegatingSessionClient {
  private final ThreadContext context;
  private final Map<Consumer<PrimitiveState>, Consumer<PrimitiveState>> stateChangeListeners = Maps.newConcurrentMap();
  private final Map<Consumer<PrimitiveEvent>, Consumer<PrimitiveEvent>> eventListeners = Maps.newConcurrentMap();

  public BlockingAwareSessionClient(SessionClient delegate, ThreadContext context) {
    super(delegate);
    this.context = context;
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    Consumer<PrimitiveState> wrappedListener = state -> context.execute(() -> listener.accept(state));
    stateChangeListeners.put(listener, wrappedListener);
    super.addStateChangeListener(wrappedListener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    Consumer<PrimitiveState> wrappedListener = stateChangeListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeStateChangeListener(wrappedListener);
    }
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return asyncFuture(super.execute(operation), context);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> context.execute(() -> listener.accept(e));
    eventListeners.put(listener, wrappedListener);
    super.addEventListener(eventType, wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = eventListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(eventType, wrappedListener);
    }
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    return asyncFuture(super.connect(), context);
  }

  @Override
  public CompletableFuture<Void> close() {
    return asyncFuture(super.close(), context);
  }
}
