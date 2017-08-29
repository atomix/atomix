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
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.collect.Maps;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwareRaftProxyClient extends DelegatingRaftProxyClient {
  private final Executor executor;
  private final Map<Consumer<RaftProxy.State>, Consumer<RaftProxy.State>> stateChangeListeners = Maps.newConcurrentMap();
  private final Map<Consumer<RaftEvent>, Consumer<RaftEvent>> eventListeners = Maps.newConcurrentMap();

  public BlockingAwareRaftProxyClient(RaftProxyClient delegate, Executor executor) {
    super(delegate);
    this.executor = checkNotNull(executor, "executor cannot be null");
  }

  @Override
  public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
    Consumer<RaftProxy.State> wrappedListener = state -> executor.execute(() -> listener.accept(state));
    stateChangeListeners.put(listener, wrappedListener);
    super.addStateChangeListener(wrappedListener);
  }

  @Override
  public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
    Consumer<RaftProxy.State> wrappedListener = stateChangeListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeStateChangeListener(wrappedListener);
    }
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return Futures.blockingAwareFuture(super.execute(operation), executor);
  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {
    Consumer<RaftEvent> wrappedListener = e -> executor.execute(() -> listener.accept(e));
    eventListeners.put(listener, wrappedListener);
    super.addEventListener(wrappedListener);
  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {
    Consumer<RaftEvent> wrappedListener = eventListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(wrappedListener);
    }
  }
}
