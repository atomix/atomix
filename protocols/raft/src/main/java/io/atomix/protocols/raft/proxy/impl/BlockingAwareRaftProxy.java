/*
 * Copyright 2017-present Open Networking Laboratory
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
import io.atomix.event.Event;
import io.atomix.event.EventListener;
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyDelegate;
import io.atomix.utils.concurrent.Futures;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwareRaftProxy extends RaftProxyDelegate {
  private final Executor executor;
  private final Map<EventListener, EventListener> listenerMap = Maps.newConcurrentMap();

  public BlockingAwareRaftProxy(RaftProxy delegate, Executor executor) {
    super(delegate);
    this.executor = checkNotNull(executor, "executor cannot be null");
  }

  @Override
  public <E extends Event> void addEventListener(EventListener<E> listener) {
    EventListener<E> wrappedListener = event -> executor.execute(() -> listener.onEvent(event));
    listenerMap.put(listener, wrappedListener);
    super.addEventListener(wrappedListener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E extends Event> void removeEventListener(EventListener<E> listener) {
    EventListener<E> wrappedListener = listenerMap.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(wrappedListener);
    }
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftCommand<T> command) {
    return Futures.blockingAwareFuture(super.submit(command), executor);
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftQuery<T> query) {
    return Futures.blockingAwareFuture(super.submit(query), executor);
  }
}
