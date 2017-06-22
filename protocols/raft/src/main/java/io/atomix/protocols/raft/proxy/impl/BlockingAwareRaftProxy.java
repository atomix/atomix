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
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyDelegate;
import io.atomix.utils.concurrent.Futures;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwareRaftProxy extends RaftProxyDelegate {
  private final Executor orderedExecutor;
  private final Executor threadPoolExecutor;
  private final Map<Consumer, Consumer> listenerMap = Maps.newConcurrentMap();

  public BlockingAwareRaftProxy(RaftProxy delegate, Executor orderedExecutor, Executor threadPoolExecutor) {
    super(delegate);
    this.orderedExecutor = checkNotNull(orderedExecutor, "orderedExecutor cannot be null");
    this.threadPoolExecutor = checkNotNull(threadPoolExecutor, "threadPoolExecutor cannot be null");
  }

  @Override
  public <T> void addEventListener(Consumer<T> listener) {
    Consumer<T> wrappedListener = event -> orderedExecutor.execute(() -> listener.accept(event));
    listenerMap.put(listener, wrappedListener);
    super.addEventListener(wrappedListener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void removeEventListener(Consumer<T> listener) {
    Consumer<T> wrappedListener = listenerMap.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(wrappedListener);
    }
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftCommand<T> command) {
    return Futures.blockingAwareFuture(super.submit(command), orderedExecutor, threadPoolExecutor);
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftQuery<T> query) {
    return Futures.blockingAwareFuture(super.submit(query), orderedExecutor, threadPoolExecutor);
  }
}
