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

import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyDelegate;
import io.atomix.utils.concurrent.Futures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class ExecutingRaftProxy extends RaftProxyDelegate {
  private final Executor orderedExecutor;
  private final Executor threadPoolExecutor;

  public ExecutingRaftProxy(RaftProxy delegate, Executor orderedExecutor, Executor threadPoolExecutor) {
    super(delegate);
    this.orderedExecutor = orderedExecutor;
    this.threadPoolExecutor = threadPoolExecutor;
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
