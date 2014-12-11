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
package net.kuujo.copycat.collections.internal.lock;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.collections.AsyncLock;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous lock implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncLock implements AsyncLock {
  private final StateMachine<AsyncLockState> stateMachine;
  private AsyncLockProxy proxy;

  public DefaultAsyncLock(StateMachine<AsyncLockState> stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public CompletableFuture<Void> lock() {
    if (proxy == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Lock closed"));
      return future;
    }
    return proxy.lock();
  }

  @Override
  public CompletableFuture<Void> unlock() {
    if (proxy == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Lock closed"));
      return future;
    }
    return proxy.unlock();
  }

  @Override
  public String name() {
    return stateMachine.name();
  }

  @Override
  public Cluster cluster() {
    return stateMachine.cluster();
  }

  @Override
  public CopycatState state() {
    return stateMachine.state();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return stateMachine.delete();
  }

  @Override
  public CompletableFuture<Void> open() {
    return stateMachine.open().thenRun(() -> {
      this.proxy = stateMachine.createProxy(AsyncLockProxy.class);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
