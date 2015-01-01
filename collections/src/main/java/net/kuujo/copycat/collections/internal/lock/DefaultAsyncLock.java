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

import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.collections.AsyncLock;
import net.kuujo.copycat.collections.AsyncLockProxy;
import net.kuujo.copycat.internal.AbstractDiscreteResource;
import net.kuujo.copycat.internal.DefaultStateMachine;
import net.kuujo.copycat.internal.util.concurrent.Futures;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Default asynchronous lock implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncLock extends AbstractDiscreteResource<AsyncLock> implements AsyncLock {
  private final StateMachine<LockState> stateMachine;
  private AsyncLockProxy proxy;

  public DefaultAsyncLock(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine<>(context, LockState.class, UnlockedLockState.class);
  }

  /**
   * If the lock is closed, returning a failed CompletableFuture. Otherwise, calls the given supplier to
   * return the completed future result.
   *
   * @param supplier The supplier to call if the lock is open.
   * @param <T> The future result type.
   * @return A completable future that if this lock is closed is immediately failed.
   */
  protected <T> CompletableFuture<T> checkOpen(Supplier<CompletableFuture<T>> supplier) {
    if (proxy == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Lock closed"));
    }
    return supplier.get();
  }

  @Override
  public CompletableFuture<Void> lock() {
    return checkOpen(proxy::lock);
  }

  @Override
  public CompletableFuture<Boolean> tryLock() {
    return checkOpen(proxy::tryLock);
  }

  @Override
  public CompletableFuture<Void> unlock() {
    return checkOpen(proxy::unlock);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return stateMachine.delete();
  }

  @Override
  public CompletableFuture<AsyncLock> open() {
    return stateMachine.open().thenRun(() -> {
      this.proxy = stateMachine.createProxy(AsyncLockProxy.class);
    }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
