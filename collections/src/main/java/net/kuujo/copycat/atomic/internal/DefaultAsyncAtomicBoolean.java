/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.atomic.internal;

import net.kuujo.copycat.atomic.AsyncAtomicBoolean;
import net.kuujo.copycat.atomic.AsyncAtomicBooleanProxy;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.resource.internal.ResourceContext;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous atomic long implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncAtomicBoolean extends AbstractResource<AsyncAtomicBoolean> implements AsyncAtomicBoolean {
  private StateMachine<AtomicBooleanState> stateMachine;
  private AsyncAtomicBooleanProxy proxy;

  public DefaultAsyncAtomicBoolean(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine<>(context, AtomicBooleanState.class, DefaultAtomicBooleanState.class);
  }

  @Override
  public CompletableFuture<Boolean> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(boolean value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<Boolean> getAndSet(boolean value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(boolean expect, boolean update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncAtomicBoolean> open() {
    return runStartupTasks()
      .thenCompose(v -> stateMachine.open())
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncAtomicBooleanProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
