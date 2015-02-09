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

import net.kuujo.copycat.atomic.AsyncBoolean;
import net.kuujo.copycat.atomic.AsyncBooleanProxy;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.resource.internal.ResourceManager;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous atomic long implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncBoolean extends AbstractResource<AsyncBoolean> implements AsyncBoolean {
  private StateMachine<BooleanState> stateMachine;
  private AsyncBooleanProxy proxy;

  public DefaultAsyncBoolean(ResourceManager context) {
    super(context);
    this.stateMachine = new DefaultStateMachine<>(context, BooleanState.class, DefaultBooleanState.class);
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
  public synchronized CompletableFuture<AsyncBoolean> open() {
    return runStartupTasks()
      .thenCompose(v -> stateMachine.open())
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncBooleanProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
