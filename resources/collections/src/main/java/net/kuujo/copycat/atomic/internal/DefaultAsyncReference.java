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

import net.kuujo.copycat.atomic.AsyncReference;
import net.kuujo.copycat.atomic.AsyncReferenceConfig;
import net.kuujo.copycat.atomic.AsyncReferenceProxy;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default asynchronous atomic reference implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncReference<T> extends AbstractResource<AsyncReference<T>> implements AsyncReference<T> {
  private StateMachine<ReferenceState<T>> stateMachine;
  private AsyncReferenceProxy<T> proxy;

  public DefaultAsyncReference(AsyncReferenceConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public DefaultAsyncReference(AsyncReferenceConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public DefaultAsyncReference(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine(context);
  }

  @Override
  public CompletableFuture<T> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(T value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<T> getAndSet(T value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncReference<T>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncReferenceProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
