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

import net.kuujo.copycat.atomic.AsyncLong;
import net.kuujo.copycat.atomic.AsyncLongConfig;
import net.kuujo.copycat.atomic.AsyncLongProxy;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default asynchronous atomic long implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncLong extends AbstractResource<AsyncLong> implements AsyncLong {
  private StateMachine<LongState> stateMachine;
  private AsyncLongProxy proxy;

  public DefaultAsyncLong(AsyncLongConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public DefaultAsyncLong(AsyncLongConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  public DefaultAsyncLong(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine<>(context);
  }

  @Override
  public CompletableFuture<Long> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<Long> addAndGet(long value) {
    return proxy.addAndGet(value);
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long value) {
    return proxy.getAndAdd(value);
  }

  @Override
  public CompletableFuture<Long> getAndSet(long value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return proxy.getAndIncrement();
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return proxy.getAndDecrement();
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return proxy.incrementAndGet();
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return proxy.decrementAndGet();
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expect, long update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncLong> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncLongProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
