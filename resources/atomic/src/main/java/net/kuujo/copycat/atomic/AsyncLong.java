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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;
import net.kuujo.copycat.state.Write;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous atomic long.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLong implements Resource<AsyncLong>, AsyncLongProxy {
  private final StateMachine<AsyncLongState> stateMachine;
  private final AsyncLongProxy proxy;

  public AsyncLong(AsyncLongConfig config, ClusterConfig cluster) {
    StateMachineConfig stateMachineConfig = new StateMachineConfig(config)
      .withDefaultConsistency(Consistency.STRONG);
    stateMachineConfig.setPartitions(1);
    stateMachine = new StateMachine<>(AsyncLongState::new, stateMachineConfig, cluster);
    proxy = stateMachine.createProxy(AsyncLongProxy.class);
  }

  public AsyncLong(AsyncLongConfig config, ClusterConfig cluster, Executor executor) {
    StateMachineConfig stateMachineConfig = new StateMachineConfig(config)
      .withDefaultConsistency(Consistency.STRONG);
    stateMachineConfig.setPartitions(1);
    stateMachine = new StateMachine<>(AsyncLongState::new, stateMachineConfig, cluster, executor);
    proxy = stateMachine.createProxy(AsyncLongProxy.class);
  }

  @Override
  public String name() {
    return stateMachine.name();
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
    return stateMachine.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return stateMachine.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return stateMachine.close();
  }

  @Override
  public boolean isClosed() {
    return stateMachine.isClosed();
  }

  /**
   * Asynchronous long state.
   */
  private static class AsyncLongState {
    private AtomicLong value = new AtomicLong();

    @Read
    public long get() {
      return this.value.get();
    }

    @Write
    public void set(long value) {
      this.value.set(value);
    }

    @Write
    public long addAndGet(long value) {
      return this.value.addAndGet(value);
    }

    @Write
    public long getAndAdd(long value) {
      return this.value.getAndAdd(value);
    }

    @Write
    public long getAndSet(long value) {
      return this.value.getAndSet(value);
    }

    @Write
    public long getAndIncrement() {
      return this.value.getAndIncrement();
    }

    @Write
    public long getAndDecrement() {
      return this.value.getAndDecrement();
    }

    @Write
    public long incrementAndGet() {
      return this.value.incrementAndGet();
    }

    @Write
    public long decrementAndGet() {
      return this.value.decrementAndGet();
    }

    @Write
    public boolean compareAndSet(long expect, long update) {
      return this.value.compareAndSet(expect, update);
    }
  }

}
