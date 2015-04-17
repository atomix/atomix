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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncReference<T> implements Resource<AsyncReference<T>>, AsyncReferenceProxy<T> {
  private final StateMachine<AsyncReferenceState> stateMachine;
  private final AsyncReferenceProxy<T> proxy;

  @SuppressWarnings("unchecked")
  public AsyncReference(AsyncReferenceConfig config, ClusterConfig cluster) {
    StateMachineConfig stateMachineConfig = new StateMachineConfig(config)
      .withDefaultConsistency(Consistency.STRONG);
    stateMachineConfig.setPartitions(1);
    stateMachine = new StateMachine<>(AsyncReferenceState::new, stateMachineConfig, cluster);
    proxy = stateMachine.createProxy(AsyncReferenceProxy.class);
  }

  @SuppressWarnings("unchecked")
  public AsyncReference(AsyncReferenceConfig config, ClusterConfig cluster, Executor executor) {
    StateMachineConfig stateMachineConfig = new StateMachineConfig(config)
      .withDefaultConsistency(Consistency.STRONG);
    stateMachineConfig.setPartitions(1);
    stateMachine = new StateMachine<>(AsyncReferenceState::new, stateMachineConfig, cluster, executor);
    proxy = stateMachine.createProxy(AsyncReferenceProxy.class);
  }

  @Override
  public String name() {
    return stateMachine.name();
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
  public synchronized CompletableFuture<AsyncReference<T>> open() {
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
   * Asynchronous reference state.
   */
  private static class AsyncReferenceState<T> {
    private AtomicReference<T> value = new AtomicReference<>();

    @Read
    public T get() {
      return this.value.get();
    }

    @Write
    public void set(T value) {
      this.value.set(value);
    }

    @Write
    public T getAndSet(T value) {
      return this.value.getAndSet(value);
    }

    @Write
    public boolean compareAndSet(T expect, T update) {
      return this.value.compareAndSet(expect, update);
    }
  }

}
