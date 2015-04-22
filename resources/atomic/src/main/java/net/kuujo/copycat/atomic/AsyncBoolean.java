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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.Write;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous atomic boolean.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncBoolean implements Resource<AsyncBoolean>, AsyncBooleanProxy {
  private final StateMachine<State> stateMachine;
  private final AsyncBooleanProxy proxy;

  public AsyncBoolean(StateMachine<State> stateMachine) {
    this.stateMachine = stateMachine;
    this.proxy = stateMachine.createProxy(AsyncBooleanProxy.class);
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
   * Asynchronous boolean state.
   */
  public static class State {
    private final AtomicBoolean value = new AtomicBoolean();

    @Read
    public boolean get() {
      return this.value.get();
    }

    @Write
    public void set(boolean value) {
      this.value.set(value);
    }

    @Write
    public boolean getAndSet(boolean value) {
      return this.value.getAndSet(value);
    }

    @Write
    public boolean compareAndSet(boolean expect, boolean update) {
      return this.value.compareAndSet(expect, update);
    }

  }

}
