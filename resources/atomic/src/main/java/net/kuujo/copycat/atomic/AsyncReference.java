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
import net.kuujo.copycat.state.DiscreteStateLog;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.Write;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncReference<T> implements Resource<AsyncReference<T>>, AsyncReferenceProxy<T> {

  /**
   * Returns a new asynchronous reference builder.
   *
   * @return A new asynchronous reference builder.
   */
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  private final StateMachine<State<T>> stateMachine;
  private final AsyncReferenceProxy<T> proxy;

  @SuppressWarnings("unchecked")
  public AsyncReference(StateMachine<State<T>> stateMachine) {
    this.stateMachine = stateMachine;
    this.proxy = stateMachine.createProxy(AsyncReferenceProxy.class);
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
  public static class State<T> {
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

  /**
   * Asynchronous set builder.
   */
  public static class Builder<T> implements net.kuujo.copycat.Builder<AsyncReference<T>> {
    private final StateMachine.Builder<State<T>> builder = StateMachine.<State<T>>builder().withState(new State<>());

    private Builder() {
    }

    /**
     * Sets the reference state log.
     *
     * @param stateLog The reference state log.
     * @return The reference builder.
     */
    public Builder<T> withLog(DiscreteStateLog stateLog) {
      builder.withLog(stateLog);
      return this;
    }

    @Override
    public AsyncReference<T> build() {
      return new AsyncReference<>(builder.build());
    }
  }

}
