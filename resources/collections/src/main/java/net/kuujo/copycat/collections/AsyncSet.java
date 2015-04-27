/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.state.*;
import net.kuujo.copycat.util.concurrent.Futures;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public class AsyncSet<T> implements AsyncCollection<AsyncSet<T>, T>, AsyncSetProxy<T> {

  /**
   * Returns a new asynchronous set builder.
   *
   * @return A new asynchronous set builder.
   */
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  private final StateMachine<State<T>> stateMachine;
  private final AsyncSetProxy<T> proxy;

  @SuppressWarnings("unchecked")
  public AsyncSet(StateMachine<State<T>> stateMachine) {
    this.stateMachine = stateMachine;
    this.proxy = stateMachine.createProxy(AsyncSetProxy.class);
  }

  @Override
  public String name() {
    return stateMachine.name();
  }

  @Override
  public Cluster cluster() {
    return stateMachine.cluster();
  }

  /**
   * If the collection is closed, returning a failed CompletableFuture. Otherwise, calls the given supplier to
   * return the completed future result.
   *
   * @param supplier The supplier to call if the collection is open.
   * @param <R> The future result type.
   * @return A completable future that if this collection is closed is immediately failed.
   */
  protected <R> CompletableFuture<R> checkOpen(Supplier<CompletableFuture<R>> supplier) {
    if (proxy == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Collection closed"));
    }
    return supplier.get();
  }

  @Override
  public CompletableFuture<Boolean> add(T value) {
    return checkOpen(() -> proxy.add(value));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends T> values) {
    return checkOpen(() -> proxy.addAll(values));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<?> values) {
    return checkOpen(() -> proxy.retainAll(values));
  }

  @Override
  public CompletableFuture<Boolean> remove(Object value) {
    return checkOpen(() -> proxy.remove(value));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<?> values) {
    return checkOpen(() -> proxy.removeAll(values));
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value) {
    return checkOpen(() -> proxy.contains(value));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<?> values) {
    return checkOpen(() -> proxy.containsAll(values));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return checkOpen(proxy::size);
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return checkOpen(proxy::isEmpty);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return checkOpen(proxy::clear);
  }

  @Override
  public CompletableFuture<AsyncSet<T>> open() {
    return stateMachine.open().thenApply(r -> this);
  }

  @Override
  public boolean isOpen() {
    return stateMachine.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return stateMachine.close();
  }

  @Override
  public boolean isClosed() {
    return stateMachine.isClosed();
  }

  /**
   * Asynchronous set state.
   */
  public static class State<T> implements Set<T> {
    private final Set<T> state = new HashSet<>();

    @Read
    @Override
    public int size() {
      return state.size();
    }

    @Read
    @Override
    public boolean isEmpty() {
      return state.isEmpty();
    }

    @Read
    @Override
    public boolean contains(Object o) {
      return state.contains(o);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
      throw new UnsupportedOperationException();
    }

    @Read
    @NotNull
    @Override
    public Object[] toArray() {
      return state.toArray();
    }

    @Read
    @NotNull
    @Override
    public <T1> T1[] toArray(T1[] a) {
      return state.toArray(a);
    }

    @Write
    @Override
    public boolean add(T t) {
      return state.add(t);
    }

    @Delete
    @Override
    public boolean remove(Object o) {
      return state.remove(o);
    }

    @Read
    @Override
    public boolean containsAll(Collection<?> c) {
      return state.containsAll(c);
    }

    @Write
    @Override
    public boolean addAll(Collection<? extends T> c) {
      return state.addAll(c);
    }

    @Write
    @Override
    public boolean retainAll(Collection<?> c) {
      return state.retainAll(c);
    }

    @Write
    @Override
    public boolean removeAll(Collection<?> c) {
      return state.removeAll(c);
    }

    @Delete
    @Override
    public void clear() {
      state.clear();
    }
  }

  /**
   * Asynchronous set builder.
   */
  public static class Builder<T> implements net.kuujo.copycat.Builder<AsyncSet<T>> {
    private final StateMachine.Builder<State<T>> builder = StateMachine.<State<T>>builder().withState(new State<>());

    private Builder() {
    }

    /**
     * Sets the set state log.
     *
     * @param stateLog The map state log.
     * @return The map builder.
     */
    public Builder<T> withLog(StateLog stateLog) {
      builder.withLog(stateLog);
      return this;
    }

    @Override
    public AsyncSet<T> build() {
      return new AsyncSet<>(builder.build());
    }
  }

}
