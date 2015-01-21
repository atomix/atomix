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
package net.kuujo.copycat.collections.internal.map;

import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.collections.AsyncMultiMap;
import net.kuujo.copycat.collections.AsyncMultiMapProxy;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.state.internal.DefaultStateMachine;
import net.kuujo.copycat.internal.util.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Default asynchronous multimap.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMultiMap<K, V> extends AbstractResource<AsyncMultiMap<K, V>> implements AsyncMultiMap<K, V> {
  private final StateMachine<MultiMapState<K, V>> stateMachine;
  private AsyncMultiMapProxy<K, V> proxy;

  @SuppressWarnings("unchecked")
  public DefaultAsyncMultiMap(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine(context, MultiMapState.class, DefaultMultiMapState.class);
  }

  /**
   * If the map is closed, returning a failed CompletableFuture. Otherwise, calls the given supplier to
   * return the completed future result.
   *
   * @param supplier The supplier to call if the map is open.
   * @param <T> The future result type.
   * @return A completable future that if this map is closed is immediately failed.
   */
  protected <T> CompletableFuture<T> checkOpen(Supplier<CompletableFuture<T>> supplier) {
    if (proxy == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Map closed"));
    }
    return supplier.get();
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
  public CompletableFuture<Boolean> containsKey(K key) {
    return checkOpen(() -> proxy.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return checkOpen(() -> proxy.containsValue(value));
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return checkOpen(() -> proxy.containsEntry(key, value));
  }

  @Override
  public CompletableFuture<Collection<V>> get(K key) {
    return checkOpen(() -> proxy.get(key));
  }

  @Override
  public CompletableFuture<Collection<V>> put(K key, V value) {
    return checkOpen(() -> proxy.put(key, value));
  }

  @Override
  public CompletableFuture<Collection<V>> remove(K key) {
    return checkOpen(() -> proxy.remove(key));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return checkOpen(() -> proxy.remove(key, value));
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends Collection<V>> m) {
    return checkOpen(() -> proxy.putAll(m));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return checkOpen(proxy::clear);
  }

  @Override
  public CompletableFuture<Set<K>> keySet() {
    return checkOpen(proxy::keySet);
  }

  @Override
  public CompletableFuture<Collection<Collection<V>>> values() {
    return checkOpen(proxy::values);
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, Collection<V>>>> entrySet() {
    return checkOpen(proxy::entrySet);
  }

  @Override
  public CompletableFuture<Collection<V>> getOrDefault(K key, Collection<V> defaultValue) {
    return checkOpen(() -> proxy.getOrDefault(key, defaultValue));
  }

  @Override
  public CompletableFuture<Void> replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function) {
    return checkOpen(() -> proxy.replaceAll(function));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return checkOpen(() -> proxy.replace(key, oldValue, newValue));
  }

  @Override
  public CompletableFuture<Collection<V>> replace(K key, Collection<V> value) {
    return checkOpen(() -> proxy.replace(key, value));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AsyncMultiMap<K, V>> open() {
    return runStartupTasks()
      .thenCompose(v -> stateMachine.open())
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncMultiMapProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
