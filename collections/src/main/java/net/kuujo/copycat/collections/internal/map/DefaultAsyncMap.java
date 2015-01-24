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

import net.kuujo.copycat.resource.internal.ResourceContext;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.collections.AsyncMap;
import net.kuujo.copycat.collections.AsyncMapProxy;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.internal.DefaultStateMachine;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Default asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMap<K, V> extends AbstractResource<AsyncMap<K, V>> implements AsyncMap<K, V> {
  private final StateMachine<MapState<K, V>> stateMachine;
  private AsyncMapProxy<K, V> proxy;

  @SuppressWarnings("unchecked")
  public DefaultAsyncMap(ResourceContext context) {
    super(context);
    this.stateMachine = new DefaultStateMachine(context, MapState.class, DefaultMapState.class);
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
  public CompletableFuture<Boolean> containsKey(Object key) {
    return checkOpen(() -> proxy.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(Object value) {
    return checkOpen(() -> proxy.containsValue(value));
  }

  @Override
  public CompletableFuture<V> get(Object key) {
    return checkOpen(() -> proxy.get(key));
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return checkOpen(() -> proxy.put(key, value));
  }

  @Override
  public CompletableFuture<V> remove(Object key) {
    return checkOpen(() -> proxy.remove(key));
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
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
  public CompletableFuture<Collection<V>> values() {
    return checkOpen(proxy::values);
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, V>>> entrySet() {
    return checkOpen(proxy::entrySet);
  }

  @Override
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return checkOpen(() -> proxy.getOrDefault(key, defaultValue));
  }

  @Override
  public CompletableFuture<Void> replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    return checkOpen(() -> proxy.replaceAll(function));
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return checkOpen(() -> proxy.putIfAbsent(key, value));
  }

  @Override
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return checkOpen(() -> proxy.remove(key, value));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return checkOpen(() -> proxy.replace(key, oldValue, newValue));
  }

  @Override
  public CompletableFuture<V> replace(K key, V value) {
    return checkOpen(() -> proxy.replace(key, value));
  }

  @Override
  public CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return checkOpen(() -> proxy.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return checkOpen(() -> proxy.computeIfPresent(key, remappingFunction));
  }

  @Override
  public CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return checkOpen(() -> proxy.compute(key, remappingFunction));
  }

  @Override
  public CompletableFuture<V> merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return checkOpen(() -> proxy.merge(key, value, remappingFunction));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AsyncMap<K, V>> open() {
    return runStartupTasks()
      .thenCompose(v -> stateMachine.open())
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncMapProxy.class);
      })
      .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
