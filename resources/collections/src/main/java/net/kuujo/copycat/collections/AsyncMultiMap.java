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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.map.DefaultMultiMapState;
import net.kuujo.copycat.collections.internal.map.MultiMapState;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Asynchronous multi-map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The multimap key type.
 * @param <V> The multimap entry type.
 */
public class AsyncMultiMap<K, V> extends AbstractResource<AsyncMultiMap<K, V>> implements AsyncMultiMapProxy<K, V> {

  /**
   * Creates a new asynchronous multimap with the given cluster.
   *
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  public static <K, V> AsyncMultiMap<K, V> create(ClusterConfig cluster) {
    return new AsyncMultiMap<>(new AsyncMultiMapConfig(), cluster);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster.
   *
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous multimap callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  public static <K, V> AsyncMultiMap<K, V> create(ClusterConfig cluster, Executor executor) {
    return new AsyncMultiMap<>(new AsyncMultiMapConfig(), cluster, executor);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param config The asynchronous multimap configuration.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  public static <K, V> AsyncMultiMap<K, V> create(AsyncMultiMapConfig config, ClusterConfig cluster) {
    return new AsyncMultiMap<>(config, cluster);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param config The asynchronous multimap configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous multimap callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  public static <K, V> AsyncMultiMap<K, V> create(AsyncMultiMapConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncMultiMap<>(config, cluster, executor);
  }

  private final StateMachine<MultiMapState<K, V>> stateMachine;
  private AsyncMultiMapProxy<K, V> proxy;

  public AsyncMultiMap(AsyncMultiMapConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncMultiMap(AsyncMultiMapConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public AsyncMultiMap(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(new DefaultMultiMapState<>(), context);
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
  public CompletableFuture<Collection<V>> values() {
    return checkOpen(proxy::values);
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, V>>> entrySet() {
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
  public synchronized CompletableFuture<AsyncMultiMap<K, V>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncMultiMapProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
