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
import net.kuujo.copycat.collections.internal.map.MapState;
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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public class AsyncMap<K, V> extends AbstractResource<AsyncMap<K, V>> implements AsyncMapProxy<K, V> {

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create() {
    return create(new AsyncMapConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(Executor executor) {
    return create(new AsyncMapConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(String name) {
    return create(new AsyncMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(String name, Executor executor) {
    return create(new AsyncMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(String name, ClusterConfig cluster) {
    return create(new AsyncMapConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncMapConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param config The asynchronous map configuration.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(AsyncMapConfig config, ClusterConfig cluster) {
    return new AsyncMap<>(config, cluster);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param config The asynchronous map configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  public static <K, V> AsyncMap<K, V> create(AsyncMapConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncMap<>(config, cluster, executor);
  }

  private final StateMachine<MapState<K, V>> stateMachine;
  private AsyncMapProxy<K, V> proxy;

  public AsyncMap(AsyncMapConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncMap(AsyncMapConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public AsyncMap(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(context);
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
  public synchronized CompletableFuture<AsyncMap<K, V>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncMapProxy.class);
      })
      .thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
