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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.state.*;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Asynchronous multi map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public class AsyncMultiMap<K, V> implements Resource<AsyncMultiMap<K, V>>, AsyncMultiMapProxy<K, V> {

  /**
   * Returns a new asynchronous map builder.
   *
   * @return A new asynchronous map builder.
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  private final StateMachine<State<K, V>> stateMachine;
  private AsyncMultiMapProxy<K, V> proxy;

  @SuppressWarnings("unchecked")
  public AsyncMultiMap(StateMachine<State<K, V>> stateMachine) {
    this.stateMachine = stateMachine;
    this.proxy = stateMachine.createProxy(AsyncMultiMapProxy.class);
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
  public CompletableFuture<AsyncMultiMap<K, V>> open() {
    return stateMachine.open().thenApply(v -> this);
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
   * Asynchronous map state.
   */
  public static class State<K, V> {
    private Map<K, Collection<V>> map = new HashMap<>();

    @Read
    public int size() {
      return map.size();
    }

    @Read
    public boolean isEmpty() {
      return map.isEmpty();
    }

    @Read
    public boolean containsKey(K key) {
      return map.containsKey(key);
    }

    @Read
    public boolean containsValue(V value) {
      for (Collection<V> values : map.values()) {
        if (values.contains(value)) {
          return true;
        }
      }
      return false;
    }

    @Read
    public boolean containsEntry(K key, V value) {
      return map.containsKey(key) && map.get(key).contains(value);
    }

    @Read
    public Collection<V> get(K key) {
      return map.get(key);
    }

    @Write
    public Collection<V> put(K key, V value) {
      Collection<V> values = map.get(key);
      if (values == null) {
        values = new ArrayList<>();
        map.put(key, values);
      }
      values.add(value);
      return values;
    }

    @Delete
    public Collection<V> remove(K key) {
      return map.remove(key);
    }

    @Delete
    public boolean remove(K key, V value) {
      Collection<V> values = map.get(key);
      if (values != null) {
        boolean result = values.remove(value);
        if (values.isEmpty()) {
          map.remove(key);
        }
        return result;
      }
      return false;
    }

    @Write
    public void putAll(Map<? extends K, ? extends Collection<V>> m) {
      map.putAll(m);
    }

    @Delete
    public void clear() {
      map.clear();
    }

    @Read
    public Set<K> keySet() {
      return map.keySet();
    }

    @Read
    public Collection<V> values() {
      Collection<V> values = new ArrayList<>();
      map.values().forEach(values::addAll);
      return values;
    }

    @Read
    public Set<Map.Entry<K, V>> entrySet() {
      Set<Map.Entry<K, V>> entries = new HashSet<>();
      for (Map.Entry<K, Collection<V>> entry : map.entrySet()) {
        entry.getValue().forEach(value -> entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), value)));
      }
      return entries;
    }

    @Read
    public Collection<V> getOrDefault(K key, Collection<V> defaultValue) {
      return map.getOrDefault(key, defaultValue);
    }

    @Write
    public void replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function) {
      map.replaceAll(function);
    }

    @Write
    public boolean replace(K key, V oldValue, V newValue) {
      Collection<V> values = map.get(key);
      if (values != null && values.remove(oldValue)) {
        values.add(newValue);
        return true;
      }
      return false;
    }

    @Write
    public Collection<V> replace(K key, Collection<V> value) {
      return map.replace(key, value);
    }
  }

  /**
   * Asynchronous map builder.
   */
  public static class Builder<K, V> implements net.kuujo.copycat.Builder<AsyncMultiMap<K, V>> {
    private final StateMachine.Builder<State<K, V>> builder = StateMachine.<State<K, V>>builder().withState(new State<>());

    private Builder() {
    }

    /**
     * Sets the map state log.
     *
     * @param stateLog The map state log.
     * @return The map builder.
     */
    public Builder<K, V> withLog(StateLog stateLog) {
      builder.withLog(stateLog);
      return this;
    }

    @Override
    public AsyncMultiMap<K, V> build() {
      return new AsyncMultiMap<>(builder.build());
    }
  }

}
