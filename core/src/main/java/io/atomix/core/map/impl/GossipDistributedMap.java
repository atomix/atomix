/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.map.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.impl.AsyncDistributedJavaCollection;
import io.atomix.core.map.AsyncDistributedMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.AsyncDistributedJavaSet;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.MapProtocol;
import io.atomix.primitive.protocol.map.MapProtocolEventListener;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Gossip-based distributed map.
 */
public class GossipDistributedMap<K, V> implements AsyncDistributedMap<K, V> {
  private final String name;
  private final PrimitiveProtocol protocol;
  private final MapProtocol<K, V> map;

  private final Map<MapEventListener<K, V>, MapProtocolEventListener<K, V>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedMap(String name, PrimitiveProtocol protocol, MapProtocol<K, V> map) {
    this.name = name;
    this.protocol = protocol;
    this.map = map;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return DistributedMapType.instance();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return protocol;
  }

  @Override
  public CompletableFuture<Integer> size() {
    return complete(() -> map.size());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return complete(() -> map.isEmpty());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return complete(() -> map.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return complete(() -> map.containsValue(value));
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return complete(() -> map.get(key));
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return complete(() -> map.put(key, value));
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return complete(() -> map.remove(key));
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
    return complete(() -> map.putAll(m));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return complete(() -> map.clear());
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return new AsyncDistributedJavaSet<>(map.keySet());
  }

  @Override
  public AsyncDistributedCollection<V> values() {
    return new AsyncDistributedJavaCollection<>(map.values());
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, V>> entrySet() {
    return new AsyncDistributedJavaSet<>(map.entrySet());
  }

  @Override
  public CompletableFuture<V> getOrDefault(K key, V defaultValue) {
    return complete(() -> map.getOrDefault(key, defaultValue));
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return complete(() -> map.putIfAbsent(key, value));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return complete(() -> map.remove(key, value));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return complete(() -> map.replace(key, oldValue, newValue));
  }

  @Override
  public CompletableFuture<V> replace(K key, V value) {
    return complete(() -> map.replace(key, value));
  }

  @Override
  public CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return CompletableFuture.completedFuture(map.compute(key, (k, v) -> {
      if (v == null) {
        return mappingFunction.apply(key);
      }
      return v;
    }));
  }

  @Override
  public CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return CompletableFuture.completedFuture(map.compute(key, (k, v) -> {
      if (v != null) {
        return remappingFunction.apply(k, v);
      }
      return v;
    }));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return CompletableFuture.completedFuture((V) map.compute(key, (BiFunction) remappingFunction));
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
    MapProtocolEventListener<K, V> eventListener = event -> executor.execute(() -> {
      switch (event.type()) {
        case INSERT:
          listener.event(new MapEvent<>(MapEvent.Type.INSERT, event.key(), event.value(), null));
          break;
        case UPDATE:
          listener.event(new MapEvent<>(MapEvent.Type.UPDATE, event.key(), event.value(), null));
          break;
        case REMOVE:
          listener.event(new MapEvent<>(MapEvent.Type.REMOVE, event.key(), null, event.value()));
          break;
        default:
          throw new AssertionError();
      }
    });
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      map.addListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
    MapProtocolEventListener<K, V> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      map.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return complete(() -> map.close());
  }

  private CompletableFuture<Void> complete(Runnable runnable) {
    try {
      runnable.run();
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  private <T> CompletableFuture<T> complete(Supplier<T> supplier) {
    try {
      return CompletableFuture.completedFuture(supplier.get());
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public DistributedMap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedMap<>(this, operationTimeout.toMillis());
  }
}
