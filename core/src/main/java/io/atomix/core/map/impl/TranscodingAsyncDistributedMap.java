/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.map.AsyncDistributedMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.TranscodingAsyncDistributedSet;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@code AsyncDistributedMap} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedMap} by transcoding operation inputs and outputs.
 *
 * @param <K2> key type of other map
 * @param <V2> value type of other map
 * @param <K1> key type of this map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncDistributedMap<K1, V1, K2, V2> extends DelegatingAsyncPrimitive implements AsyncDistributedMap<K1, V1> {

  private final AsyncDistributedMap<K2, V2> backingMap;
  private final Function<K1, K2> keyEncoder;
  private final Function<K2, K1> keyDecoder;
  private final Function<V2, V1> valueDecoder;
  private final Function<V1, V2> valueEncoder;
  private final Function<Entry<K2, V2>, Entry<K1, V1>> entryDecoder;
  private final Function<Entry<K1, V1>, Entry<K2, V2>> entryEncoder;
  private final Map<MapEventListener<K1, V1>, InternalBackingMapEventListener> listeners =
      Maps.newIdentityHashMap();

  public TranscodingAsyncDistributedMap(
      AsyncDistributedMap<K2, V2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    super(backingMap);
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
    this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
    this.entryDecoder = e -> e == null ? null : Maps.immutableEntry(keyDecoder.apply(e.getKey()), valueDecoder.apply(e.getValue()));
    this.entryEncoder = e -> e == null ? null : Maps.immutableEntry(keyEncoder.apply(e.getKey()), valueEncoder.apply(e.getValue()));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return backingMap.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K1 key) {
    try {
      return backingMap.containsKey(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V1 value) {
    try {
      return backingMap.containsValue(valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> get(K1 key) {
    try {
      return backingMap.get(keyEncoder.apply(key)).thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> getOrDefault(K1 key, V1 defaultValue) {
    try {
      return backingMap.getOrDefault(keyEncoder.apply(key), valueEncoder.apply(defaultValue))
          .thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> put(K1 key, V1 value) {
    try {
      return backingMap.put(keyEncoder.apply(key), valueEncoder.apply(value))
          .thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K1, ? extends V1> map) {
    return backingMap.putAll(map.entrySet().stream()
        .map(e -> entryEncoder.apply((Map.Entry<K1, V1>) e))
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
  }

  @Override
  public CompletableFuture<V1> remove(K1 key) {
    try {
      return backingMap.remove(keyEncoder.apply(key)).thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> computeIfAbsent(K1 key, Function<? super K1, ? extends V1> mappingFunction) {
    return backingMap.computeIfAbsent(keyEncoder.apply(key), (k) ->
        valueEncoder.apply(mappingFunction.apply(keyDecoder.apply(k))))
        .thenApply(valueDecoder);
  }

  @Override
  public CompletableFuture<V1> computeIfPresent(K1 key, BiFunction<? super K1, ? super V1, ? extends V1> remappingFunction) {
    return backingMap.computeIfPresent(keyEncoder.apply(key), (k, v) ->
        valueEncoder.apply(remappingFunction.apply(keyDecoder.apply(k), valueDecoder.apply(v))))
        .thenApply(valueDecoder);
  }

  @Override
  public CompletableFuture<V1> compute(K1 key, BiFunction<? super K1, ? super V1, ? extends V1> remappingFunction) {
    return backingMap.compute(keyEncoder.apply(key), (k, v) ->
        valueEncoder.apply(remappingFunction.apply(keyDecoder.apply(k), valueDecoder.apply(v))))
        .thenApply(valueDecoder);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return backingMap.clear();
  }

  @Override
  public AsyncDistributedSet<K1> keySet() {
    return new TranscodingAsyncDistributedSet<>(backingMap.keySet(), keyEncoder, keyDecoder);
  }

  @Override
  public AsyncDistributedCollection<V1> values() {
    return new TranscodingAsyncDistributedCollection<>(backingMap.values(), valueEncoder, valueDecoder);
  }

  @Override
  public AsyncDistributedSet<Entry<K1, V1>> entrySet() {
    return new TranscodingAsyncDistributedSet<>(backingMap.entrySet(), entryEncoder, entryDecoder);
  }

  @Override
  public CompletableFuture<V1> putIfAbsent(K1 key, V1 value) {
    try {
      return backingMap.putIfAbsent(keyEncoder.apply(key), valueEncoder.apply(value))
          .thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, V1 value) {
    try {
      return backingMap.remove(keyEncoder.apply(key), valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V1> replace(K1 key, V1 value) {
    try {
      return backingMap.replace(keyEncoder.apply(key), valueEncoder.apply(value))
          .thenApply(valueDecoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> replace(K1 key, V1 oldValue, V1 newValue) {
    try {
      return backingMap.replace(keyEncoder.apply(key),
          valueEncoder.apply(oldValue),
          valueEncoder.apply(newValue));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> lock(K1 key) {
    return backingMap.lock(keyEncoder.apply(key));
  }

  @Override
  public CompletableFuture<Boolean> tryLock(K1 key) {
    return backingMap.tryLock(keyEncoder.apply(key));
  }

  @Override
  public CompletableFuture<Boolean> tryLock(K1 key, Duration timeout) {
    return backingMap.tryLock(keyEncoder.apply(key), timeout);
  }

  @Override
  public CompletableFuture<Boolean> isLocked(K1 key) {
    return backingMap.isLocked(keyEncoder.apply(key));
  }

  @Override
  public CompletableFuture<Void> unlock(K1 key) {
    return backingMap.unlock(keyEncoder.apply(key));
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K1, V1> listener, Executor executor) {
    synchronized (listeners) {
      InternalBackingMapEventListener backingMapListener =
          listeners.computeIfAbsent(listener, k -> new InternalBackingMapEventListener(listener));
      return backingMap.addListener(backingMapListener, executor);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K1, V1> listener) {
    synchronized (listeners) {
      InternalBackingMapEventListener backingMapListener = listeners.remove(listener);
      if (backingMapListener != null) {
        return backingMap.removeListener(backingMapListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public DistributedMap<K1, V1> sync(Duration operationTimeout) {
    return new BlockingDistributedMap<>(this, operationTimeout.toMillis());
  }

  private class InternalBackingMapEventListener implements MapEventListener<K2, V2> {

    private final MapEventListener<K1, V1> listener;

    InternalBackingMapEventListener(MapEventListener<K1, V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(MapEvent<K2, V2> event) {
      listener.event(new MapEvent<>(
          event.type(),
          keyDecoder.apply(event.key()),
          event.newValue() != null ? valueDecoder.apply(event.newValue()) : null,
          event.oldValue() != null ? valueDecoder.apply(event.oldValue()) : null));
    }
  }
}
