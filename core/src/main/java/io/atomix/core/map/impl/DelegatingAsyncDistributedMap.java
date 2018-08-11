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
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.TranscodingIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AsyncDistributedMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Distributed map implementation that delegates to an atomic map.
 */
public class DelegatingAsyncDistributedMap<K, V> extends DelegatingAsyncPrimitive implements AsyncDistributedMap<K, V> {
  private final AsyncAtomicMap<K, V> atomicMap;
  private final Map<MapEventListener<K, V>, AtomicMapEventListener<K, V>> listenerMap = Maps.newConcurrentMap();

  public DelegatingAsyncDistributedMap(AsyncAtomicMap<K, V> atomicMap) {
    super(atomicMap);
    this.atomicMap = atomicMap;
  }

  @Override
  public PrimitiveType type() {
    return DistributedMapType.instance();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return atomicMap.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return atomicMap.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return atomicMap.containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return atomicMap.containsValue(value);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return atomicMap.get(key).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return atomicMap.put(key, value).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return atomicMap.remove(key).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return atomicMap.clear();
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return atomicMap.keySet();
  }

  @Override
  public AsyncDistributedCollection<V> values() {
    return new UnwrappedValues(atomicMap.values());
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, V>> entrySet() {
    return new UnwrappedEntrySet(atomicMap.entrySet());
  }

  @Override
  public CompletableFuture<V> getOrDefault(K key, V defaultValue) {
    return atomicMap.getOrDefault(key, defaultValue).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return atomicMap.putIfAbsent(key, value).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return atomicMap.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return atomicMap.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<V> replace(K key, V value) {
    return atomicMap.replace(key, value).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return atomicMap.computeIfAbsent(key, mappingFunction).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return atomicMap.computeIfPresent(key, remappingFunction).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return atomicMap.compute(key, remappingFunction).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
    AtomicMapEventListener<K, V> atomicListener = new InternalAtomicMapEventListener<>(listener);
    if (listenerMap.putIfAbsent(listener, atomicListener) == null) {
      return atomicMap.addListener(atomicListener, executor);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
    AtomicMapEventListener<K, V> atomicListener = listenerMap.remove(listener);
    if (atomicListener != null) {
      return atomicMap.removeListener(atomicListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public DistributedMap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedMap<>(this, operationTimeout.toMillis());
  }

  private class UnwrappedValues implements AsyncDistributedCollection<V> {
    private final AsyncDistributedCollection<Versioned<V>> values;
    private final Map<CollectionEventListener<V>, CollectionEventListener<Versioned<V>>> listenerMap = Maps.newConcurrentMap();

    UnwrappedValues(AsyncDistributedCollection<Versioned<V>> values) {
      this.values = values;
    }

    @Override
    public String name() {
      return DelegatingAsyncDistributedMap.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedCollectionType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return DelegatingAsyncDistributedMap.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(V element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(V element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return values.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return values.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return values.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(V element) {
      return values.contains(new Versioned<>(element, 0));
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends V> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends V> c) {
      return values.containsAll((Collection) c.stream().map(value -> new Versioned<>(value, 0)).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends V> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends V> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<V> listener, Executor executor) {
      CollectionEventListener<Versioned<V>> atomicListener = new VersionedCollectionEventListener(listener);
      if (listenerMap.putIfAbsent(listener, atomicListener) == null) {
        return values.addListener(atomicListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<V> listener) {
      CollectionEventListener<Versioned<V>> atomicListener = listenerMap.remove(listener);
      if (atomicListener != null) {
        return values.removeListener(atomicListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<V> iterator() {
      return new TranscodingIterator<>(values.iterator(), Versioned::valueOrNull);
    }

    @Override
    public CompletableFuture<Void> close() {
      return values.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return values.delete();
    }

    @Override
    public DistributedCollection<V> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }

    private class VersionedCollectionEventListener implements CollectionEventListener<Versioned<V>> {
      private final CollectionEventListener<V> listener;

      VersionedCollectionEventListener(CollectionEventListener<V> listener) {
        this.listener = listener;
      }

      @Override
      public void event(CollectionEvent<Versioned<V>> event) {
        listener.event(new CollectionEvent<>(event.type(), Versioned.valueOrNull(event.element())));
      }
    }
  }

  private class UnwrappedEntrySet implements AsyncDistributedSet<Map.Entry<K, V>> {
    private final AsyncDistributedSet<Map.Entry<K, Versioned<V>>> entries;
    private final Map<CollectionEventListener<Map.Entry<K, V>>, CollectionEventListener<Map.Entry<K, Versioned<V>>>> listenerMap = Maps.newConcurrentMap();

    UnwrappedEntrySet(AsyncDistributedSet<Map.Entry<K, Versioned<V>>> entries) {
      this.entries = entries;
    }

    @Override
    public String name() {
      return DelegatingAsyncDistributedMap.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return DelegatingAsyncDistributedMap.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<K, V> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<K, V> element) {
      return DelegatingAsyncDistributedMap.this.remove(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return entries.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return entries.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Map.Entry<K, V> element) {
      return get(element.getKey()).thenApply(value -> Objects.equals(value, element.getValue()));
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Map.Entry<K, V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Map.Entry<K, V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Map.Entry<K, V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Map.Entry<K, V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<K, V>> listener, Executor executor) {
      CollectionEventListener<Map.Entry<K, Versioned<V>>> atomicListener = new VersionedCollectionEventListener(listener);
      if (listenerMap.putIfAbsent(listener, atomicListener) == null) {
        return entries.addListener(atomicListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<K, V>> listener) {
      CollectionEventListener<Map.Entry<K, Versioned<V>>> atomicListener = listenerMap.remove(listener);
      if (atomicListener != null) {
        return entries.removeListener(atomicListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<K, V>> iterator() {
      return new TranscodingIterator<>(entries.iterator(), entry -> Maps.immutableEntry(entry.getKey(), Versioned.valueOrNull(entry.getValue())));
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Map.Entry<K, V>>> transactionLog) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> close() {
      return entries.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return entries.delete();
    }

    @Override
    public DistributedSet<Map.Entry<K, V>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    private class VersionedCollectionEventListener implements CollectionEventListener<Map.Entry<K, Versioned<V>>> {
      private final CollectionEventListener<Map.Entry<K, V>> listener;

      VersionedCollectionEventListener(CollectionEventListener<Map.Entry<K, V>> listener) {
        this.listener = listener;
      }

      @Override
      public void event(CollectionEvent<Map.Entry<K, Versioned<V>>> event) {
        listener.event(new CollectionEvent<>(event.type(), event.element() == null ? null
            : Maps.immutableEntry(event.element().getKey(), Versioned.valueOrNull(event.element().getValue()))));
      }
    }
  }

  private class InternalAtomicMapEventListener<K, V> implements AtomicMapEventListener<K, V> {
    private final MapEventListener<K, V> mapListener;

    InternalAtomicMapEventListener(MapEventListener<K, V> mapListener) {
      this.mapListener = mapListener;
    }

    @Override
    public void event(AtomicMapEvent<K, V> event) {
      switch (event.type()) {
        case INSERT:
          mapListener.event(new MapEvent<>(
              MapEvent.Type.INSERT,
              event.key(),
              Versioned.valueOrNull(event.newValue()),
              Versioned.valueOrNull(event.oldValue())));
          break;
        case UPDATE:
          mapListener.event(new MapEvent<>(
              MapEvent.Type.UPDATE,
              event.key(),
              Versioned.valueOrNull(event.newValue()),
              Versioned.valueOrNull(event.oldValue())));
          break;
        case REMOVE:
          mapListener.event(new MapEvent<>(
              MapEvent.Type.REMOVE,
              event.key(),
              Versioned.valueOrNull(event.newValue()),
              Versioned.valueOrNull(event.oldValue())));
          break;
      }
    }
  }
}
