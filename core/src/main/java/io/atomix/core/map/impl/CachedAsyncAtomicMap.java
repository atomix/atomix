/*
 * Copyright 2019-present Open Networking Foundation
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

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.AsyncDistributedJavaSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

/**
 * Implementation of {@link io.atomix.core.map.AsyncAtomicMap} that caches the entire map locally, allowing for
 * e.g. local iteration.
 */
public class CachedAsyncAtomicMap<K, V> extends DelegatingAsyncAtomicMap<K, V> {
  private final AsyncAtomicMap<K, V> map;
  private final Map<K, Supplier<CompletableFuture<Versioned<V>>>> cache = new ConcurrentHashMap<>();
  private final Map<AtomicMapEventListener<K, V>, Executor> eventListeners = new ConcurrentHashMap<>();
  private final AtomicMapEventListener<K, V> cacheUpdater = event -> {
    Versioned<V> newValue = event.newValue();
    if (newValue == null) {
      cache.remove(event.key());
    } else {
      cache.put(event.key(), () -> CompletableFuture.completedFuture(newValue));
    }
    eventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
  };
  private final Consumer<PrimitiveState> stateListener = status -> {
    if (status == PrimitiveState.CONNECTED) {
      create();
    }
  };

  public CachedAsyncAtomicMap(AsyncAtomicMap<K, V> delegate) {
    super(delegate);
    this.map = delegate;
    map.addStateChangeListener(stateListener);
  }

  /**
   * Creates the cache.
   *
   * @return a future to be completed once the cache has been created
   */
  CompletableFuture<AsyncAtomicMap<K, V>> create() {
    return map.addListener(cacheUpdater)
        .thenCompose(v -> create(map.entrySet().iterator()));
  }

  private CompletableFuture<AsyncAtomicMap<K, V>> create(AsyncIterator<Map.Entry<K, Versioned<V>>> iterator) {
    CompletableFuture<AsyncAtomicMap<K, V>> future = new CompletableFuture<>();
    create(iterator, future);
    return future;
  }

  private void create(
      AsyncIterator<Map.Entry<K, Versioned<V>>> iterator,
      CompletableFuture<AsyncAtomicMap<K, V>> future) {
    iterator.hasNext().whenComplete((hasNext, hasNextError) -> {
      if (hasNextError == null) {
        if (hasNext) {
          iterator.next().whenComplete((entry, nextError) -> {
            if (nextError == null) {
              cache.put(entry.getKey(), () -> CompletableFuture.completedFuture(entry.getValue()));
              create(iterator, future);
            } else {
              future.completeExceptionally(nextError);
            }
          });
        } else {
          future.complete(this);
        }
      } else {
        future.completeExceptionally(hasNextError);
      }
    });
  }

  private <T> CompletableFuture<T> runOnCache(Supplier<T> supplier) {
    return CompletableFuture.completedFuture(supplier.get());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return runOnCache(cache::size);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return runOnCache(() -> cache.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    containsValue(value, cache.values().iterator(), future);
    return future;
  }

  private void containsValue(V value, Iterator<Supplier<CompletableFuture<Versioned<V>>>> iterator, CompletableFuture<Boolean> future) {
    if (iterator.hasNext()) {
      iterator.next().get().whenComplete((result, error) -> {
        if (error == null) {
          if (Objects.equals(value, result.value())) {
            future.complete(true);
          } else {
            containsValue(value, iterator, future);
          }
        } else {
          future.completeExceptionally(error);
        }
      });
    } else {
      future.complete(false);
    }
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return cache.getOrDefault(key, () -> CompletableFuture.completedFuture(null)).get();
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
    CompletableFuture<Map<K, Versioned<V>>> future = new CompletableFuture<>();
    getAllPresent(keys.iterator(), new HashMap<>(), future);
    return future;
  }

  private void getAllPresent(Iterator<K> iterator, Map<K, Versioned<V>> map, CompletableFuture<Map<K, Versioned<V>>> future) {
    if (iterator.hasNext()) {
      K key = iterator.next();
      Supplier<CompletableFuture<Versioned<V>>> value = cache.get(key);
      if (value != null) {
        value.get().whenComplete((result, error) -> {
          if (error == null) {
            if (result != null) {
              map.put(key, result);
            }
            getAllPresent(iterator, map, future);
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        getAllPresent(iterator, map, future);
      }
    } else {
      future.complete(map);
    }
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return cache.getOrDefault(key, () -> CompletableFuture.completedFuture(new Versioned<>(defaultValue, 0))).get();
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value, Duration ttl) {
    return super.put(key, value, ttl)
        .thenApply(result -> {
          cache.put(key, () -> map.get(key));
          return result;
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value, Duration ttl) {
    return super.putAndGet(key, value, ttl)
        .thenApply(result -> {
          cache.put(key, () -> CompletableFuture.completedFuture(result));
          return result;
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value, Duration ttl) {
    return super.putIfAbsent(key, value, ttl)
        .thenApply(result -> {
          cache.put(key, () -> map.get(key));
          return result;
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return super.remove(key)
        .thenApply(result -> {
          cache.remove(key);
          return result;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return super.remove(key, value)
        .thenApply(result -> {
          if (result) {
            cache.remove(key);
          }
          return result;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return super.remove(key, version)
        .thenApply(result -> {
          if (result) {
            cache.remove(key);
          }
          return result;
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return super.replace(key, value)
        .thenApply(result -> {
          cache.put(key, () -> map.get(key));
          return result;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return super.replace(key, oldValue, newValue)
        .thenApply(result -> {
          if (result) {
            cache.put(key, () -> map.get(key));
          }
          return result;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return super.replace(key, oldVersion, newValue)
        .thenApply(result -> {
          if (result) {
            cache.put(key, () -> map.get(key));
          }
          return result;
        });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return super.clear().thenRun(() -> cache.clear());
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return new AsyncDistributedJavaSet<>(name(), protocol(), cache.keySet());
  }

  @Override
  public AsyncDistributedCollection<Versioned<V>> values() {
    return new Values();
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, Versioned<V>>> entrySet() {
    return new EntrySet();
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMapEventListener<K, V> listener, Executor executor) {
    eventListeners.put(listener, executor);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMapEventListener<K, V> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  private class Values implements AsyncDistributedCollection<Versioned<V>> {
    private final Map<CollectionEventListener<Versioned<V>>, AtomicMapEventListener<K, V>> listenerMap = new ConcurrentHashMap<>();

    @Override
    public String name() {
      return CachedAsyncAtomicMap.this.name();
    }

    @Override
    public PrimitiveType type() {
      return CachedAsyncAtomicMap.this.type();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return CachedAsyncAtomicMap.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Versioned<V> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Versioned<V> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return CachedAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return CachedAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return CachedAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Versioned<V> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Versioned<V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Versioned<V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Versioned<V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Versioned<V>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Versioned<V>> listener, Executor executor) {
      AtomicMapEventListener<K, V> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.newValue()));
            break;
          case UPDATE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.oldValue()));
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.newValue()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.oldValue()));
            break;
        }
      };
      if (listenerMap.putIfAbsent(listener, mapListener) == null) {
        return CachedAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<V>> listener) {
      AtomicMapEventListener<K, V> mapListener = listenerMap.remove(listener);
      if (mapListener != null) {
        return CachedAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public DistributedCollection<Versioned<V>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }

    @Override
    public AsyncIterator<Versioned<V>> iterator() {
      return new ValuesIterator();
    }

    @Override
    public CompletableFuture<Void> close() {
      return CachedAsyncAtomicMap.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return CachedAsyncAtomicMap.this.delete();
    }
  }

  private class ValuesIterator implements AsyncIterator<Versioned<V>> {
    private final Iterator<Supplier<CompletableFuture<Versioned<V>>>> iterator;

    private ValuesIterator() {
      this.iterator = cache.values().iterator();
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      return CompletableFuture.completedFuture(iterator.hasNext());
    }

    @Override
    public CompletableFuture<Versioned<V>> next() {
      return iterator.next().get();
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  private class EntrySet implements AsyncDistributedSet<Map.Entry<K, Versioned<V>>> {
    private final Map<CollectionEventListener<Map.Entry<K, Versioned<V>>>, AtomicMapEventListener<K, V>> listenerMap = new ConcurrentHashMap<>();

    @Override
    public String name() {
      return CachedAsyncAtomicMap.this.name();
    }

    @Override
    public PrimitiveType type() {
      return CachedAsyncAtomicMap.this.type();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return CachedAsyncAtomicMap.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<K, Versioned<V>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<K, Versioned<V>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return CachedAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return CachedAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return CachedAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Map.Entry<K, Versioned<V>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Map.Entry<K, Versioned<V>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Map.Entry<K, Versioned<V>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Map.Entry<K, Versioned<V>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Map.Entry<K, Versioned<V>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<K, Versioned<V>>> listener, Executor executor) {
      AtomicMapEventListener<K, V> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case UPDATE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, Maps.immutableEntry(event.key(), event.oldValue())));
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, Maps.immutableEntry(event.key(), event.oldValue())));
            break;
        }
      };
      if (listenerMap.putIfAbsent(listener, mapListener) == null) {
        return CachedAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<K, Versioned<V>>> listener) {
      AtomicMapEventListener<K, V> mapListener = listenerMap.remove(listener);
      if (mapListener != null) {
        return CachedAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<K, Versioned<V>>> iterator() {
      return new EntrySetIterator();
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Map.Entry<K, Versioned<V>>>> transactionLog) {
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
      return CachedAsyncAtomicMap.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return CachedAsyncAtomicMap.this.delete();
    }

    @Override
    public DistributedSet<Map.Entry<K, Versioned<V>>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }
  }

  private class EntrySetIterator implements AsyncIterator<Map.Entry<K, Versioned<V>>> {
    private final Iterator<Map.Entry<K, Supplier<CompletableFuture<Versioned<V>>>>> iterator;

    EntrySetIterator() {
      this.iterator = cache.entrySet().iterator();
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      return CompletableFuture.completedFuture(iterator.hasNext());
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<V>>> next() {
      Map.Entry<K, Supplier<CompletableFuture<Versioned<V>>>> entry = iterator.next();
      return entry.getValue().get()
          .thenApply(value -> Maps.immutableEntry(entry.getKey(), value));
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }
}
