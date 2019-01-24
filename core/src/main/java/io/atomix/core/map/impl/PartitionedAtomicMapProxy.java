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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedProxyIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Distributed resource providing the {@link AsyncAtomicMap} primitive.
 */
public abstract class PartitionedAtomicMapProxy<P extends AsyncPrimitive, S extends AtomicMapService<K>, K>
    extends AbstractAsyncPrimitive<P, S>
    implements AsyncAtomicMap<K, byte[]>, AtomicMapClient<K> {
  private final Map<AtomicMapEventListener<K, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  protected PartitionedAtomicMapProxy(ProxyClient<S> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void change(AtomicMapEvent<K, byte[]> event) {
    mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxyClient().applyAll(service -> service.size())
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return getProxyClient().applyBy(key.toString(), service -> service.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return getProxyClient().applyAll(service -> service.containsValue(value))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(K key) {
    return getProxyClient().applyBy(key.toString(), service -> service.get(key));
  }

  @Override
  public CompletableFuture<Map<K, Versioned<byte[]>>> getAllPresent(Iterable<K> keys) {
    return Futures.allOf(getProxyClient().getPartitions()
        .stream()
        .map(partition -> {
          Set<K> uniqueKeys = new HashSet<>();
          for (K key : keys) {
            uniqueKeys.add(key);
          }
          return partition.apply(service -> service.getAllPresent(uniqueKeys));
        })
        .collect(Collectors.toList()))
        .thenApply(maps -> {
          Map<K, Versioned<byte[]>> result = new HashMap<>();
          for (Map<K, Versioned<byte[]>> map : maps) {
            result.putAll(map);
          }
          return ImmutableMap.copyOf(result);
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(K key, byte[] defaultValue) {
    return getProxyClient().applyBy(key.toString(), service -> service.getOrDefault(key, defaultValue));
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return new AtomicMapKeySet();
  }

  @Override
  public AsyncDistributedCollection<Versioned<byte[]>> values() {
    return new AtomicMapValuesCollection();
  }

  @Override
  public AsyncDistributedSet<Entry<K, Versioned<byte[]>>> entrySet() {
    return new AtomicMapEntrySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> put(K key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key.toString(), service -> service.put(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putAndGet(K key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key.toString(), service -> service.putAndGet(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(K key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key.toString(), service -> service.putIfAbsent(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> remove(K key) {
    return getProxyClient().applyBy(key.toString(), service -> service.remove(key))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(K key, byte[] value) {
    return getProxyClient().applyBy(key.toString(), service -> service.remove(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(K key, long version) {
    return getProxyClient().applyBy(key.toString(), service -> service.remove(key, version))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> replace(K key, byte[] value) {
    return getProxyClient().applyBy(key.toString(), service -> service.replace(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(K key, byte[] oldValue, byte[] newValue) {
    return getProxyClient().applyBy(key.toString(), service -> service.replace(key, oldValue, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(K key, long oldVersion, byte[] newValue) {
    return getProxyClient().applyBy(key.toString(), service -> service.replace(key, oldVersion, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptAll(service -> service.clear());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> computeIf(K key,
                                                        Predicate<? super byte[]> condition,
                                                        BiFunction<? super K, ? super byte[], ? extends byte[]> remappingFunction) {
    return get(key).thenCompose(r1 -> {
      byte[] existingValue = r1 == null ? null : r1.value();
      // if the condition evaluates to false, return existing value.
      if (!condition.test(existingValue)) {
        return CompletableFuture.completedFuture(r1);
      }

      byte[] computedValue;
      try {
        computedValue = remappingFunction.apply(key, existingValue);
      } catch (Exception e) {
        return Futures.exceptionalFuture(e);
      }

      if (computedValue == null && r1 == null) {
        return CompletableFuture.completedFuture(null);
      }

      if (r1 == null) {
        return getProxyClient().applyBy(key.toString(), service -> service.putIfAbsent(key, computedValue))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> new Versioned<>(computedValue, result.version()));
      } else if (computedValue == null) {
        return getProxyClient().applyBy(key.toString(), service -> service.remove(key, r1.version()))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(v -> null);
      } else {
        return getProxyClient().applyBy(key.toString(), service -> service.replace(key, r1.version(), computedValue))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> result.status() == MapEntryUpdateResult.Status.OK
                ? new Versioned(computedValue, result.version()) : result.result());
      }
    });
  }

  private CompletableFuture<MapEntryUpdateResult<K, byte[]>> checkLocked(
      MapEntryUpdateResult<K, byte[]> result) {
    if (result.status() == MapEntryUpdateResult.Status.PRECONDITION_FAILED
        || result.status() == MapEntryUpdateResult.Status.WRITE_LOCK) {
      return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
    }
    return CompletableFuture.completedFuture(result);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicMapEventListener<K, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      mapEventListeners.put(listener, executor);
      return getProxyClient().acceptAll(service -> service.listen()).thenApply(v -> null);
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicMapEventListener<K, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void throwIfLocked(MapEntryUpdateResult<K, byte[]> result) {
    if (result != null) {
      throwIfLocked(result.status());
    }
  }

  private void throwIfLocked(MapEntryUpdateResult.Status status) {
    if (status == MapEntryUpdateResult.Status.WRITE_LOCK) {
      throw new ConcurrentModificationException("Cannot update map: Another transaction in progress");
    }
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
    Map<PartitionId, List<MapUpdate<K, byte[]>>> updatesGroupedByMap = Maps.newIdentityHashMap();
    transactionLog.records().forEach(update -> {
      updatesGroupedByMap.computeIfAbsent(getProxyClient().getPartitionId(update.key().toString()), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<MapUpdate<K, byte[]>>> transactionsByMap =
        Maps.transformValues(updatesGroupedByMap, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsByMap.entrySet()
        .stream()
        .map(e -> getProxyClient().applyOn(e.getKey(), service -> service.prepare(e.getValue()))
            .thenApply(v -> v == PrepareResult.OK || v == PrepareResult.PARTIAL_FAILURE))
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return getProxyClient().applyAll(service -> service.commit(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return getProxyClient().applyAll(service -> service.rollback(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<P> connect() {
    return super.connect()
        .thenCompose(v -> Futures.allOf(getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenRun(() -> getProxyClient().getPartitions().forEach(partition -> {
          partition.addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              partition.accept(service -> service.listen());
            }
          });
        }))
        .thenApply(v -> (P) this);
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }

  /**
   * Provides a view of the AtomicMap's entry set.
   */
  private class AtomicMapEntrySet implements AsyncDistributedSet<Map.Entry<K, Versioned<byte[]>>> {
    private final Map<CollectionEventListener<Map.Entry<K, Versioned<byte[]>>>, AtomicMapEventListener<K, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAtomicMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedAtomicMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<K, Versioned<byte[]>>> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, Maps.immutableEntry(event.key(), event.oldValue())));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return PartitionedAtomicMapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<K, Versioned<byte[]>>> listener) {
      AtomicMapEventListener<K, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAtomicMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> add(Entry<K, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Entry<K, Versioned<byte[]>> element) {
      if (element.getValue().version() > 0) {
        return PartitionedAtomicMapProxy.this.remove(element.getKey(), element.getValue().version());
      } else {
        return PartitionedAtomicMapProxy.this.remove(element.getKey(), element.getValue().value());
      }
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAtomicMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAtomicMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedAtomicMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Entry<K, Versioned<byte[]>> element) {
      return get(element.getKey()).thenApply(versioned -> {
        if (versioned == null) {
          return false;
        } else if (!Arrays.equals(versioned.value(), element.getValue().value())) {
          return false;
        } else if (element.getValue().version() > 0 && versioned.version() != element.getValue().version()) {
          return false;
        }
        return true;
      });
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<Map.Entry<K, Versioned<byte[]>>> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMapService::iterateEntries,
          AtomicMapService::nextEntries,
          AtomicMapService::closeEntries);
    }

    @Override
    public DistributedSet<Entry<K, Versioned<byte[]>>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return PartitionedAtomicMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedAtomicMapProxy.this.delete();
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Entry<K, Versioned<byte[]>>>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Provides a view of the AtomicMap's key set.
   */
  private class AtomicMapKeySet implements AsyncDistributedSet<K> {
    private final Map<CollectionEventListener<K>, AtomicMapEventListener<K, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAtomicMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedAtomicMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<K> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return PartitionedAtomicMapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<K> listener) {
      AtomicMapEventListener<K, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAtomicMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> add(K element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(K element) {
      return PartitionedAtomicMapProxy.this.remove(element).thenApply(value -> value != null);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAtomicMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAtomicMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedAtomicMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(K element) {
      return containsKey(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends K> keys) {
      Map<PartitionId, Collection<K>> partitions = Maps.newHashMap();
      keys.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key.toString()), k -> Lists.newArrayList()).add(key));
      return Futures.allOf(partitions.entrySet().stream()
          .map(entry -> getProxyClient()
              .applyOn(entry.getKey(), service -> service.containsKeys(entry.getValue())))
          .collect(Collectors.toList()))
          .thenApply(results -> results.stream().reduce(Boolean::logicalAnd).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<K> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMapService::iterateKeys,
          AtomicMapService::nextKeys,
          AtomicMapService::closeKeys);
    }

    @Override
    public DistributedSet<K> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return PartitionedAtomicMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedAtomicMapProxy.this.delete();
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<K>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Provides a view of the AtomicMap's values collection.
   */
  private class AtomicMapValuesCollection implements AsyncDistributedCollection<Versioned<byte[]>> {
    private final Map<CollectionEventListener<Versioned<byte[]>>, AtomicMapEventListener<K, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAtomicMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return PartitionedAtomicMapProxy.this.protocol();
    }

    @Override
    public PrimitiveType type() {
      return DistributedCollectionType.instance();
    }

    @Override
    public CompletableFuture<Boolean> add(Versioned<byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Versioned<byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAtomicMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAtomicMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedAtomicMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Versioned<byte[]> element) {
      return containsValue(element.value());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<Versioned<byte[]>> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMapService::iterateValues,
          AtomicMapService::nextValues,
          AtomicMapService::closeValues);
    }

    @Override
    public DistributedCollection<Versioned<byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return PartitionedAtomicMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return PartitionedAtomicMapProxy.this.delete();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<Versioned<byte[]>> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.newValue()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.oldValue()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return PartitionedAtomicMapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<byte[]>> listener) {
      AtomicMapEventListener<K, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAtomicMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }
  }
}
