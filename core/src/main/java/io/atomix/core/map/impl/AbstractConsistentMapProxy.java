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
import io.atomix.core.collection.AsyncDistributedSet;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.DistributedSet;
import io.atomix.core.collection.SetEvent;
import io.atomix.core.collection.SetEventListener;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedSet;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Distributed resource providing the {@link AsyncConsistentMap} primitive.
 */
public abstract class AbstractConsistentMapProxy<P extends AsyncPrimitive, S extends ConsistentMapService>
    extends AbstractAsyncPrimitive<P, S>
    implements AsyncConsistentMap<String, byte[]>, ConsistentMapClient {
  private final Map<MapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  protected AbstractConsistentMapProxy(ProxyClient<S> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void change(MapEvent<String, byte[]> event) {
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
  public CompletableFuture<Boolean> containsKey(String key) {
    return getProxyClient().applyBy(key, service -> service.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return getProxyClient().applyAll(service -> service.containsValue(value))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return getProxyClient().applyBy(key, service -> service.get(key));
  }

  @Override
  public CompletableFuture<Map<String, Versioned<byte[]>>> getAllPresent(Iterable<String> keys) {
    return Futures.allOf(getProxyClient().getPartitions()
        .stream()
        .map(partition -> {
          Set<String> uniqueKeys = new HashSet<>();
          for (String key : keys) {
            uniqueKeys.add(key);
          }
          return partition.apply(service -> service.getAllPresent(uniqueKeys));
        })
        .collect(Collectors.toList()))
        .thenApply(maps -> {
          Map<String, Versioned<byte[]>> result = new HashMap<>();
          for (Map<String, Versioned<byte[]>> map : maps) {
            result.putAll(map);
          }
          return ImmutableMap.copyOf(result);
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
    return getProxyClient().applyBy(key, service -> service.getOrDefault(key, defaultValue));
  }

  @Override
  public AsyncDistributedSet<String> keySet() {
    return new ConsistentMapKeySet();
  }

  @Override
  public AsyncDistributedCollection<Versioned<byte[]>> values() {
    return new ConsistentMapValuesCollection();
  }

  @Override
  public AsyncDistributedSet<Entry<String, Versioned<byte[]>>> entrySet() {
    return new ConsistentMapEntrySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key, service -> service.put(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putAndGet(String key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key, service -> service.putAndGet(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return getProxyClient().applyBy(key, service -> service.putIfAbsent(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return getProxyClient().applyBy(key, service -> service.remove(key))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return getProxyClient().applyBy(key, service -> service.remove(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, long version) {
    return getProxyClient().applyBy(key, service -> service.remove(key, version))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return getProxyClient().applyBy(key, service -> service.replace(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return getProxyClient().applyBy(key, service -> service.replace(key, oldValue, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return getProxyClient().applyBy(key, service -> service.replace(key, oldVersion, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptAll(service -> service.clear());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> computeIf(String key,
                                                        Predicate<? super byte[]> condition,
                                                        BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
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
        return getProxyClient().applyBy(key, service -> service.putIfAbsent(key, computedValue))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> new Versioned<>(computedValue, result.version()));
      } else if (computedValue == null) {
        return getProxyClient().applyBy(key, service -> service.remove(key, r1.version()))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(v -> null);
      } else {
        return getProxyClient().applyBy(key, service -> service.replace(key, r1.version(), computedValue))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> result.status() == MapEntryUpdateResult.Status.OK
                ? new Versioned(computedValue, result.version()) : result.result());
      }
    });
  }

  private CompletableFuture<MapEntryUpdateResult<String, byte[]>> checkLocked(
      MapEntryUpdateResult<String, byte[]> result) {
    if (result.status() == MapEntryUpdateResult.Status.PRECONDITION_FAILED ||
        result.status() == MapEntryUpdateResult.Status.WRITE_LOCK) {
      return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
    }
    return CompletableFuture.completedFuture(result);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(MapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      mapEventListeners.put(listener, executor);
      return getProxyClient().acceptAll(service -> service.listen()).thenApply(v -> null);
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(MapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void throwIfLocked(MapEntryUpdateResult<String, byte[]> result) {
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
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
    Map<PartitionId, List<MapUpdate<String, byte[]>>> updatesGroupedByMap = Maps.newIdentityHashMap();
    transactionLog.records().forEach(update -> {
      updatesGroupedByMap.computeIfAbsent(getProxyClient().getPartitionId(update.key()), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<MapUpdate<String, byte[]>>> transactionsByMap =
        Maps.transformValues(updatesGroupedByMap, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsByMap.entrySet()
        .stream()
        .map(e -> getProxyClient().applyOn(e.getKey(), service -> service.prepare(transactionLog))
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
   * Provides a view of the ConsistentMap's entry set.
   */
  private class ConsistentMapEntrySet implements AsyncDistributedSet<Map.Entry<String, Versioned<byte[]>>> {
    private final Map<SetEventListener<Map.Entry<String, Versioned<byte[]>>>, MapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AbstractConsistentMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AbstractConsistentMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(SetEventListener<Map.Entry<String, Versioned<byte[]>>> listener) {
      MapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new SetEvent<>(name(), SetEvent.Type.ADD, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case REMOVE:
            listener.event(new SetEvent<>(name(), SetEvent.Type.REMOVE, Maps.immutableEntry(event.key(), event.oldValue())));
            break;
          default:
            break;
        }
      };
      eventListeners.put(listener, mapListener);
      return AbstractConsistentMapProxy.this.addListener(mapListener);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(SetEventListener<Map.Entry<String, Versioned<byte[]>>> listener) {
      MapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AbstractConsistentMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> add(Entry<String, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Entry<String, Versioned<byte[]>> element) {
      if (element.getValue().version() > 0) {
        return AbstractConsistentMapProxy.this.remove(element.getKey(), element.getValue().version());
      } else {
        return AbstractConsistentMapProxy.this.remove(element.getKey(), element.getValue().value());
      }
    }

    @Override
    public CompletableFuture<Integer> size() {
      return AbstractConsistentMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AbstractConsistentMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AbstractConsistentMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Entry<String, Versioned<byte[]>> element) {
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
    public CompletableFuture<Boolean> addAll(Collection<? extends Entry<String, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Entry<String, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Entry<String, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Entry<String, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<AsyncIterator<Map.Entry<String, Versioned<byte[]>>>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateEntries())
              .thenApply(iteratorId -> new ConsistentMapPartitionIterator<>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextEntries(iteratorId, position),
                  service -> service.closeEntries(iteratorId)))))
          .thenApply(iterators -> new ConsistentMapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public DistributedSet<Entry<String, Versioned<byte[]>>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Provides a view of the ConsistentMap's key set.
   */
  private class ConsistentMapKeySet implements AsyncDistributedSet<String> {
    private final Map<SetEventListener<String>, MapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AbstractConsistentMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AbstractConsistentMapProxy.this.protocol();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(SetEventListener<String> listener) {
      MapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new SetEvent<>(name(), SetEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new SetEvent<>(name(), SetEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      eventListeners.put(listener, mapListener);
      return AbstractConsistentMapProxy.this.addListener(mapListener);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(SetEventListener<String> listener) {
      MapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AbstractConsistentMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return AbstractConsistentMapProxy.this.remove(element).thenApply(value -> value != null);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return AbstractConsistentMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AbstractConsistentMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AbstractConsistentMapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> keys) {
      Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
      keys.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
      return Futures.allOf(partitions.entrySet().stream()
          .map(entry -> getProxyClient()
              .applyOn(entry.getKey(), service -> service.containsKeys(entry.getValue())))
          .collect(Collectors.toList()))
          .thenApply(results -> results.stream().reduce(Boolean::logicalAnd).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<AsyncIterator<String>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateKeys())
              .thenApply(iteratorId -> new ConsistentMapPartitionIterator<>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextKeys(iteratorId, position),
                  service -> service.closeKeys(iteratorId)))))
          .thenApply(iterators -> new ConsistentMapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public DistributedSet<String> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Provides a view of the ConsistentMap's values collection.
   */
  private class ConsistentMapValuesCollection implements AsyncDistributedCollection<Versioned<byte[]>> {
    @Override
    public String name() {
      return AbstractConsistentMapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AbstractConsistentMapProxy.this.protocol();
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
      return AbstractConsistentMapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AbstractConsistentMapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AbstractConsistentMapProxy.this.clear();
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
    public CompletableFuture<AsyncIterator<Versioned<byte[]>>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateValues())
              .thenApply(iteratorId -> new ConsistentMapPartitionIterator<>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextValues(iteratorId, position),
                  service -> service.closeValues(iteratorId)))))
          .thenApply(iterators -> new ConsistentMapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public DistributedCollection<Versioned<byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Consistent map iterator.
   */
  private class ConsistentMapIterator<T> implements AsyncIterator<T> {
    private final Iterator<AsyncIterator<T>> iterators;
    private volatile AsyncIterator<T> iterator;

    public ConsistentMapIterator(Collection<AsyncIterator<T>> iterators) {
      this.iterators = iterators.iterator();
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      if (iterator == null && iterators.hasNext()) {
        iterator = iterators.next();
      }
      if (iterator == null) {
        return CompletableFuture.completedFuture(false);
      }
      return iterator.hasNext()
          .thenCompose(hasNext -> {
            if (!hasNext) {
              iterator = null;
              return hasNext();
            }
            return CompletableFuture.completedFuture(true);
          });
    }

    @Override
    public CompletableFuture<T> next() {
      if (iterator == null && iterators.hasNext()) {
        iterator = iterators.next();
      }
      if (iterator == null) {
        return Futures.exceptionalFuture(new NoSuchElementException());
      }
      return iterator.next();
    }
  }

  /**
   * Consistent map partition iterator.
   */
  private class ConsistentMapPartitionIterator<T> implements AsyncIterator<T> {
    private final PartitionId partitionId;
    private final long iteratorId;
    private final BiFunction<ConsistentMapService, Integer, ConsistentMapService.Batch<T>> nextFunction;
    private final Consumer<ConsistentMapService> closeFunction;
    private volatile CompletableFuture<ConsistentMapService.Batch<T>> batch;
    private volatile CompletableFuture<Void> closeFuture;

    ConsistentMapPartitionIterator(
        PartitionId partitionId,
        long iteratorId,
        BiFunction<ConsistentMapService, Integer, ConsistentMapService.Batch<T>> nextFunction,
        Consumer<ConsistentMapService> closeFunction) {
      this.partitionId = partitionId;
      this.iteratorId = iteratorId;
      this.nextFunction = nextFunction;
      this.closeFunction = closeFunction;
      this.batch = CompletableFuture.completedFuture(
          new ConsistentMapService.Batch<T>(0, Collections.emptyList()));
    }

    /**
     * Returns the current batch iterator or lazily fetches the next batch from the cluster.
     *
     * @return the next batch iterator
     */
    private CompletableFuture<Iterator<T>> batch() {
      return batch.thenCompose(iterator -> {
        if (iterator != null && !iterator.hasNext()) {
          batch = fetch(iterator.position());
          return batch.thenApply(Function.identity());
        }
        return CompletableFuture.completedFuture(iterator);
      });
    }

    /**
     * Fetches the next batch of entries from the cluster.
     *
     * @param position the position from which to fetch the next batch
     * @return the next batch of entries from the cluster
     */
    private CompletableFuture<ConsistentMapService.Batch<T>> fetch(int position) {
      return getProxyClient().applyOn(partitionId, service -> nextFunction.apply(service, position))
          .thenCompose(batch -> {
            if (batch == null) {
              return close().thenApply(v -> null);
            }
            return CompletableFuture.completedFuture(batch);
          });
    }

    /**
     * Closes the iterator.
     *
     * @return future to be completed once the iterator has been closed
     */
    private CompletableFuture<Void> close() {
      if (closeFuture == null) {
        synchronized (this) {
          if (closeFuture == null) {
            closeFuture = getProxyClient().acceptOn(partitionId, service -> closeFunction.accept(service));
          }
        }
      }
      return closeFuture;
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      return batch().thenApply(iterator -> iterator != null && iterator.hasNext());
    }

    @Override
    public CompletableFuture<T> next() {
      return batch().thenCompose(iterator -> {
        if (iterator == null) {
          return Futures.exceptionalFuture(new NoSuchElementException());
        }
        return CompletableFuture.completedFuture(iterator.next());
      });
    }
  }
}