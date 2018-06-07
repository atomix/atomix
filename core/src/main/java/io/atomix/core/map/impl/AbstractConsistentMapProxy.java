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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitiveProxy;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
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
 * Distributed resource providing the {@link AsyncConsistentMap} primitive.
 */
public abstract class AbstractConsistentMapProxy<P extends AsyncPrimitive, S extends ConsistentMapService>
    extends AbstractAsyncPrimitiveProxy<P, S>
    implements AsyncConsistentMap<String, byte[]>, ConsistentMapClient {
  private final Map<MapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  protected AbstractConsistentMapProxy(Class<S> serviceClass, PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(serviceClass, proxy, registry);
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
    return applyAll(service -> service.size())
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return applyBy(key, service -> service.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return applyAll(service -> service.containsValue(value))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return applyBy(key, service -> service.get(key));
  }

  @Override
  public CompletableFuture<Map<String, Versioned<byte[]>>> getAllPresent(Iterable<String> keys) {
    return Futures.allOf(getPartitionIds()
        .stream()
        .map(partition -> {
          Set<String> uniqueKeys = new HashSet<>();
          for (String key : keys) {
            uniqueKeys.add(key);
          }
          return applyOn(partition, service -> service.getAllPresent(uniqueKeys));
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
    return applyBy(key, service -> service.getOrDefault(key, defaultValue));
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return applyAll(service -> service.keySet())
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableSet.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableSet.of()));
  }

  @Override
  public CompletableFuture<Collection<Versioned<byte[]>>> values() {
    return applyAll(service -> service.values())
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableList.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableList.of()));
  }

  @Override
  public CompletableFuture<Set<Entry<String, Versioned<byte[]>>>> entrySet() {
    return applyAll(service -> service.entrySet())
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableSet.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableSet.of()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return applyBy(key, service -> service.put(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putAndGet(String key, byte[] value, Duration ttl) {
    return applyBy(key, service -> service.putAndGet(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return applyBy(key, service -> service.putIfAbsent(key, value, ttl.toMillis()))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return applyBy(key, service -> service.remove(key))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return applyBy(key, service -> service.remove(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, long version) {
    return applyBy(key, service -> service.remove(key, version))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return applyBy(key, service -> service.replace(key, value))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return applyBy(key, service -> service.replace(key, oldValue, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return applyBy(key, service -> service.replace(key, oldVersion, newValue))
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return acceptAll(service -> service.clear());
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
        return applyBy(key, service -> service.putIfAbsent(key, computedValue))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> new Versioned<>(computedValue, result.version()));
      } else if (computedValue == null) {
        return applyBy(key, service -> service.remove(key, r1.version()))
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(v -> null);
      } else {
        return applyBy(key, service -> service.replace(key, r1.version(), computedValue))
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
      return acceptAll(service -> service.listen()).thenApply(v -> null);
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(MapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return acceptAll(service -> service.unlisten()).thenApply(v -> null);
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
      PartitionProxy partition = getPartition(update.key());
      updatesGroupedByMap.computeIfAbsent(partition.partitionId(), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<MapUpdate<String, byte[]>>> transactionsByMap =
        Maps.transformValues(updatesGroupedByMap, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsByMap.entrySet()
        .stream()
        .map(e -> applyOn(e.getKey(), service -> service.prepare(transactionLog))
            .thenApply(v -> v == PrepareResult.OK || v == PrepareResult.PARTIAL_FAILURE))
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return applyAll(service -> service.commit(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return applyAll(service -> service.rollback(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<P> connect() {
    return super.connect()
        .thenRun(() -> getPartitionIds().forEach(partition -> {
          addStateChangeListenerOn(partition, state -> {
            if (state == PartitionProxy.State.CONNECTED && isListening()) {
              acceptOn(partition, service -> service.listen());
            }
          });
        }))
        .thenApply(v -> (P) this);
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }
}