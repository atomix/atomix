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
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapException;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.map.impl.ConsistentMapOperations.ContainsKey;
import io.atomix.core.map.impl.ConsistentMapOperations.ContainsValue;
import io.atomix.core.map.impl.ConsistentMapOperations.Get;
import io.atomix.core.map.impl.ConsistentMapOperations.GetAllPresent;
import io.atomix.core.map.impl.ConsistentMapOperations.GetOrDefault;
import io.atomix.core.map.impl.ConsistentMapOperations.Put;
import io.atomix.core.map.impl.ConsistentMapOperations.Remove;
import io.atomix.core.map.impl.ConsistentMapOperations.RemoveValue;
import io.atomix.core.map.impl.ConsistentMapOperations.RemoveVersion;
import io.atomix.core.map.impl.ConsistentMapOperations.Replace;
import io.atomix.core.map.impl.ConsistentMapOperations.ReplaceValue;
import io.atomix.core.map.impl.ConsistentMapOperations.ReplaceVersion;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionCommit;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionPrepare;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionRollback;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
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

import static io.atomix.core.map.impl.ConsistentMapEvents.CHANGE;
import static io.atomix.core.map.impl.ConsistentMapOperations.ADD_LISTENER;
import static io.atomix.core.map.impl.ConsistentMapOperations.CLEAR;
import static io.atomix.core.map.impl.ConsistentMapOperations.COMMIT;
import static io.atomix.core.map.impl.ConsistentMapOperations.CONTAINS_KEY;
import static io.atomix.core.map.impl.ConsistentMapOperations.CONTAINS_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.ENTRY_SET;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET_ALL_PRESENT;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET_OR_DEFAULT;
import static io.atomix.core.map.impl.ConsistentMapOperations.KEY_SET;
import static io.atomix.core.map.impl.ConsistentMapOperations.PREPARE;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT_AND_GET;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT_IF_ABSENT;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_LISTENER;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_VERSION;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE_VERSION;
import static io.atomix.core.map.impl.ConsistentMapOperations.ROLLBACK;
import static io.atomix.core.map.impl.ConsistentMapOperations.SIZE;
import static io.atomix.core.map.impl.ConsistentMapOperations.VALUES;

/**
 * Distributed resource providing the {@link AsyncConsistentMap} primitive.
 */
public class ConsistentMapProxy extends AbstractAsyncPrimitive<AsyncConsistentMap<String, byte[]>> implements AsyncConsistentMap<String, byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(ConsistentMapOperations.NAMESPACE)
      .register(ConsistentMapEvents.NAMESPACE)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
      .build());

  private final Map<MapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  public ConsistentMapProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  protected Serializer serializer() {
    return SERIALIZER;
  }

  private void handleEvent(List<MapEvent<String, byte[]>> events) {
    events.forEach(event ->
        mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.<Integer>invokes(SIZE, serializer()::decode)
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return invoke(key, CONTAINS_KEY, serializer()::encode, new ContainsKey(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return this.<ContainsValue, Boolean>invokes(
        CONTAINS_VALUE,
        SERIALIZER::encode,
        new ContainsValue(value),
        SERIALIZER::decode)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return invoke(key, GET, serializer()::encode, new Get(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map<String, Versioned<byte[]>>> getAllPresent(Iterable<String> keys) {
    return Futures.allOf(getPartitions()
        .stream()
        .map(partition -> {
          Set<String> uniqueKeys = new HashSet<>();
          for (String key : keys) {
            uniqueKeys.add(key);
          }
          return partition.<GetAllPresent, Map<String, Versioned<byte[]>>>invoke(
              GET_ALL_PRESENT,
              serializer()::encode,
              new GetAllPresent(uniqueKeys),
              serializer()::decode);
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
    return invoke(
        key,
        GET_OR_DEFAULT,
        serializer()::encode,
        new GetOrDefault(key, defaultValue),
        serializer()::decode);
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return this.<Set<String>>invokes(KEY_SET, SERIALIZER::decode)
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableSet.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableSet.of()));
  }

  @Override
  public CompletableFuture<Collection<Versioned<byte[]>>> values() {
    return this.<Collection<Versioned<byte[]>>>invokes(VALUES, SERIALIZER::decode)
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableList.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableList.of()));
  }

  @Override
  public CompletableFuture<Set<Entry<String, Versioned<byte[]>>>> entrySet() {
    return this.<Set<Map.Entry<String, Versioned<byte[]>>>>invokes(ENTRY_SET, SERIALIZER::decode)
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableSet.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableSet.of()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return this.<Put, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        PUT,
        serializer()::encode,
        new Put(key, value, ttl.toMillis()),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putAndGet(String key, byte[] value, Duration ttl) {
    return this.<Put, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        PUT_AND_GET,
        serializer()::encode,
        new Put(key, value, ttl.toMillis()),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return this.<Put, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        PUT_IF_ABSENT,
        serializer()::encode,
        new Put(key, value, ttl.toMillis()),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return this.<Remove, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REMOVE,
        serializer()::encode,
        new Remove(key),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return this.<RemoveValue, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REMOVE_VALUE,
        serializer()::encode,
        new RemoveValue(key, value),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(String key, long version) {
    return this.<RemoveVersion, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REMOVE_VERSION,
        serializer()::encode,
        new RemoveVersion(key, version),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return this.<Replace, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REPLACE,
        serializer()::encode,
        new Replace(key, value),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.result());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return this.<ReplaceValue, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REPLACE_VALUE,
        serializer()::encode,
        new ReplaceValue(key, oldValue, newValue),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return this.<ReplaceVersion, MapEntryUpdateResult<String, byte[]>>invoke(
        key,
        REPLACE_VERSION,
        serializer()::encode,
        new ReplaceVersion(key, oldVersion, newValue),
        serializer()::decode)
        .whenComplete((r, e) -> throwIfLocked(r))
        .thenApply(v -> v.updated());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return CompletableFuture.allOf(getPartitions()
        .stream()
        .map(partition -> partition.<MapEntryUpdateResult.Status>invoke(CLEAR, serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> null))
        .toArray(CompletableFuture[]::new));
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
        return this.<Put, MapEntryUpdateResult<String, byte[]>>invoke(
            key,
            PUT_IF_ABSENT,
            serializer()::encode,
            new Put(key, computedValue, 0),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(result -> new Versioned<>(computedValue, result.version()));
      } else if (computedValue == null) {
        return this.<RemoveVersion, MapEntryUpdateResult<String, byte[]>>invoke(
            key,
            REMOVE_VERSION,
            serializer()::encode,
            new RemoveVersion(key, r1.version()),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenCompose(r -> checkLocked(r))
            .thenApply(v -> null);
      } else {
        return this.<ReplaceVersion, MapEntryUpdateResult<String, byte[]>>invoke(
            key,
            REPLACE_VERSION,
            serializer()::encode,
            new ReplaceVersion(key, r1.version(), computedValue),
            serializer()::decode)
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
      return Futures.exceptionalFuture(new ConsistentMapException.ConcurrentModification());
    }
    return CompletableFuture.completedFuture(result);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(MapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      return invokes(ADD_LISTENER);
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(MapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return invokes(REMOVE_LISTENER);
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
        .map(e -> this.<TransactionPrepare, PrepareResult>invoke(
            e.getKey(),
            PREPARE,
            serializer()::encode,
            new TransactionPrepare(transactionLog),
            serializer()::decode)
            .thenApply(v -> v == PrepareResult.OK || v == PrepareResult.PARTIAL_FAILURE))
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return this.<TransactionCommit, CommitResult>invokes(
        COMMIT,
        serializer()::encode,
        new TransactionCommit(transactionId),
        serializer()::decode)
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return this.invokes(
        ROLLBACK,
        serializer()::encode,
        new TransactionRollback(transactionId),
        serializer()::decode)
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<AsyncConsistentMap<String, byte[]>> connect() {
    return super.connect()
        .thenRun(() -> getPartitions().forEach(partition -> {
          partition.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
          partition.addStateChangeListener(state -> {
            if (state == PartitionProxy.State.CONNECTED && isListening()) {
              partition.invoke(ADD_LISTENER);
            }
          });
        }))
        .thenApply(v -> this);
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }

  @Override
  public ConsistentMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingConsistentMap<>(this, operationTimeout.toMillis());
  }
}