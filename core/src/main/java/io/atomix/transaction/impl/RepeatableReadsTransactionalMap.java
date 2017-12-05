/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.transaction.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.map.AsyncConsistentMap;
import io.atomix.map.impl.MapUpdate;
import io.atomix.transaction.TransactionId;
import io.atomix.transaction.TransactionLog;
import io.atomix.utils.time.Versioned;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default transactional map.
 */
public class RepeatableReadsTransactionalMap<K, V> extends TransactionalMapParticipant<K, V> {
  private final Map<K, CompletableFuture<Versioned<V>>> readCache = Maps.newConcurrentMap();
  private final Map<K, V> writeCache = Maps.newConcurrentMap();
  private final Set<K> deleteSet = Sets.newConcurrentHashSet();

  public RepeatableReadsTransactionalMap(TransactionId transactionId, AsyncConsistentMap<K, V> consistentMap) {
    super(transactionId, consistentMap);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    if (deleteSet.contains(key)) {
      return CompletableFuture.completedFuture(null);
    }
    V latest = writeCache.get(key);
    if (latest != null) {
      return CompletableFuture.completedFuture(latest);
    } else {
      return readCache.computeIfAbsent(key, k -> consistentMap.get(k))
          .thenApply(Versioned::valueOrNull);
    }
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return get(key).thenApply(Objects::nonNull);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return get(key)
        .thenApply(latest -> {
          writeCache.put(key, value);
          deleteSet.remove(key);
          return latest;
        });
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return get(key)
        .thenCompose(latest -> {
          if (latest == null) {
            return put(key, value);
          }
          return CompletableFuture.completedFuture(latest);
        });
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return get(key)
        .thenApply(latest -> {
          if (latest != null) {
            writeCache.remove(key);
            deleteSet.add(key);
          }
          return latest;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return get(key)
        .thenCompose(latest -> {
          if (Objects.equals(value, latest)) {
            return remove(key).thenApply(v -> true);
          }
          return CompletableFuture.completedFuture(false);
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return get(key)
        .thenCompose(latest -> {
          if (Objects.equals(oldValue, latest)) {
            return put(key, newValue).thenApply(v -> true);
          }
          return CompletableFuture.completedFuture(false);
        });
  }

  @Override
  public TransactionLog<MapUpdate<K, V>> log() {
    return new TransactionLog<>(transactionId, 0, Stream.concat(
        // 1st stream: delete ops
        deleteSet.stream()
            .map(key -> Pair.of(key, readCache.get(key).join()))
            .filter(e -> e.getValue() != null)
            .map(e -> MapUpdate.<K, V>builder()
                .withType(MapUpdate.Type.REMOVE_IF_VERSION_MATCH)
                .withKey(e.getKey())
                .withVersion(e.getValue().version())
                .build()),
        // 2nd stream: write ops
        writeCache.entrySet().stream()
            .map(e -> {
              Versioned<V> original = readCache.get(e.getKey()).join();
              if (original == null) {
                return MapUpdate.<K, V>builder()
                    .withType(MapUpdate.Type.PUT_IF_ABSENT)
                    .withKey(e.getKey())
                    .withValue(e.getValue())
                    .build();
              } else {
                return MapUpdate.<K, V>builder()
                    .withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
                    .withKey(e.getKey())
                    .withVersion(original.version())
                    .withValue(e.getValue())
                    .build();
              }
            })).collect(Collectors.toList()));
  }
}
