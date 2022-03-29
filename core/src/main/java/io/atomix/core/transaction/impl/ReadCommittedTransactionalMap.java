// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.map.impl.MapUpdate.Type;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Default transactional map.
 */
public class ReadCommittedTransactionalMap<K, V> extends TransactionalMapParticipant<K, V> {
  private final Map<K, MapUpdate<K, V>> updates = Maps.newConcurrentMap();

  public ReadCommittedTransactionalMap(TransactionId transactionId, AsyncAtomicMap<K, V> consistentMap) {
    super(transactionId, consistentMap);
  }

  @Override
  public ProxyProtocol protocol() {
    return (ProxyProtocol) consistentMap.protocol();
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return consistentMap.get(key).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return consistentMap.get(key).thenApply(Objects::nonNull);
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return consistentMap.get(key)
        .thenApply(versioned -> {
          if (versioned == null) {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.PUT_IF_ABSENT)
                .withKey(key)
                .withValue(value)
                .build());
            return null;
          } else {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.PUT_IF_VERSION_MATCH)
                .withKey(key)
                .withValue(value)
                .withVersion(versioned.version())
                .build());
            return versioned.value();
          }
        });
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return consistentMap.get(key)
        .thenApply(versioned -> {
          if (versioned == null) {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.PUT_IF_ABSENT)
                .withKey(key)
                .withValue(value)
                .build());
            return null;
          } else {
            return versioned.value();
          }
        });
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return consistentMap.get(key)
        .thenApply(versioned -> {
          if (versioned != null) {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.REMOVE_IF_VERSION_MATCH)
                .withKey(key)
                .withVersion(versioned.version())
                .build());
            return versioned.value();
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return consistentMap.get(key)
        .thenApply(versioned -> {
          if (versioned != null && Objects.equals(versioned.value(), value)) {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.REMOVE_IF_VERSION_MATCH)
                .withKey(key)
                .withVersion(versioned.version())
                .build());
            return true;
          }
          return false;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return consistentMap.get(key)
        .thenApply(versioned -> {
          if (versioned != null && Objects.equals(versioned.value(), oldValue)) {
            updates.put(key, MapUpdate.<K, V>builder()
                .withType(Type.PUT_IF_VERSION_MATCH)
                .withKey(key)
                .withValue(newValue)
                .withVersion(versioned.version())
                .build());
            return true;
          }
          return false;
        });
  }

  @Override
  public TransactionLog<MapUpdate<K, V>> log() {
    return new TransactionLog<>(transactionId, 0, Lists.newArrayList(updates.values()));
  }
}
