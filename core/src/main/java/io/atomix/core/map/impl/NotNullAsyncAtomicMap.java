/*
 * Copyright 2017-present Open Networking Foundation
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
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * {@link AsyncAtomicMap} that doesn't allow null values.
 */
public class NotNullAsyncAtomicMap<K, V> extends DelegatingAsyncAtomicMap<K, V> {

  public NotNullAsyncAtomicMap(AsyncAtomicMap<K, V> delegateMap) {
    super(delegateMap);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    if (value == null) {
      return CompletableFuture.completedFuture(false);
    }
    return super.containsValue(value);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return super.get(key).thenApply(v -> v != null && v.value() == null ? null : v);
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
    return super.getAllPresent(keys).thenApply(m -> ImmutableMap.copyOf(m.entrySet()
        .stream().filter(e -> e.getValue().value() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return super.getOrDefault(key, defaultValue).thenApply(v -> v != null && v.value() == null ? null : v);
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value) {
    if (value == null) {
      return super.remove(key);
    }
    return super.put(key, value);
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
    if (value == null) {
      return super.remove(key).thenApply(v -> null);
    }
    return super.putAndGet(key, value);
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
    if (value == null) {
      return super.remove(key);
    }
    return super.putIfAbsent(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    if (value == null) {
      return CompletableFuture.completedFuture(false);
    }
    return super.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return super.remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    if (value == null) {
      return super.remove(key);
    }
    return super.replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    if (oldValue == null) {
      return super.putIfAbsent(key, newValue).thenApply(Objects::isNull);
    } else if (newValue == null) {
      return super.remove(key, oldValue);
    }
    return super.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return super.replace(key, oldVersion, newValue);
  }
}
