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

import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AtomicTreeMap;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link AsyncAtomicTreeMap}.
 */
public class AtomicTreeMapProxy<K extends Comparable<K>> extends AbstractAtomicMapProxy<AsyncAtomicTreeMap<K, byte[]>, AtomicTreeMapService<K>, K> implements AsyncAtomicTreeMap<K, byte[]> {
  public AtomicTreeMapProxy(ProxyClient<AtomicTreeMapService<K>> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  protected K greaterKey(K a, K b) {
    return a.compareTo(b) > 0 ? a : b;
  }

  protected K lesserKey(K a, K b) {
    return a.compareTo(b) < 0 ? a : b;
  }

  protected Map.Entry<K, Versioned<byte[]>> greaterEntry(Map.Entry<K, Versioned<byte[]>> a, Map.Entry<K, Versioned<byte[]>> b) {
    return a.getKey().compareTo(b.getKey()) > 0 ? a : b;
  }

  protected Map.Entry<K, Versioned<byte[]>> lesserEntry(Map.Entry<K, Versioned<byte[]>> a, Map.Entry<K, Versioned<byte[]>> b) {
    return a.getKey().compareTo(b.getKey()) < 0 ? a : b;
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return getProxyClient().applyAll(service -> service.firstKey())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return getProxyClient().applyAll(service -> service.lastKey())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> ceilingEntry(K key) {
    return getProxyClient().applyAll(service -> service.ceilingEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> floorEntry(K key) {
    return getProxyClient().applyAll(service -> service.floorEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> higherEntry(K key) {
    return getProxyClient().applyAll(service -> service.higherEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lowerEntry(K key) {
    return getProxyClient().applyAll(service -> service.lowerEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> firstEntry() {
    return getProxyClient().applyAll(service -> service.firstEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lastEntry() {
    return getProxyClient().applyAll(service -> service.lastEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollFirstEntry() {
    return getProxyClient().applyAll(service -> service.pollFirstEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollLastEntry() {
    return getProxyClient().applyAll(service -> service.pollLastEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return getProxyClient().applyAll(service -> service.lowerKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return getProxyClient().applyAll(service -> service.floorKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return getProxyClient().applyAll(service -> service.ceilingKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return getProxyClient().applyAll(service -> service.higherKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public AsyncAtomicNavigableMap<K, Versioned<byte[]>> descendingMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AtomicTreeMap<K, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicTreeMap<>(this, operationTimeout.toMillis());
  }
}