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

import io.atomix.core.map.AsyncConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link io.atomix.core.map.AsyncConsistentTreeMap}.
 */
public class ConsistentTreeMapProxy extends AbstractConsistentMapProxy<AsyncConsistentTreeMap<byte[]>, ConsistentTreeMapService> implements AsyncConsistentTreeMap<byte[]> {
  public ConsistentTreeMapProxy(ProxyClient<ConsistentTreeMapService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  protected String greaterKey(String a, String b) {
    return a.compareTo(b) > 0 ? a : b;
  }

  protected String lesserKey(String a, String b) {
    return a.compareTo(b) < 0 ? a : b;
  }

  protected Map.Entry<String, Versioned<byte[]>> greaterEntry(Map.Entry<String, Versioned<byte[]>> a, Map.Entry<String, Versioned<byte[]>> b) {
    return a.getKey().compareTo(b.getKey()) > 0 ? a : b;
  }

  protected Map.Entry<String, Versioned<byte[]>> lesserEntry(Map.Entry<String, Versioned<byte[]>> a, Map.Entry<String, Versioned<byte[]>> b) {
    return a.getKey().compareTo(b.getKey()) < 0 ? a : b;
  }

  @Override
  public CompletableFuture<String> firstKey() {
    return getProxyClient().applyAll(service -> service.firstKey())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> lastKey() {
    return getProxyClient().applyAll(service -> service.lastKey())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> ceilingEntry(String key) {
    return getProxyClient().applyAll(service -> service.ceilingEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> floorEntry(String key) {
    return getProxyClient().applyAll(service -> service.floorEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> higherEntry(String key) {
    return getProxyClient().applyAll(service -> service.higherEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lowerEntry(String key) {
    return getProxyClient().applyAll(service -> service.lowerEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> firstEntry() {
    return getProxyClient().applyAll(service -> service.firstEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lastEntry() {
    return getProxyClient().applyAll(service -> service.lastEntry())
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<String> lowerKey(String key) {
    return getProxyClient().applyAll(service -> service.lowerKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> floorKey(String key) {
    return getProxyClient().applyAll(service -> service.floorKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> ceilingKey(String key) {
    return getProxyClient().applyAll(service -> service.ceilingKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> higherKey(String key) {
    return getProxyClient().applyAll(service -> service.higherKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<NavigableSet<String>> navigableKeySet() {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<NavigableMap<String, byte[]>> subMap(
      String upperKey, String lowerKey, boolean inclusiveUpper, boolean inclusiveLower) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public ConsistentTreeMap<byte[]> sync(Duration operationTimeout) {
    return new BlockingConsistentTreeMap<>(this, operationTimeout.toMillis());
  }
}