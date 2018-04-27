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
import io.atomix.core.map.impl.ConsistentTreeMapOperations.CeilingEntry;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.CeilingKey;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.FloorEntry;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.FloorKey;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.HigherEntry;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.HigherKey;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.LowerEntry;
import io.atomix.core.map.impl.ConsistentTreeMapOperations.LowerKey;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static io.atomix.core.map.impl.ConsistentTreeMapOperations.CEILING_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.CEILING_KEY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.FIRST_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.FIRST_KEY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.FLOOR_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.FLOOR_KEY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.HIGHER_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.HIGHER_KEY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.LAST_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.LAST_KEY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.LOWER_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.LOWER_KEY;

/**
 * Implementation of {@link io.atomix.core.map.AsyncConsistentTreeMap}.
 */
public class ConsistentTreeMapProxy extends ConsistentMapProxy implements AsyncConsistentTreeMap<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(ConsistentMapOperations.NAMESPACE)
      .register(ConsistentTreeMapOperations.NAMESPACE)
      .register(ConsistentMapEvents.NAMESPACE)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 150)
      .register(ConsistentMapService.TransactionScope.class)
      .register(TransactionLog.class)
      .register(TransactionId.class)
      .register(ConsistentMapService.MapEntryValue.class)
      .register(ConsistentMapService.MapEntryValue.Type.class)
      .register(new HashMap().keySet().getClass())
      .register(TreeMap.class)
      .build());

  public ConsistentTreeMapProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
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
    return this.<String>invokeAll(FIRST_KEY)
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> lastKey() {
    return this.<String>invokeAll(LAST_KEY)
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> ceilingEntry(String key) {
    return this.<CeilingEntry, Map.Entry<String, Versioned<byte[]>>>invokeAll(CEILING_ENTRY, new CeilingEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> floorEntry(String key) {
    return this.<FloorEntry, Map.Entry<String, Versioned<byte[]>>>invokeAll(FLOOR_ENTRY, new FloorEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> higherEntry(String key) {
    return this.<HigherEntry, Map.Entry<String, Versioned<byte[]>>>invokeAll(HIGHER_ENTRY, new HigherEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lowerEntry(String key) {
    return this.<LowerEntry, Map.Entry<String, Versioned<byte[]>>>invokeAll(LOWER_ENTRY, new LowerEntry(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> firstEntry() {
    return this.<Map.Entry<String, Versioned<byte[]>>>invokeAll(FIRST_ENTRY)
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserEntry).orElse(null));
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lastEntry() {
    return this.<Map.Entry<String, Versioned<byte[]>>>invokeAll(LAST_ENTRY)
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterEntry).orElse(null));
  }

  @Override
  public CompletableFuture<String> lowerKey(String key) {
    return this.<LowerKey, String>invokeAll(LOWER_KEY, new LowerKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> floorKey(String key) {
    return this.<FloorKey, String>invokeAll(FLOOR_KEY, new FloorKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::greaterKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> ceilingKey(String key) {
    return this.<CeilingKey, String>invokeAll(CEILING_KEY, new CeilingKey(key))
        .thenApply(results -> results.filter(Objects::nonNull).reduce(this::lesserKey).orElse(null));
  }

  @Override
  public CompletableFuture<String> higherKey(String key) {
    return this.<HigherKey, String>invokeAll(HIGHER_KEY, new HigherKey(key))
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