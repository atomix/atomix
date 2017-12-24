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
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

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
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.POLL_FIRST_ENTRY;
import static io.atomix.core.map.impl.ConsistentTreeMapOperations.POLL_LAST_ENTRY;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

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

  public ConsistentTreeMapProxy(PrimitiveProxy proxy) {
    super(proxy);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public CompletableFuture<String> firstKey() {
    return proxy.invoke(FIRST_KEY, serializer()::decode);
  }

  @Override
  public CompletableFuture<String> lastKey() {
    return proxy.invoke(LAST_KEY, serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> ceilingEntry(String key) {
    return proxy.invoke(CEILING_ENTRY, serializer()::encode, new CeilingEntry(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> floorEntry(String key) {
    return proxy.invoke(FLOOR_ENTRY, serializer()::encode, new FloorEntry(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> higherEntry(String key) {
    return proxy.invoke(HIGHER_ENTRY, serializer()::encode, new HigherEntry(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lowerEntry(String key) {
    return proxy.invoke(LOWER_ENTRY, serializer()::encode, new LowerEntry(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> firstEntry() {
    return proxy.invoke(FIRST_ENTRY, serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lastEntry() {
    return proxy.invoke(LAST_ENTRY, serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> pollFirstEntry() {
    return proxy.invoke(POLL_FIRST_ENTRY, serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> pollLastEntry() {
    return proxy.invoke(POLL_LAST_ENTRY, serializer()::decode);
  }

  @Override
  public CompletableFuture<String> lowerKey(String key) {
    return proxy.invoke(LOWER_KEY, serializer()::encode, new LowerKey(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<String> floorKey(String key) {
    return proxy.invoke(FLOOR_KEY, serializer()::encode, new FloorKey(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<String> ceilingKey(String key) {
    return proxy.invoke(CEILING_KEY, serializer()::encode, new CeilingKey(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<String> higherKey(String key) {
    return proxy.invoke(HIGHER_KEY, serializer()::encode, new HigherKey(key), serializer()::decode);
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