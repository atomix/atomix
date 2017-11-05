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

package io.atomix.primitives.map.impl;

import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.CeilingEntry;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.CeilingKey;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FloorEntry;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FloorKey;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.HigherEntry;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.HigherKey;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LowerEntry;
import io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LowerKey;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.time.Versioned;
import io.atomix.transaction.TransactionId;
import io.atomix.transaction.TransactionLog;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.CEILING_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.CEILING_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FIRST_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FIRST_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FLOOR_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.FLOOR_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.HIGHER_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.HIGHER_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LAST_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LAST_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LOWER_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.LOWER_KEY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.POLL_FIRST_ENTRY;
import static io.atomix.primitives.map.impl.AtomixConsistentTreeMapOperations.POLL_LAST_ENTRY;

/**
 * Implementation of {@link io.atomix.primitives.map.AsyncConsistentTreeMap}.
 */
public class AtomixConsistentTreeMap extends RaftConsistentMap implements AsyncConsistentTreeMap<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftConsistentMapOperations.NAMESPACE)
      .register(AtomixConsistentTreeMapOperations.NAMESPACE)
      .register(RaftConsistentMapEvents.NAMESPACE)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 150)
      .register(RaftConsistentMapService.TransactionScope.class)
      .register(TransactionLog.class)
      .register(TransactionId.class)
      .register(RaftConsistentMapService.MapEntryValue.class)
      .register(RaftConsistentMapService.MapEntryValue.Type.class)
      .register(new HashMap().keySet().getClass())
      .register(TreeMap.class)
      .build());

  public AtomixConsistentTreeMap(RaftProxy proxy) {
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
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> higherEntry(
      String key) {
    return proxy.invoke(HIGHER_ENTRY, serializer()::encode, new HigherEntry(key), serializer()::decode);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lowerEntry(
      String key) {
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
      String upperKey, String lowerKey, boolean inclusiveUpper,
      boolean inclusiveLower) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }
}