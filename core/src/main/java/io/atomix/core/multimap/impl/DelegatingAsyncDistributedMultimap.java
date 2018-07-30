/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.multimap.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AsyncDistributedMap;
import io.atomix.core.map.impl.TranscodingAsyncDistributedMap;
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AsyncDistributedMultimap;
import io.atomix.core.multimap.AtomicMultimapEvent;
import io.atomix.core.multimap.AtomicMultimapEventListener;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multimap.DistributedMultimapType;
import io.atomix.core.multimap.MultimapEvent;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Distributed multimap implementation that delegates to an atomic multimap.
 */
public class DelegatingAsyncDistributedMultimap<K, V> extends DelegatingAsyncPrimitive implements AsyncDistributedMultimap<K, V> {
  private final AsyncAtomicMultimap<K, V> atomicMultimap;
  private final Map<MultimapEventListener<K, V>, AtomicMultimapEventListener<K, V>> listenerMap = Maps.newConcurrentMap();

  public DelegatingAsyncDistributedMultimap(AsyncAtomicMultimap<K, V> atomicMultimap) {
    super(atomicMultimap);
    this.atomicMultimap = atomicMultimap;
  }

  @Override
  public PrimitiveType type() {
    return DistributedMultimapType.instance();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return atomicMultimap.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return atomicMultimap.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return atomicMultimap.containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return atomicMultimap.containsValue(value);
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return atomicMultimap.containsEntry(key, value);
  }

  @Override
  public CompletableFuture<Boolean> put(K key, V value) {
    return atomicMultimap.put(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return atomicMultimap.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(K key, Collection<? extends V> values) {
    return atomicMultimap.removeAll(key, values);
  }

  @Override
  public CompletableFuture<Collection<V>> removeAll(K key) {
    return atomicMultimap.removeAll(key).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Boolean> putAll(K key, Collection<? extends V> values) {
    return atomicMultimap.putAll(key, values);
  }

  @Override
  public CompletableFuture<Collection<V>> replaceValues(K key, Collection<V> values) {
    return atomicMultimap.replaceValues(key, values).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return atomicMultimap.clear();
  }

  @Override
  public CompletableFuture<Collection<V>> get(K key) {
    return atomicMultimap.get(key).thenApply(Versioned::valueOrNull);
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return atomicMultimap.keySet();
  }

  @Override
  public AsyncDistributedMultiset<K> keys() {
    return atomicMultimap.keys();
  }

  @Override
  public AsyncDistributedMultiset<V> values() {
    return atomicMultimap.values();
  }

  @Override
  public AsyncDistributedCollection<Map.Entry<K, V>> entries() {
    return atomicMultimap.entries();
  }

  @Override
  public AsyncDistributedMap<K, Collection<V>> asMap() {
    return new TranscodingAsyncDistributedMap<>(atomicMultimap.asMap(), k -> k, k -> k, v -> new Versioned<>(v, 0), Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Void> addListener(MultimapEventListener<K, V> listener, Executor executor) {
    AtomicMultimapEventListener<K, V> atomicListener = new InternalAtomicMultimapEventListener(listener);
    if (listenerMap.putIfAbsent(listener, atomicListener) == null) {
      return atomicMultimap.addListener(atomicListener, executor);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(MultimapEventListener<K, V> listener) {
    AtomicMultimapEventListener<K, V> atomicListener = listenerMap.remove(listener);
    if (atomicListener != null) {
      return atomicMultimap.removeListener(atomicListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public DistributedMultimap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedMultimap<>(this, operationTimeout.toMillis());
  }

  private class InternalAtomicMultimapEventListener implements AtomicMultimapEventListener<K, V> {
    private final MultimapEventListener<K, V> mapListener;

    InternalAtomicMultimapEventListener(MultimapEventListener<K, V> mapListener) {
      this.mapListener = mapListener;
    }

    @Override
    public void event(AtomicMultimapEvent<K, V> event) {
      mapListener.event(new MultimapEvent<>(MultimapEvent.Type.valueOf(event.type().name()), event.key(), event.newValue(), event.oldValue()));
    }
  }
}
