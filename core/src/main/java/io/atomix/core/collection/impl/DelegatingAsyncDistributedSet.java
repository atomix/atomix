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
package io.atomix.core.collection.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedSet;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedSet;
import io.atomix.core.collection.DistributedSetType;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.primitive.DelegatingAsyncPrimitive;
import io.atomix.primitive.PrimitiveType;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Implementation of {@link AsyncDistributedSet}.
 *
 * @param <E> set entry type
 */
public class DelegatingAsyncDistributedSet<E> extends DelegatingAsyncPrimitive implements AsyncDistributedSet<E> {

  private final AsyncConsistentMap<E, Boolean> backingMap;
  private final Map<CollectionEventListener<E>, MapEventListener<E, Boolean>> listenerMapping = Maps.newIdentityHashMap();

  public DelegatingAsyncDistributedSet(AsyncConsistentMap<E, Boolean> backingMap) {
    super(backingMap);
    this.backingMap = backingMap;
  }

  @Override
  public PrimitiveType type() {
    return DistributedSetType.instance();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return backingMap.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return backingMap.containsKey(element);
  }

  @Override
  public CompletableFuture<Boolean> add(E entry) {
    return backingMap.putIfAbsent(entry, true).thenApply(Objects::isNull);
  }

  @Override
  public CompletableFuture<Boolean> remove(E entry) {
    return backingMap.remove(entry, true);
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return Futures.allOf(c.stream().map(this::contains).collect(Collectors.toList())).thenApply(v ->
        v.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return Futures.allOf(c.stream().map(this::add).collect(Collectors.toList())).thenApply(v ->
        v.stream().reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return Futures.allOf(c.stream().map(this::remove).collect(Collectors.toList())).thenApply(v ->
        v.stream().reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<AsyncIterator<E>> iterator() {
    return backingMap.keySet().iterator();
  }

  @Override
  public CompletableFuture<Void> clear() {
    return backingMap.clear();
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
    MapEventListener<E, Boolean> mapEventListener = mapEvent -> {
      if (mapEvent.type() == MapEvent.Type.INSERT) {
        listener.onEvent(new CollectionEvent<>(CollectionEvent.Type.ADD, mapEvent.key()));
      } else if (mapEvent.type() == MapEvent.Type.REMOVE) {
        listener.onEvent(new CollectionEvent<>(CollectionEvent.Type.REMOVE, mapEvent.key()));
      }
    };
    if (listenerMapping.putIfAbsent(listener, mapEventListener) == null) {
      return backingMap.addListener(mapEventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    MapEventListener<E, Boolean> mapEventListener = listenerMapping.remove(listener);
    if (mapEventListener != null) {
      return backingMap.removeListener(mapEventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<E>(this, operationTimeout.toMillis());
  }
}
