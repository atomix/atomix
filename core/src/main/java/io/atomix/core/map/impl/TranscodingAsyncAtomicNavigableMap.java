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
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncConsistentMap} that maps its operations to operations on a
 * differently typed {@code AsyncConsistentMap} by transcoding operation inputs and outputs.
 *
 * @param <K>  comparable key type
 * @param <V2> value type of other map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncAtomicNavigableMap<K extends Comparable<K>, V1, V2>
    extends TranscodingAsyncAtomicSortedMap<K, V1, V2>
    implements AsyncAtomicNavigableMap<K, V1> {
  private final AsyncAtomicNavigableMap<K, V2> backingMap;

  public TranscodingAsyncAtomicNavigableMap(
      AsyncAtomicNavigableMap<K, V2> backingMap,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    super(backingMap, valueEncoder, valueDecoder);
    this.backingMap = backingMap;
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> lowerEntry(K key) {
    return backingMap.lowerEntry(keyEncoder.apply(key)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return backingMap.lowerKey(keyEncoder.apply(key)).thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> floorEntry(K key) {
    return backingMap.floorEntry(keyEncoder.apply(key)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return backingMap.floorKey(keyEncoder.apply(key)).thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> ceilingEntry(K key) {
    return backingMap.ceilingEntry(keyEncoder.apply(key)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return backingMap.ceilingKey(keyEncoder.apply(key)).thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> higherEntry(K key) {
    return backingMap.higherEntry(keyEncoder.apply(key)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return backingMap.higherKey(keyEncoder.apply(key)).thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> firstEntry() {
    return backingMap.firstEntry().thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> lastEntry() {
    return backingMap.lastEntry().thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> pollFirstEntry() {
    return backingMap.pollFirstEntry().thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V1>>> pollLastEntry() {
    return backingMap.pollLastEntry().thenApply(entryDecoder);
  }

  @Override
  public AsyncAtomicNavigableMap<K, V1> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new TranscodingAsyncAtomicNavigableMap<>(
        backingMap.subMap(keyEncoder.apply(fromKey), fromInclusive, keyEncoder.apply(toKey), toInclusive),
        valueEncoder,
        valueDecoder);
  }

  @Override
  public AsyncAtomicNavigableMap<K, V1> headMap(K toKey, boolean inclusive) {
    return new TranscodingAsyncAtomicNavigableMap<>(
        backingMap.headMap(keyEncoder.apply(toKey), inclusive),
        valueEncoder,
        valueDecoder);
  }

  @Override
  public AsyncAtomicNavigableMap<K, V1> tailMap(K fromKey, boolean inclusive) {
    return new TranscodingAsyncAtomicNavigableMap<>(
        backingMap.tailMap(keyEncoder.apply(fromKey), inclusive),
        valueEncoder,
        valueDecoder);
  }

  @Override
  public AsyncAtomicNavigableMap<K, V1> descendingMap() {
    return new TranscodingAsyncAtomicNavigableMap<>(backingMap.descendingMap(), valueEncoder, valueDecoder);
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return backingMap.navigableKeySet();
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return backingMap.descendingKeySet();
  }

  @Override
  public AtomicNavigableMap<K, V1> sync(Duration timeout) {
    return new BlockingAtomicNavigableMap<>(this, timeout.toMillis());
  }
}
