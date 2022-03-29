// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicSortedMap;
import io.atomix.core.map.AtomicSortedMap;

import java.time.Duration;
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
public class TranscodingAsyncAtomicSortedMap<K extends Comparable<K>, V1, V2>
    extends TranscodingAsyncAtomicMap<K, V1, K, V2>
    implements AsyncAtomicSortedMap<K, V1> {
  private final AsyncAtomicSortedMap<K, V2> backingMap;

  public TranscodingAsyncAtomicSortedMap(
      AsyncAtomicSortedMap<K, V2> backingMap,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    super(backingMap, Function.identity(), Function.identity(), valueEncoder, valueDecoder);
    this.backingMap = backingMap;
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return backingMap.firstKey().thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return backingMap.lastKey().thenApply(keyDecoder);
  }

  @Override
  public AsyncAtomicSortedMap<K, V1> subMap(K fromKey, K toKey) {
    return new TranscodingAsyncAtomicSortedMap<>(backingMap.subMap(fromKey, toKey), valueEncoder, valueDecoder);
  }

  @Override
  public AsyncAtomicSortedMap<K, V1> headMap(K toKey) {
    return new TranscodingAsyncAtomicSortedMap<>(backingMap.headMap(toKey), valueEncoder, valueDecoder);
  }

  @Override
  public AsyncAtomicSortedMap<K, V1> tailMap(K fromKey) {
    return new TranscodingAsyncAtomicSortedMap<>(backingMap.tailMap(fromKey), valueEncoder, valueDecoder);
  }

  @Override
  public AtomicSortedMap<K, V1> sync(Duration timeout) {
    return new BlockingAtomicSortedMap<>(this, timeout.toMillis());
  }
}
