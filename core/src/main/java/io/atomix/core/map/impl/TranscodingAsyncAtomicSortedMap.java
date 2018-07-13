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
