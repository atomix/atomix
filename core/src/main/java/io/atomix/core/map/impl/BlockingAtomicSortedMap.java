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

/**
 * Default implementation of {@code AtomicSortedMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingAtomicSortedMap<K extends Comparable<K>, V> extends BlockingAtomicMap<K, V> implements AtomicSortedMap<K, V> {

  private final AsyncAtomicSortedMap<K, V> asyncMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicSortedMap(AsyncAtomicSortedMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap, operationTimeoutMillis);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public K firstKey() {
    return complete(asyncMap.firstKey());
  }

  @Override
  public K lastKey() {
    return complete(asyncMap.lastKey());
  }

  @Override
  public AtomicSortedMap<K, V> subMap(K fromKey, K toKey) {
    return new BlockingAtomicSortedMap<>(asyncMap.subMap(fromKey, toKey), operationTimeoutMillis);
  }

  @Override
  public AtomicSortedMap<K, V> headMap(K toKey) {
    return new BlockingAtomicSortedMap<>(asyncMap.headMap(toKey), operationTimeoutMillis);
  }

  @Override
  public AtomicSortedMap<K, V> tailMap(K fromKey) {
    return new BlockingAtomicSortedMap<>(asyncMap.tailMap(fromKey), operationTimeoutMillis);
  }

  @Override
  public AsyncAtomicSortedMap<K, V> async() {
    return asyncMap;
  }
}
