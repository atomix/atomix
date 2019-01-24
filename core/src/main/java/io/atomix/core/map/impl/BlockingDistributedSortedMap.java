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

import io.atomix.core.map.AsyncDistributedSortedMap;
import io.atomix.core.map.DistributedSortedMap;

import java.util.Comparator;

/**
 * Default implementation of {@code ConsistentMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingDistributedSortedMap<K extends Comparable<K>, V> extends BlockingDistributedMap<K, V> implements DistributedSortedMap<K, V> {

  private final long operationTimeoutMillis;
  private final AsyncDistributedSortedMap<K, V> asyncMap;

  public BlockingDistributedSortedMap(AsyncDistributedSortedMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap, operationTimeoutMillis);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Comparator<? super K> comparator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedSortedMap<K, V> subMap(K fromKey, K toKey) {
    return new BlockingDistributedSortedMap<>(asyncMap.subMap(fromKey, toKey), operationTimeoutMillis);
  }

  @Override
  public DistributedSortedMap<K, V> headMap(K toKey) {
    return new BlockingDistributedSortedMap<>(asyncMap.headMap(toKey), operationTimeoutMillis);
  }

  @Override
  public DistributedSortedMap<K, V> tailMap(K fromKey) {
    return new BlockingDistributedSortedMap<>(asyncMap.tailMap(fromKey), operationTimeoutMillis);
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
  public AsyncDistributedSortedMap<K, V> async() {
    return asyncMap;
  }
}
