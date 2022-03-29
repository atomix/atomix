// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
