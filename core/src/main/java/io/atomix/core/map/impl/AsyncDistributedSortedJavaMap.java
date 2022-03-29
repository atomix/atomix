// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncDistributedSortedMap;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;

/**
 * Java-map wrapped asynchronous distributed sorted map.
 */
public class AsyncDistributedSortedJavaMap<K extends Comparable<K>, V> extends AsyncDistributedJavaMap<K, V> implements AsyncDistributedSortedMap<K, V> {
  private final SortedMap<K, V> map;

  public AsyncDistributedSortedJavaMap(String name, PrimitiveProtocol protocol, SortedMap<K, V> map) {
    super(name, protocol, map);
    this.map = map;
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return complete(() -> map.firstKey());
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return complete(() -> map.lastKey());
  }

  @Override
  public AsyncDistributedSortedMap<K, V> subMap(K fromKey, K toKey) {
    return new AsyncDistributedSortedJavaMap<>(name(), protocol(), map.subMap(fromKey, toKey));
  }

  @Override
  public AsyncDistributedSortedMap<K, V> headMap(K toKey) {
    return new AsyncDistributedSortedJavaMap<>(name(), protocol(), map.headMap(toKey));
  }

  @Override
  public AsyncDistributedSortedMap<K, V> tailMap(K fromKey) {
    return new AsyncDistributedSortedJavaMap<>(name(), protocol(), map.tailMap(fromKey));
  }

  @Override
  public DistributedSortedMap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedSortedMap<>(this, operationTimeout.toMillis());
  }
}
