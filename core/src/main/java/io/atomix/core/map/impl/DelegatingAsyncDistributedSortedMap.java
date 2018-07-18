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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicSortedMap;
import io.atomix.core.map.AsyncDistributedSortedMap;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.core.map.DistributedSortedMapType;
import io.atomix.primitive.PrimitiveType;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating asynchronous distributed tree map.
 */
public class DelegatingAsyncDistributedSortedMap<K extends Comparable<K>, V> extends DelegatingAsyncDistributedMap<K, V> implements AsyncDistributedSortedMap<K, V> {
  private final AsyncAtomicSortedMap<K, V> atomicMap;

  public DelegatingAsyncDistributedSortedMap(AsyncAtomicSortedMap<K, V> atomicMap) {
    super(atomicMap);
    this.atomicMap = atomicMap;
  }

  @Override
  public PrimitiveType type() {
    return DistributedSortedMapType.instance();
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return atomicMap.firstKey();
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return atomicMap.lastKey();
  }

  @Override
  public AsyncDistributedSortedMap<K, V> subMap(K fromKey, K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedSortedMap<K, V> headMap(K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedSortedMap<K, V> tailMap(K fromKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedSortedMap<K, V> sync(Duration timeout) {
    return new BlockingDistributedSortedMap<>(this, timeout.toMillis());
  }
}
