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
package io.atomix.core.set.impl;

import io.atomix.core.cache.CacheConfig;
import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedSortedSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Caching sorted set.
 */
public class CachingAsyncDistributedSortedSet<E extends Comparable<E>>
    extends CachingAsyncDistributedSet<E>
    implements AsyncDistributedSortedSet<E> {
  private final AsyncDistributedSortedSet<E> set;

  public CachingAsyncDistributedSortedSet(AsyncDistributedSortedSet<E> set, CacheConfig cacheConfig) {
    super(set, cacheConfig);
    this.set = set;
  }

  @Override
  public AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement) {
    return set.subSet(fromElement, toElement);
  }

  @Override
  public AsyncDistributedSortedSet<E> headSet(E toElement) {
    return set.headSet(toElement);
  }

  @Override
  public AsyncDistributedSortedSet<E> tailSet(E fromElement) {
    return set.tailSet(fromElement);
  }

  @Override
  public CompletableFuture<E> first() {
    return set.first();
  }

  @Override
  public CompletableFuture<E> last() {
    return set.last();
  }

  @Override
  public DistributedSortedSet<E> sync(Duration timeout) {
    return new BlockingDistributedSortedSet<>(this, timeout.toMillis());
  }
}
