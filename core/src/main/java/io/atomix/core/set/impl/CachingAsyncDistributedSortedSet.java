// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
