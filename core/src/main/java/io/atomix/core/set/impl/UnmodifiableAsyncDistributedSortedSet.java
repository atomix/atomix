// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedSortedSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable sorted set.
 */
public class UnmodifiableAsyncDistributedSortedSet<E extends Comparable<E>>
    extends UnmodifiableAsyncDistributedSet<E>
    implements AsyncDistributedSortedSet<E> {
  private final AsyncDistributedSortedSet<E> set;

  public UnmodifiableAsyncDistributedSortedSet(AsyncDistributedSortedSet<E> set) {
    super(set);
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
