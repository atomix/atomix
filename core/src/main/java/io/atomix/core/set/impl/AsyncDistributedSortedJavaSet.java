// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedSortedSet;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed sorted Java set.
 */
public class AsyncDistributedSortedJavaSet<E extends Comparable<E>> extends AsyncDistributedJavaSet<E> implements AsyncDistributedSortedSet<E> {
  private final SortedSet<E> set;

  public AsyncDistributedSortedJavaSet(String name, PrimitiveProtocol protocol, SortedSet<E> set) {
    super(name, protocol, set);
    this.set = set;
  }

  @Override
  public AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement) {
    return new AsyncDistributedSortedJavaSet<>(name(), protocol(), set.subSet(fromElement, toElement));
  }

  @Override
  public AsyncDistributedSortedSet<E> headSet(E toElement) {
    return new AsyncDistributedSortedJavaSet<>(name(), protocol(), set.headSet(toElement));
  }

  @Override
  public AsyncDistributedSortedSet<E> tailSet(E fromElement) {
    return new AsyncDistributedSortedJavaSet<>(name(), protocol(), set.tailSet(fromElement));
  }

  @Override
  public CompletableFuture<E> first() {
    return CompletableFuture.completedFuture(set.first());
  }

  @Override
  public CompletableFuture<E> last() {
    return CompletableFuture.completedFuture(set.last());
  }

  @Override
  public DistributedSortedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSortedSet<>(this, operationTimeout.toMillis());
  }
}
