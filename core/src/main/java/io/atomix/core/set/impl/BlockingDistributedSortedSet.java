// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedSortedSet;

import java.util.Comparator;
import java.util.SortedSet;

/**
 * Implementation of {@link DistributedSortedSet} that merely delegates to a {@link AsyncDistributedSortedSet}
 * and waits for the operation to complete.
 *
 * @param <E> set element type
 */
public class BlockingDistributedSortedSet<E extends Comparable<E>> extends BlockingDistributedSet<E> implements DistributedSortedSet<E> {

  private final long operationTimeoutMillis;

  private final AsyncDistributedSortedSet<E> asyncSet;

  public BlockingDistributedSortedSet(AsyncDistributedSortedSet<E> asyncSet, long operationTimeoutMillis) {
    super(asyncSet, operationTimeoutMillis);
    this.asyncSet = asyncSet;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Comparator<? super E> comparator() {
    return null;
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return new BlockingDistributedSortedSet<>(asyncSet.subSet(fromElement, toElement), operationTimeoutMillis);
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    return new BlockingDistributedSortedSet<>(asyncSet.headSet(toElement), operationTimeoutMillis);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return new BlockingDistributedSortedSet<>(asyncSet.tailSet(fromElement), operationTimeoutMillis);
  }

  @Override
  public E first() {
    return complete(asyncSet.first());
  }

  @Override
  public E last() {
    return complete(asyncSet.last());
  }

  @Override
  public AsyncDistributedSortedSet<E> async() {
    return asyncSet;
  }
}
