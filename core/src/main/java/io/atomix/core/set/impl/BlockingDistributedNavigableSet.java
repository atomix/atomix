// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.iterator.impl.BlockingIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSet;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Implementation of {@link DistributedNavigableSet} that merely delegates to a {@link AsyncDistributedNavigableSet}
 * and waits for the operation to complete.
 *
 * @param <E> set element type
 */
public class BlockingDistributedNavigableSet<E extends Comparable<E>> extends BlockingDistributedSortedSet<E> implements DistributedNavigableSet<E> {

  private final long operationTimeoutMillis;

  private final AsyncDistributedNavigableSet<E> asyncSet;

  public BlockingDistributedNavigableSet(AsyncDistributedNavigableSet<E> asyncSet, long operationTimeoutMillis) {
    super(asyncSet, operationTimeoutMillis);
    this.asyncSet = asyncSet;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public E lower(E e) {
    return complete(asyncSet.lower(e));
  }

  @Override
  public E floor(E e) {
    return complete(asyncSet.floor(e));
  }

  @Override
  public E ceiling(E e) {
    return complete(asyncSet.ceiling(e));
  }

  @Override
  public E higher(E e) {
    return complete(asyncSet.higher(e));
  }

  @Override
  public E pollFirst() {
    return complete(asyncSet.pollFirst());
  }

  @Override
  public E pollLast() {
    return complete(asyncSet.pollLast());
  }

  @Override
  public NavigableSet<E> descendingSet() {
    return new BlockingDistributedNavigableSet<>(asyncSet.descendingSet(), operationTimeoutMillis);
  }

  @Override
  public Iterator<E> descendingIterator() {
    return new BlockingIterator<>(asyncSet.descendingIterator(), operationTimeoutMillis);
  }

  @Override
  public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return new BlockingDistributedNavigableSet<>(asyncSet.subSet(fromElement, fromInclusive, toElement, toInclusive), operationTimeoutMillis);
  }

  @Override
  public NavigableSet<E> headSet(E toElement, boolean inclusive) {
    return new BlockingDistributedNavigableSet<>(asyncSet.headSet(toElement, inclusive), operationTimeoutMillis);
  }

  @Override
  public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return new BlockingDistributedNavigableSet<>(asyncSet.tailSet(fromElement, inclusive), operationTimeoutMillis);
  }

  @Override
  public AsyncDistributedNavigableSet<E> async() {
    return asyncSet;
  }
}
