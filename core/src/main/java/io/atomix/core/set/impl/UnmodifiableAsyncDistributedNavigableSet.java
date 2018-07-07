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

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable navigable set.
 */
public class UnmodifiableAsyncDistributedNavigableSet<E extends Comparable<E>>
    extends UnmodifiableAsyncDistributedSortedSet<E>
    implements AsyncDistributedNavigableSet<E> {
  private final AsyncDistributedNavigableSet<E> set;

  public UnmodifiableAsyncDistributedNavigableSet(AsyncDistributedNavigableSet<E> set) {
    super(set);
    this.set = set;
  }

  @Override
  public CompletableFuture<E> lower(E e) {
    return set.lower(e);
  }

  @Override
  public CompletableFuture<E> floor(E e) {
    return set.floor(e);
  }

  @Override
  public CompletableFuture<E> ceiling(E e) {
    return set.ceiling(e);
  }

  @Override
  public CompletableFuture<E> higher(E e) {
    return set.higher(e);
  }

  @Override
  public CompletableFuture<E> pollFirst() {
    return set.pollFirst();
  }

  @Override
  public CompletableFuture<E> pollLast() {
    return set.pollLast();
  }

  @Override
  public AsyncDistributedNavigableSet<E> descendingSet() {
    return set.descendingSet();
  }

  @Override
  public AsyncIterator<E> descendingIterator() {
    return set.descendingIterator();
  }

  @Override
  public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return set.subSet(fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
    return set.headSet(toElement, inclusive);
  }

  @Override
  public AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return set.tailSet(fromElement, inclusive);
  }

  @Override
  public DistributedNavigableSet<E> sync(Duration timeout) {
    return new BlockingDistributedNavigableSet<>(this, timeout.toMillis());
  }
}
