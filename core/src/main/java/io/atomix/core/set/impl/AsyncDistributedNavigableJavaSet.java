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
import io.atomix.core.iterator.impl.AsyncJavaIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed navigable Java set.
 */
public class AsyncDistributedNavigableJavaSet<E extends Comparable<E>> extends AsyncDistributedSortedJavaSet<E> implements AsyncDistributedNavigableSet<E> {
  private final NavigableSet<E> set;

  public AsyncDistributedNavigableJavaSet(String name, PrimitiveProtocol protocol, NavigableSet<E> set) {
    super(name, protocol, set);
    this.set = set;
  }

  @Override
  public CompletableFuture<E> lower(E e) {
    return CompletableFuture.completedFuture(set.lower(e));
  }

  @Override
  public CompletableFuture<E> floor(E e) {
    return CompletableFuture.completedFuture(set.floor(e));
  }

  @Override
  public CompletableFuture<E> ceiling(E e) {
    return CompletableFuture.completedFuture(set.ceiling(e));
  }

  @Override
  public CompletableFuture<E> higher(E e) {
    return CompletableFuture.completedFuture(set.higher(e));
  }

  @Override
  public CompletableFuture<E> pollFirst() {
    return CompletableFuture.completedFuture(set.pollFirst());
  }

  @Override
  public CompletableFuture<E> pollLast() {
    return CompletableFuture.completedFuture(set.pollLast());
  }

  @Override
  public AsyncDistributedNavigableSet<E> descendingSet() {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), set.descendingSet());
  }

  @Override
  public AsyncIterator<E> descendingIterator() {
    return new AsyncJavaIterator<>(set.descendingIterator());
  }

  @Override
  public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), set.subSet(fromElement, fromInclusive, toElement, toInclusive));
  }

  @Override
  public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), set.headSet(toElement, inclusive));
  }

  @Override
  public AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), set.tailSet(fromElement, inclusive));
  }

  @Override
  public DistributedNavigableSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
  }
}
