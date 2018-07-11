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
