/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
