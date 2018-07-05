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

import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.impl.TranscodingIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.DistributedNavigableSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncDistributedNavigableSet} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedNavigableSet} by transcoding operation inputs and outputs.
 *
 * @param <E2> key type of other map
 * @param <E1> key type of this map
 */
public class TranscodingAsyncDistributedNavigableSet<E1, E2> extends TranscodingAsyncDistributedSortedSet<E1, E2> implements AsyncDistributedNavigableSet<E1> {
  private final AsyncDistributedNavigableSet<E2> backingSet;

  public TranscodingAsyncDistributedNavigableSet(
      AsyncDistributedNavigableSet<E2> backingSet,
      Function<E1, E2> entryEncoder,
      Function<E2, E1> entryDecoder) {
    super(backingSet, entryEncoder, entryDecoder);
    this.backingSet = backingSet;
  }

  @Override
  public CompletableFuture<E1> lower(E1 e) {
    return backingSet.lower(entryEncoder.apply(e)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> floor(E1 e) {
    return backingSet.floor(entryEncoder.apply(e)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> ceiling(E1 e) {
    return backingSet.ceiling(entryEncoder.apply(e)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> higher(E1 e) {
    return backingSet.higher(entryEncoder.apply(e)).thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> pollFirst() {
    return backingSet.pollFirst().thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> pollLast() {
    return backingSet.pollLast().thenApply(entryDecoder);
  }

  @Override
  public AsyncDistributedNavigableSet<E1> descendingSet() {
    return new TranscodingAsyncDistributedNavigableSet<>(backingSet.descendingSet(), entryEncoder, entryDecoder);
  }

  @Override
  public AsyncIterator<E1> descendingIterator() {
    return new TranscodingIterator<>(backingSet.descendingIterator(), entryDecoder);
  }

  @Override
  public AsyncDistributedNavigableSet<E1> subSet(E1 fromElement, boolean fromInclusive, E1 toElement, boolean toInclusive) {
    return new TranscodingAsyncDistributedNavigableSet<>(
        backingSet.subSet(entryEncoder.apply(fromElement), fromInclusive, entryEncoder.apply(toElement), toInclusive),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public AsyncDistributedNavigableSet<E1> headSet(E1 toElement, boolean inclusive) {
    return new TranscodingAsyncDistributedNavigableSet<>(
        backingSet.headSet(entryEncoder.apply(toElement), inclusive),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public AsyncDistributedNavigableSet<E1> tailSet(E1 fromElement, boolean inclusive) {
    return new TranscodingAsyncDistributedNavigableSet<>(
        backingSet.tailSet(entryEncoder.apply(fromElement), inclusive),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public DistributedNavigableSet<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
  }
}
