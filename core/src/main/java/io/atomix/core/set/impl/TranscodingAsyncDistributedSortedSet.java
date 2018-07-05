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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncDistributedSortedSet} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedSortedSet} by transcoding operation inputs and outputs.
 *
 * @param <E2> key type of other map
 * @param <E1> key type of this map
 */
public class TranscodingAsyncDistributedSortedSet<E1, E2> extends TranscodingAsyncDistributedSet<E1, E2> implements AsyncDistributedSortedSet<E1> {
  private final AsyncDistributedSortedSet<E2> backingSet;

  public TranscodingAsyncDistributedSortedSet(
      AsyncDistributedSortedSet<E2> backingSet,
      Function<E1, E2> entryEncoder,
      Function<E2, E1> entryDecoder) {
    super(backingSet, entryEncoder, entryDecoder);
    this.backingSet = backingSet;
  }

  @Override
  public AsyncDistributedSortedSet<E1> subSet(E1 fromElement, E1 toElement) {
    return new TranscodingAsyncDistributedSortedSet<>(
        backingSet.subSet(entryEncoder.apply(fromElement), entryEncoder.apply(toElement)),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public AsyncDistributedSortedSet<E1> headSet(E1 toElement) {
    return new TranscodingAsyncDistributedSortedSet<>(
        backingSet.headSet(entryEncoder.apply(toElement)),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public AsyncDistributedSortedSet<E1> tailSet(E1 fromElement) {
    return new TranscodingAsyncDistributedSortedSet<>(
        backingSet.tailSet(entryEncoder.apply(fromElement)),
        entryEncoder,
        entryDecoder);
  }

  @Override
  public CompletableFuture<E1> first() {
    return backingSet.first().thenApply(entryDecoder);
  }

  @Override
  public CompletableFuture<E1> last() {
    return backingSet.last().thenApply(entryDecoder);
  }

  @Override
  public DistributedSortedSet<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedSortedSet<>(this, operationTimeout.toMillis());
  }
}
