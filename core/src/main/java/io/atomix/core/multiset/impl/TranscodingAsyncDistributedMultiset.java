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
package io.atomix.core.multiset.impl;

import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.TranscodingAsyncDistributedSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Transcoding multiset.
 */
public class TranscodingAsyncDistributedMultiset<E1, E2> extends TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedMultiset<E1> {
  private final AsyncDistributedMultiset<E2> backingMultiset;
  private final Function<E1, E2> elementEncoder;
  private final Function<E2, E1> elementDecoder;

  public TranscodingAsyncDistributedMultiset(
      AsyncDistributedMultiset<E2> backingMultiset,
      Function<E1, E2> elementEncoder,
      Function<E2, E1> elementDecoder) {
    super(backingMultiset, elementEncoder, elementDecoder);
    this.backingMultiset = backingMultiset;
    this.elementEncoder = k -> k == null ? null : elementEncoder.apply(k);
    this.elementDecoder = k -> k == null ? null : elementDecoder.apply(k);
  }

  @Override
  public CompletableFuture<Integer> count(Object element) {
    return backingMultiset.count(elementEncoder.apply((E1) element));
  }

  @Override
  public CompletableFuture<Integer> add(E1 element, int occurrences) {
    return backingMultiset.add(elementEncoder.apply(element), occurrences);
  }

  @Override
  public CompletableFuture<Integer> remove(Object element, int occurrences) {
    return backingMultiset.remove(elementEncoder.apply((E1) element), occurrences);
  }

  @Override
  public CompletableFuture<Integer> setCount(E1 element, int count) {
    return backingMultiset.setCount(elementEncoder.apply(element), count);
  }

  @Override
  public CompletableFuture<Boolean> setCount(E1 element, int oldCount, int newCount) {
    return backingMultiset.setCount(elementEncoder.apply(element), oldCount, newCount);
  }

  @Override
  public AsyncDistributedSet<E1> elementSet() {
    return new TranscodingAsyncDistributedSet<>(backingMultiset.elementSet(), elementEncoder, elementDecoder);
  }

  @Override
  public AsyncDistributedSet<Multiset.Entry<E1>> entrySet() {
    Function<Multiset.Entry<E1>, Multiset.Entry<E2>> entryEncoder = entry ->
        Multisets.immutableEntry(elementEncoder.apply(entry.getElement()), entry.getCount());
    Function<Multiset.Entry<E2>, Multiset.Entry<E1>> entryDecoder = entry ->
        Multisets.immutableEntry(elementDecoder.apply(entry.getElement()), entry.getCount());
    return new TranscodingAsyncDistributedSet<>(backingMultiset.entrySet(), entryEncoder, entryDecoder);
  }

  @Override
  public DistributedMultiset<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
  }
}
