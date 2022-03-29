// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list.impl;

import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transcoding distributed list.
 */
public class TranscodingAsyncDistributedList<E1, E2> extends TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedList<E1> {
  private final AsyncDistributedList<E2> backingList;
  private final Function<E1, E2> elementEncoder;
  private final Function<E2, E1> elementDecoder;

  public TranscodingAsyncDistributedList(AsyncDistributedList<E2> backingList, Function<E1, E2> elementEncoder, Function<E2, E1> elementDecoder) {
    super(backingList, elementEncoder, elementDecoder);
    this.backingList = backingList;
    this.elementEncoder = k -> k == null ? null : elementEncoder.apply(k);
    this.elementDecoder = k -> k == null ? null : elementDecoder.apply(k);
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends E1> c) {
    return backingList.addAll(index, c.stream().map(elementEncoder).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<E1> get(int index) {
    return backingList.get(index).thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<E1> set(int index, E1 element) {
    return backingList.set(index, elementEncoder.apply(element)).thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<Void> add(int index, E1 element) {
    return backingList.add(index, elementEncoder.apply(element));
  }

  @Override
  public CompletableFuture<E1> remove(int index) {
    return backingList.remove(index).thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<Integer> indexOf(Object o) {
    return backingList.indexOf(elementEncoder.apply((E1) o));
  }

  @Override
  public CompletableFuture<Integer> lastIndexOf(Object o) {
    return backingList.lastIndexOf(elementEncoder.apply((E1) o));
  }

  @Override
  public DistributedList<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedList<>(this, operationTimeout.toMillis());
  }
}
