// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list.impl;

import io.atomix.core.collection.impl.UnmodifiableAsyncDistributedCollection;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed list.
 */
public class UnmodifiableAsyncDistributedList<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedList<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  private final AsyncDistributedList<E> asyncList;

  public UnmodifiableAsyncDistributedList(AsyncDistributedList<E> delegateCollection) {
    super(delegateCollection);
    this.asyncList = delegateCollection;
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> get(int index) {
    return asyncList.get(index);
  }

  @Override
  public CompletableFuture<E> set(int index, E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Void> add(int index, E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> remove(int index) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Integer> indexOf(Object o) {
    return asyncList.indexOf(o);
  }

  @Override
  public CompletableFuture<Integer> lastIndexOf(Object o) {
    return asyncList.lastIndexOf(o);
  }

  @Override
  public DistributedList<E> sync(Duration operationTimeout) {
    return new BlockingDistributedList<>(this, operationTimeout.toMillis());
  }
}
