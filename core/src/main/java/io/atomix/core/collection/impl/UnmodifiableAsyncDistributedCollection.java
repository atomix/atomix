// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection.impl;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed collection.
 */
public class UnmodifiableAsyncDistributedCollection<E> extends DelegatingAsyncDistributedCollection<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  public UnmodifiableAsyncDistributedCollection(AsyncDistributedCollection<E> delegateCollection) {
    super(delegateCollection);
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }
}
