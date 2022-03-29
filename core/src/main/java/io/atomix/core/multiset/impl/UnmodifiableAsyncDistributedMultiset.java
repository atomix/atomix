// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multiset.impl;

import com.google.common.collect.Multiset;
import io.atomix.core.collection.impl.UnmodifiableAsyncDistributedCollection;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed multiset.
 */
public class UnmodifiableAsyncDistributedMultiset<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedMultiset<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  private final AsyncDistributedMultiset<E> asyncMultiset;

  public UnmodifiableAsyncDistributedMultiset(AsyncDistributedMultiset<E> delegateCollection) {
    super(delegateCollection);
    this.asyncMultiset = delegateCollection;
  }

  @Override
  public CompletableFuture<Integer> count(Object element) {
    return asyncMultiset.count(element);
  }

  @Override
  public CompletableFuture<Integer> add(E element, int occurrences) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Integer> remove(Object element, int occurrences) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Integer> setCount(E element, int count) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> setCount(E element, int oldCount, int newCount) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public AsyncDistributedSet<E> elementSet() {
    return asyncMultiset.elementSet();
  }

  @Override
  public AsyncDistributedSet<Multiset.Entry<E>> entrySet() {
    return asyncMultiset.entrySet();
  }

  @Override
  public DistributedMultiset<E> sync(Duration operationTimeout) {
    return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
  }
}
