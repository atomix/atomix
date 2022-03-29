// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.impl.UnmodifiableAsyncDistributedCollection;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed queue.
 */
public class UnmodifiableAsyncDistributedQueue<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedQueue<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  public UnmodifiableAsyncDistributedQueue(AsyncDistributedCollection<E> delegateCollection) {
    super(delegateCollection);
  }

  @Override
  public CompletableFuture<Boolean> offer(E e) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> remove() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> poll() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> element() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<E> peek() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public DistributedQueue<E> sync(Duration operationTimeout) {
    return new BlockingDistributedQueue<>(this, operationTimeout.toMillis());
  }
}
