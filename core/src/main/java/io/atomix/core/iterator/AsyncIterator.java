// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator;

import io.atomix.core.iterator.impl.BlockingIterator;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous iterator.
 */
public interface AsyncIterator<T> {

  /**
   * Returns whether the iterator has a next item.
   *
   * @return whether a next item exists in the iterator
   */
  CompletableFuture<Boolean> hasNext();

  /**
   * Returns the next item in the iterator.
   *
   * @return the next item in the iterator
   */
  CompletableFuture<T> next();

  /**
   * Closes the iterator.
   *
   * @return a future to be completed once the iterator has been closed
   */
  CompletableFuture<Void> close();

  /**
   * Returns a synchronous iterator.
   *
   * @return the synchronous iterator
   */
  default Iterator<T> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  /**
   * Returns a synchronous iterator.
   *
   * @param timeout the iterator operation timeout
   * @return the synchronous iterator
   */
  default Iterator<T> sync(Duration timeout) {
    return new BlockingIterator<>(this, timeout.toMillis());
  }
}
