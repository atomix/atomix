// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

import io.atomix.core.iterator.AsyncIterator;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous Java iterator.
 */
public class AsyncJavaIterator<E> implements AsyncIterator<E> {
  private final Iterator<E> iterator;

  public AsyncJavaIterator(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return CompletableFuture.completedFuture(iterator.hasNext());
  }

  @Override
  public CompletableFuture<E> next() {
    return CompletableFuture.completedFuture(iterator.next());
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }
}
