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
