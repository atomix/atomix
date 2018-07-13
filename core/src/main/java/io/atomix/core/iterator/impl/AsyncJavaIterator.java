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
