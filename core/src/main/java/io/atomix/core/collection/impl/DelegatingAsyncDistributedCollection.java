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
package io.atomix.core.collection.impl;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Delegating distributed collection.
 */
public class DelegatingAsyncDistributedCollection<E> extends DelegatingAsyncPrimitive implements AsyncDistributedCollection<E> {
  private final AsyncDistributedCollection<E> delegateCollection;

  public DelegatingAsyncDistributedCollection(AsyncDistributedCollection<E> delegateCollection) {
    super(delegateCollection);
    this.delegateCollection = delegateCollection;
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return delegateCollection.add(element);
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return delegateCollection.remove(element);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegateCollection.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return delegateCollection.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return delegateCollection.contains(element);
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return delegateCollection.addAll(c);
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return delegateCollection.containsAll(c);
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return delegateCollection.retainAll(c);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return delegateCollection.removeAll(c);
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor) {
    return delegateCollection.addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    return delegateCollection.removeListener(listener);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegateCollection.clear();
  }

  @Override
  public AsyncIterator<E> iterator() {
    return delegateCollection.iterator();
  }

  @Override
  public DistributedCollection<E> sync(Duration operationTimeout) {
    return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
  }
}
