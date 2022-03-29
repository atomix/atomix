// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
