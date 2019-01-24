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
package io.atomix.core.set.impl;

import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Descending navigable set.
 */
public class DescendingAsyncDistributedNavigableSet<E extends Comparable<E>>
    extends DelegatingAsyncPrimitive<AsyncDistributedNavigableSet<E>>
    implements AsyncDistributedNavigableSet<E> {
  public DescendingAsyncDistributedNavigableSet(AsyncDistributedNavigableSet<E> primitive) {
    super(primitive);
  }

  @Override
  public String name() {
    return delegate().name();
  }

  @Override
  public PrimitiveType type() {
    return delegate().type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return delegate().protocol();
  }

  @Override
  public CompletableFuture<E> lower(E e) {
    return delegate().higher(e);
  }

  @Override
  public CompletableFuture<E> floor(E e) {
    return delegate().ceiling(e);
  }

  @Override
  public CompletableFuture<E> ceiling(E e) {
    return delegate().floor(e);
  }

  @Override
  public CompletableFuture<E> higher(E e) {
    return delegate().lower(e);
  }

  @Override
  public CompletableFuture<E> pollFirst() {
    return delegate().pollLast();
  }

  @Override
  public CompletableFuture<E> pollLast() {
    return delegate().pollFirst();
  }

  @Override
  public AsyncDistributedNavigableSet<E> descendingSet() {
    return delegate();
  }

  @Override
  public AsyncIterator<E> descendingIterator() {
    return delegate().iterator();
  }

  @Override
  public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return delegate().subSet(fromElement, fromInclusive, toElement, toInclusive).descendingSet();
  }

  @Override
  public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
    return delegate().headSet(toElement, inclusive).descendingSet();
  }

  @Override
  public AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return tailSet(fromElement, inclusive).descendingSet();
  }

  @Override
  public AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement) {
    return subSet(fromElement, true, toElement, false);
  }

  @Override
  public AsyncDistributedSortedSet<E> headSet(E toElement) {
    return headSet(toElement, false);
  }

  @Override
  public AsyncDistributedSortedSet<E> tailSet(E fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public CompletableFuture<E> first() {
    return delegate().last();
  }

  @Override
  public CompletableFuture<E> last() {
    return delegate().first();
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return delegate().add(element);
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return delegate().remove(element);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegate().size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return delegate().isEmpty();
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegate().clear();
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return delegate().contains(element);
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return delegate().addAll(c);
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return delegate().containsAll(c);
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return delegate().retainAll(c);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return delegate().removeAll(c);
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor) {
    return delegate().addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    return delegate().removeListener(listener);
  }

  @Override
  public AsyncIterator<E> iterator() {
    return delegate().descendingIterator();
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return delegate().prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return delegate().commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return delegate().rollback(transactionId);
  }

  @Override
  public CompletableFuture<Void> close() {
    return delegate().close();
  }

  @Override
  public DistributedNavigableSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
  }
}
