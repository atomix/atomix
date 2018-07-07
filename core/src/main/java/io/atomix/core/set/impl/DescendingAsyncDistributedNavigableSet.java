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

import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Descending navigable set.
 */
public class DescendingAsyncDistributedNavigableSet<E extends Comparable<E>> implements AsyncDistributedNavigableSet<E> {
  private final AsyncDistributedNavigableSet<E> set;

  public DescendingAsyncDistributedNavigableSet(AsyncDistributedNavigableSet<E> set) {
    this.set = set;
  }

  @Override
  public String name() {
    return set.name();
  }

  @Override
  public PrimitiveType type() {
    return set.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return set.protocol();
  }

  @Override
  public CompletableFuture<E> lower(E e) {
    return set.higher(e);
  }

  @Override
  public CompletableFuture<E> floor(E e) {
    return set.ceiling(e);
  }

  @Override
  public CompletableFuture<E> ceiling(E e) {
    return set.floor(e);
  }

  @Override
  public CompletableFuture<E> higher(E e) {
    return set.lower(e);
  }

  @Override
  public CompletableFuture<E> pollFirst() {
    return set.pollLast();
  }

  @Override
  public CompletableFuture<E> pollLast() {
    return set.pollFirst();
  }

  @Override
  public AsyncDistributedNavigableSet<E> descendingSet() {
    return set;
  }

  @Override
  public AsyncIterator<E> descendingIterator() {
    return set.iterator();
  }

  @Override
  public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return set.subSet(fromElement, fromInclusive, toElement, toInclusive).descendingSet();
  }

  @Override
  public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
    return set.headSet(toElement, inclusive).descendingSet();
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
    return set.last();
  }

  @Override
  public CompletableFuture<E> last() {
    return set.first();
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return set.add(element);
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return remove(element);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return set.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return set.isEmpty();
  }

  @Override
  public CompletableFuture<Void> clear() {
    return set.clear();
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return set.contains(element);
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return set.addAll(c);
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return set.containsAll(c);
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return retainAll(c);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return set.removeAll(c);
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
    return set.addListener(listener);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    return set.removeListener(listener);
  }

  @Override
  public AsyncIterator<E> iterator() {
    return set.descendingIterator();
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return set.prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return set.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return set.rollback(transactionId);
  }

  @Override
  public CompletableFuture<Void> close() {
    return set.close();
  }

  @Override
  public DistributedNavigableSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
  }
}