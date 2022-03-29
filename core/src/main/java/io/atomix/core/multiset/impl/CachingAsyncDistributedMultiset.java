// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multiset.impl;

import com.google.common.collect.Multiset;
import io.atomix.core.cache.CacheConfig;
import io.atomix.core.collection.impl.CachingAsyncDistributedCollection;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Caching asynchronous distributed multiset.
 */
public class CachingAsyncDistributedMultiset<E> extends CachingAsyncDistributedCollection<E> implements AsyncDistributedMultiset<E> {
  private final AsyncDistributedMultiset<E> backingMultiset;

  public CachingAsyncDistributedMultiset(AsyncDistributedMultiset<E> backingCollection, CacheConfig config) {
    super(backingCollection, config);
    this.backingMultiset = backingCollection;
  }

  @Override
  public CompletableFuture<Integer> count(Object element) {
    return backingMultiset.count(element);
  }

  @Override
  public CompletableFuture<Integer> add(E element, int occurrences) {
    return backingMultiset.add(element, occurrences).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public CompletableFuture<Integer> remove(Object element, int occurrences) {
    return backingMultiset.remove(element, occurrences).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public CompletableFuture<Integer> setCount(E element, int count) {
    return backingMultiset.setCount(element, count).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public CompletableFuture<Boolean> setCount(E element, int oldCount, int newCount) {
    return backingMultiset.setCount(element, oldCount, newCount).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public AsyncDistributedSet<E> elementSet() {
    return backingMultiset.elementSet();
  }

  @Override
  public AsyncDistributedSet<Multiset.Entry<E>> entrySet() {
    return backingMultiset.entrySet();
  }

  @Override
  public DistributedMultiset<E> sync(Duration operationTimeout) {
    return new BlockingDistributedMultiset<E>(this, operationTimeout.toMillis());
  }
}
