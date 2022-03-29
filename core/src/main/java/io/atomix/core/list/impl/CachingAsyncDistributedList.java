// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list.impl;

import io.atomix.core.cache.CacheConfig;
import io.atomix.core.collection.impl.CachingAsyncDistributedCollection;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Caching asynchronous distributed list.
 */
public class CachingAsyncDistributedList<E> extends CachingAsyncDistributedCollection<E> implements AsyncDistributedList<E> {
  private final AsyncDistributedList<E> backingList;

  public CachingAsyncDistributedList(AsyncDistributedList<E> backingCollection, CacheConfig cacheConfig) {
    super(backingCollection, cacheConfig);
    this.backingList = backingCollection;
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends E> c) {
    return backingList.addAll(index, c).thenApply(result -> {
      cache.invalidateAll();
      return result;
    });
  }

  @Override
  public CompletableFuture<E> get(int index) {
    return backingList.get(index);
  }

  @Override
  public CompletableFuture<E> set(int index, E element) {
    return backingList.set(index, element).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public CompletableFuture<Void> add(int index, E element) {
    return backingList.add(index, element).thenApply(result -> {
      cache.invalidate(element);
      return result;
    });
  }

  @Override
  public CompletableFuture<E> remove(int index) {
    return backingList.remove(index).thenApply(result -> {
      cache.invalidate(result);
      return result;
    });
  }

  @Override
  public CompletableFuture<Integer> indexOf(Object o) {
    return backingList.indexOf(o);
  }

  @Override
  public CompletableFuture<Integer> lastIndexOf(Object o) {
    return backingList.lastIndexOf(o);
  }

  @Override
  public DistributedList<E> sync(Duration operationTimeout) {
    return new BlockingDistributedList<>(this, operationTimeout.toMillis());
  }
}
