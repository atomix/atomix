// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.cache.CacheConfig;
import io.atomix.core.collection.impl.CachingAsyncDistributedCollection;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Caching asynchronous distributed queue.
 */
public class CachingAsyncDistributedQueue<E> extends CachingAsyncDistributedCollection<E> implements AsyncDistributedQueue<E> {
  private final AsyncDistributedQueue<E> backingQueue;

  public CachingAsyncDistributedQueue(AsyncDistributedQueue<E> backingCollection, CacheConfig cacheConfig) {
    super(backingCollection, cacheConfig);
    this.backingQueue = backingCollection;
  }

  @Override
  public CompletableFuture<Boolean> offer(E e) {
    return backingQueue.offer(e);
  }

  @Override
  public CompletableFuture<E> remove() {
    return backingQueue.remove().thenApply(result -> {
      cache.invalidate(result);
      return result;
    });
  }

  @Override
  public CompletableFuture<E> poll() {
    return backingQueue.poll().thenApply(result -> {
      cache.invalidate(result);
      return result;
    });
  }

  @Override
  public CompletableFuture<E> element() {
    return backingQueue.element().thenApply(result -> {
      cache.invalidate(result);
      return result;
    });
  }

  @Override
  public CompletableFuture<E> peek() {
    return backingQueue.peek();
  }

  @Override
  public DistributedQueue<E> sync(Duration operationTimeout) {
    return new BlockingDistributedQueue<>(this, operationTimeout.toMillis());
  }
}
