/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.collection.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.cache.CacheConfig;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.primitive.PrimitiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * {@code AsyncDistributedCollection} that caches entries on read.
 * <p>
 * The cache entries are automatically invalidated when updates are detected either locally or
 * remotely.
 *
 * @param <E> element type
 */
public class CachingAsyncDistributedCollection<E> extends DelegatingAsyncDistributedCollection<E> {
  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final LoadingCache<E, CompletableFuture<Boolean>> cache;
  private final CollectionEventListener<E> cacheUpdater;
  private final Consumer<PrimitiveState> statusListener;
  private final Map<CollectionEventListener<E>, Executor> eventListeners = new ConcurrentHashMap<>();

  /**
   * Constructor to configure cache size.
   *
   * @param backingCollection a distributed collection for backing
   * @param cacheConfig       the cache configuration
   */
  public CachingAsyncDistributedCollection(AsyncDistributedCollection<E> backingCollection, CacheConfig cacheConfig) {
    super(backingCollection);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheConfig.getSize())
        .build(CacheLoader.from(CachingAsyncDistributedCollection.super::contains));
    cacheUpdater = event -> {
      cache.invalidate(event.element());
      eventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
    };
    statusListener = status -> {
      log.debug("{} status changed to {}", this.name(), status);
      // If the status of the underlying map is SUSPENDED or INACTIVE
      // we can no longer guarantee that the cache will be in sync.
      if (status == PrimitiveState.SUSPENDED || status == PrimitiveState.CLOSED) {
        cache.invalidateAll();
      }
    };
    super.addListener(cacheUpdater, MoreExecutors.directExecutor());
    super.addStateChangeListener(statusListener);
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return super.add(element).whenComplete((r, e) -> {
      if (r) {
        cache.invalidate(element);
      }
    });
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return super.addAll(c).whenComplete((r, e) -> {
      if (r) {
        c.forEach(cache::invalidate);
      }
    });
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return super.retainAll(c).whenComplete((r, e) -> {
      if (r) {
        c.forEach(cache::invalidate);
      }
    });
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return super.removeAll(c).whenComplete((r, e) -> {
      if (r) {
        c.forEach(cache::invalidate);
      }
    });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return super.clear().whenComplete((r, e) -> {
      cache.invalidateAll();
    });
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor) {
    eventListeners.put(listener, executor);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }
}
