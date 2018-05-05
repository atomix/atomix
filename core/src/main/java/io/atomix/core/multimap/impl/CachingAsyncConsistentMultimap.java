/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.multimap.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.atomix.core.multimap.AsyncConsistentMultimap;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.atomix.primitive.DistributedPrimitive.Status.INACTIVE;
import static io.atomix.primitive.DistributedPrimitive.Status.SUSPENDED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Caching {@link AsyncConsistentMultimap} implementation.
 */
public class CachingAsyncConsistentMultimap<K, V> extends DelegatingAsyncConsistentMultimap<K, V> {
  private static final int DEFAULT_CACHE_SIZE = 1000;
  private final Logger log = getLogger(getClass());

  private final LoadingCache<K, CompletableFuture<Versioned<Collection<? extends V>>>> cache;
  private final MultimapEventListener<K, V> cacheUpdater;
  private final Consumer<Status> statusListener;

  /**
   * Default constructor.
   *
   * @param backingMap a distributed, strongly consistent map for backing
   */
  public CachingAsyncConsistentMultimap(AsyncConsistentMultimap<K, V> backingMap) {
    this(backingMap, DEFAULT_CACHE_SIZE);
  }

  /**
   * Constructor to configure cache size.
   *
   * @param backingMap a distributed, strongly consistent map for backing
   * @param cacheSize  the maximum size of the cache
   */
  public CachingAsyncConsistentMultimap(AsyncConsistentMultimap<K, V> backingMap, int cacheSize) {
    super(backingMap);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(CacheLoader.from(CachingAsyncConsistentMultimap.super::get));
    cacheUpdater = event -> {
      V oldValue = event.oldValue();
      V newValue = event.newValue();
      CompletableFuture<Versioned<Collection<? extends V>>> future = cache.getUnchecked(event.key());
      switch (event.type()) {
        case INSERT:
          if (future.isDone()) {
            Versioned<Collection<? extends V>> oldVersioned = future.join();
            Versioned<Collection<? extends V>> newVersioned = new Versioned<>(
                ImmutableSet.<V>builder().addAll(oldVersioned.value()).add(newValue).build(),
                oldVersioned.version(),
                oldVersioned.creationTime());
            cache.put(event.key(), CompletableFuture.completedFuture(newVersioned));
          } else {
            cache.put(event.key(), future.thenApply(versioned -> new Versioned<>(
                ImmutableSet.<V>builder().addAll(versioned.value()).add(newValue).build(),
                versioned.version(),
                versioned.creationTime())));
          }
          break;
        case REMOVE:
          if (future.isDone()) {
            Versioned<Collection<? extends V>> oldVersioned = future.join();
            cache.put(event.key(), CompletableFuture.completedFuture(new Versioned<>(oldVersioned.value()
                .stream()
                .filter(value -> !Objects.equals(value, oldValue))
                .collect(Collectors.toSet()), oldVersioned.version(), oldVersioned.creationTime())));
          } else {
            cache.put(event.key(), future.thenApply(versioned -> new Versioned<>(versioned.value()
                .stream()
                .filter(value -> !Objects.equals(value, oldValue))
                .collect(Collectors.toSet()), versioned.version(), versioned.creationTime())));
          }
          break;
        default:
          break;
      }
    };
    statusListener = status -> {
      log.debug("{} status changed to {}", this.name(), status);
      // If the status of the underlying map is SUSPENDED or INACTIVE
      // we can no longer guarantee that the cache will be in sync.
      if (status == SUSPENDED || status == INACTIVE) {
        cache.invalidateAll();
      }
    };
    super.addListener(cacheUpdater);
    super.addStatusChangeListener(statusListener);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return get(key).thenApply(value -> value != null && !value.value().isEmpty());
  }

  @Override
  public CompletableFuture<Boolean> put(K key, V value) {
    return super.put(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return super.remove(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(K key, Collection<? extends V> values) {
    return super.removeAll(key, values)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V>>> removeAll(K key) {
    return super.removeAll(key)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Boolean> putAll(K key, Collection<? extends V> values) {
    return super.putAll(key, values)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V>>> replaceValues(K key, Collection<V> values) {
    return super.replaceValues(key, values)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return super.clear()
        .whenComplete((r, e) -> cache.invalidateAll());
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V>>> get(K key) {
    return cache.getUnchecked(key)
        .whenComplete((r, e) -> {
          if (e != null) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Void> close() {
    super.removeStatusChangeListener(statusListener);
    return super.close().thenCompose(v -> removeListener(cacheUpdater));
  }
}
