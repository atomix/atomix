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
package io.atomix.core.map.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.MapEventListener;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.atomix.primitive.DistributedPrimitive.Status.INACTIVE;
import static io.atomix.primitive.DistributedPrimitive.Status.SUSPENDED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@code AsyncConsistentMap} that caches entries on read.
 * <p>
 * The cache entries are automatically invalidated when updates are detected either locally or
 * remotely.
 * <p> This implementation only attempts to serve cached entries for {@link AsyncConsistentMap#get get}
 * {@link AsyncConsistentMap#getOrDefault(Object, Object) getOrDefault}, and
 * {@link AsyncConsistentMap#containsKey(Object) containsKey} calls. All other calls skip the cache
 * and directly go the backing map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CachingAsyncConsistentMap<K, V> extends DelegatingAsyncConsistentMap<K, V> {
  private static final int DEFAULT_CACHE_SIZE = 10000;
  private final Logger log = getLogger(getClass());

  private final LoadingCache<K, CompletableFuture<Versioned<V>>> cache;
  private final AsyncConsistentMap<K, V> backingMap;
  private final MapEventListener<K, V> cacheUpdater;
  private final Consumer<Status> statusListener;

  /**
   * Default constructor.
   *
   * @param backingMap a distributed, strongly consistent map for backing
   */
  public CachingAsyncConsistentMap(AsyncConsistentMap<K, V> backingMap) {
    this(backingMap, DEFAULT_CACHE_SIZE);
  }

  /**
   * Constructor to configure cache size.
   *
   * @param backingMap a distributed, strongly consistent map for backing
   * @param cacheSize  the maximum size of the cache
   */
  public CachingAsyncConsistentMap(AsyncConsistentMap<K, V> backingMap, int cacheSize) {
    super(backingMap);
    this.backingMap = backingMap;
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(CacheLoader.from(CachingAsyncConsistentMap.super::get));
    cacheUpdater = event -> {
      Versioned<V> newValue = event.newValue();
      if (newValue == null) {
        cache.invalidate(event.key());
      } else {
        cache.put(event.key(), CompletableFuture.completedFuture(newValue));
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
  public CompletableFuture<Void> destroy() {
    super.removeStatusChangeListener(statusListener);
    return super.destroy().thenCompose(v -> removeListener(cacheUpdater));
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return cache.getUnchecked(key)
        .whenComplete((r, e) -> {
          if (e != null) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return cache.getUnchecked(key).thenCompose(r -> {
      if (r == null) {
        CompletableFuture<Versioned<V>> versioned = backingMap.getOrDefault(key, defaultValue);
        cache.put(key, versioned);
        return versioned;
      } else {
        return CompletableFuture.completedFuture(r);
      }
    }).whenComplete((r, e) -> {
      if (e != null) {
        cache.invalidate(key);
      }
    });
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(K key,
                                                   Predicate<? super V> condition,
                                                   BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return super.computeIf(key, condition, remappingFunction)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value) {
    return super.put(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
    return super.putAndGet(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
    return super.putIfAbsent(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return super.remove(key)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return cache.getUnchecked(key).thenApply(Objects::nonNull)
        .whenComplete((r, e) -> {
          if (e != null) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return super.clear()
        .whenComplete((r, e) -> cache.invalidateAll());
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return super.remove(key, value)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return super.remove(key, version)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return super.replace(key, value)
        .whenComplete((r, e) -> cache.invalidate(key));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return super.replace(key, oldValue, newValue)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(key);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return super.replace(key, oldVersion, newValue)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(key);
          }
        });
  }
}
