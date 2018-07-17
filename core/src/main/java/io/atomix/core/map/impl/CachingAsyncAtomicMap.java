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
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.cache.CacheConfig;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.primitive.PrimitiveState;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@code AsyncConsistentMap} that caches entries on read.
 * <p>
 * The cache entries are automatically invalidated when updates are detected either locally or
 * remotely.
 * <p> This implementation only attempts to serve cached entries for {@link AsyncAtomicMap#get get}
 * {@link AsyncAtomicMap#getOrDefault(Object, Object) getOrDefault}, and
 * {@link AsyncAtomicMap#containsKey(Object) containsKey} calls. All other calls skip the cache
 * and directly go the backing map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CachingAsyncAtomicMap<K, V> extends DelegatingAsyncAtomicMap<K, V> {
  private final Logger log = getLogger(getClass());

  private final LoadingCache<K, CompletableFuture<Versioned<V>>> cache;
  private final AsyncAtomicMap<K, V> backingMap;
  private final AtomicMapEventListener<K, V> cacheUpdater;
  private final Consumer<PrimitiveState> statusListener;
  private final Map<AtomicMapEventListener<K, V>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  /**
   * Constructor to configure cache size.
   *
   * @param backingMap  a distributed, strongly consistent map for backing
   * @param cacheConfig the cache configuration
   */
  public CachingAsyncAtomicMap(AsyncAtomicMap<K, V> backingMap, CacheConfig cacheConfig) {
    super(backingMap);
    this.backingMap = backingMap;
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheConfig.getSize())
        .build(CacheLoader.from(CachingAsyncAtomicMap.super::get));
    cacheUpdater = event -> {
      Versioned<V> newValue = event.newValue();
      if (newValue == null) {
        cache.invalidate(event.key());
      } else {
        cache.put(event.key(), CompletableFuture.completedFuture(newValue));
      }
      mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
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
  public CompletableFuture<Void> delete() {
    super.removeStateChangeListener(statusListener);
    return super.delete().thenCompose(v -> removeListener(cacheUpdater));
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

  @Override
  public CompletableFuture<Void> addListener(AtomicMapEventListener<K, V> listener, Executor executor) {
    mapEventListeners.put(listener, executor);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMapEventListener<K, V> listener) {
    mapEventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }
}
