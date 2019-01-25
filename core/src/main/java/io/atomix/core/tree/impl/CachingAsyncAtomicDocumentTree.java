/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.tree.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.atomix.core.cache.CacheConfig;
import io.atomix.core.tree.AsyncAtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.core.tree.DocumentTreeEventListener;
import io.atomix.primitive.PrimitiveState;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static io.atomix.primitive.PrimitiveState.CLOSED;
import static io.atomix.primitive.PrimitiveState.SUSPENDED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Caching asynchronous document tree.
 */
public class CachingAsyncAtomicDocumentTree<V> extends DelegatingAsyncAtomicDocumentTree<V> implements AsyncAtomicDocumentTree<V> {
  private final Logger log = getLogger(getClass());

  private final LoadingCache<DocumentPath, CompletableFuture<Versioned<V>>> cache;
  private final DocumentTreeEventListener<V> cacheUpdater;
  private final Consumer<PrimitiveState> stateListener;
  private final Map<DocumentTreeEventListener<V>, InternalListener<V>> eventListeners = new ConcurrentHashMap<>();

  /**
   * Constructor to configure cache size.
   *
   * @param backingTree a distributed, strongly consistent map for backing
   * @param cacheConfig the cache configuration
   */
  public CachingAsyncAtomicDocumentTree(AsyncAtomicDocumentTree<V> backingTree, CacheConfig cacheConfig) {
    super(backingTree);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheConfig.getSize())
        .build(CacheLoader.from(CachingAsyncAtomicDocumentTree.super::get));
    cacheUpdater = event -> {
      if (!event.newValue().isPresent()) {
        cache.invalidate(event.path());
      } else {
        cache.put(event.path(), CompletableFuture.completedFuture(event.newValue().get()));
      }
      eventListeners.values().forEach(listener -> listener.event(event));
    };
    stateListener = status -> {
      log.debug("{} status changed to {}", this.name(), status);
      // If the status of the underlying map is SUSPENDED or INACTIVE
      // we can no longer guarantee that the cache will be in sync.
      if (status == SUSPENDED || status == CLOSED) {
        cache.invalidateAll();
      }
    };
    super.addListener(root(), cacheUpdater, MoreExecutors.directExecutor());
    super.addStateChangeListener(stateListener);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(DocumentPath path) {
    try {
      return cache.getUnchecked(path);
    } catch (UncheckedExecutionException e) {
      return Futures.exceptionalFuture(e.getCause());
    }
  }

  @Override
  public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
    return super.set(path, value)
        .whenComplete((r, e) -> cache.invalidate(path));
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, V value) {
    return super.create(path, value)
        .whenComplete((r, e) -> cache.invalidate(path));
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
    return super.createRecursive(path, value)
        .whenComplete((r, e) -> cache.invalidate(path));
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
    return super.replace(path, newValue, version)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(path);
          }
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
    return super.replace(path, newValue, currentValue)
        .whenComplete((r, e) -> {
          if (r) {
            cache.invalidate(path);
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(DocumentPath path) {
    return super.remove(path)
        .whenComplete((r, e) -> cache.invalidate(path));
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeEventListener<V> listener, Executor executor) {
    eventListeners.put(listener, new InternalListener<>(path, listener, executor));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AtomicDocumentTree<V> sync(Duration operationTimeout) {
    return new BlockingAtomicDocumentTree<>(this, operationTimeout.toMillis());
  }

  private static class InternalListener<V> implements DocumentTreeEventListener<V> {
    private final DocumentPath path;
    private final DocumentTreeEventListener<V> listener;
    private final Executor executor;

    InternalListener(DocumentPath path, DocumentTreeEventListener<V> listener, Executor executor) {
      this.path = path;
      this.listener = listener;
      this.executor = executor;
    }

    @Override
    public void event(DocumentTreeEvent<V> event) {
      if (event.path().isDescendentOf(path)) {
        executor.execute(() -> listener.event(event));
      }
    }
  }
}
