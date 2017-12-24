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

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.atomix.primitive.DistributedPrimitive.Status.INACTIVE;
import static io.atomix.primitive.DistributedPrimitive.Status.SUSPENDED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Caching asynchronous document tree.
 */
public class CachingAsyncDocumentTree<V> extends DelegatingAsyncDocumentTree<V> implements AsyncDocumentTree<V> {
  private static final int DEFAULT_CACHE_SIZE = 10000;
  private final Logger log = getLogger(getClass());

  private final LoadingCache<DocumentPath, CompletableFuture<Versioned<V>>> cache;
  private final DocumentTreeListener<V> cacheUpdater;
  private final Consumer<Status> statusListener;

  /**
   * Default constructor.
   *
   * @param backingTree a distributed, strongly consistent map for backing
   */
  public CachingAsyncDocumentTree(AsyncDocumentTree<V> backingTree) {
    this(backingTree, DEFAULT_CACHE_SIZE);
  }

  /**
   * Constructor to configure cache size.
   *
   * @param backingTree a distributed, strongly consistent map for backing
   * @param cacheSize   the maximum size of the cache
   */
  public CachingAsyncDocumentTree(AsyncDocumentTree<V> backingTree, int cacheSize) {
    super(backingTree);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(CacheLoader.from(CachingAsyncDocumentTree.super::get));
    cacheUpdater = event -> {
      if (!event.newValue().isPresent()) {
        cache.invalidate(event.path());
      } else {
        cache.put(event.path(), CompletableFuture.completedFuture(event.newValue().get()));
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
  public CompletableFuture<Versioned<V>> get(DocumentPath path) {
    return cache.getUnchecked(path);
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
  public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
    return super.removeNode(path)
        .whenComplete((r, e) -> cache.invalidate(path));
  }

  @Override
  public DocumentTree<V> sync(Duration operationTimeout) {
    return new BlockingDocumentTree<>(this, operationTimeout.toMillis());
  }
}
