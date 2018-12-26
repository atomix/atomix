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
package io.atomix.core.election.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.atomix.core.cache.CacheConfig;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.PrimitiveState;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Caching leader elector primitive.
 */
public class CachingAsyncLeaderElector<T> extends DelegatingAsyncLeaderElector<T> {
  private final LoadingCache<String, CompletableFuture<Leadership<T>>> cache;
  private final LeadershipEventListener<T> cacheUpdater;
  private final Consumer<PrimitiveState> statusListener;

  public CachingAsyncLeaderElector(AsyncLeaderElector<T> delegateLeaderElector, CacheConfig cacheConfig) {
    super(delegateLeaderElector);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheConfig.getSize())
        .build(CacheLoader.from(super::getLeadership));

    cacheUpdater = event -> {
      Leadership<T> leadership = event.newLeadership();
      cache.put(event.topic(), CompletableFuture.completedFuture(leadership));
    };
    statusListener = status -> {
      if (status == PrimitiveState.SUSPENDED || status == PrimitiveState.CLOSED) {
        cache.invalidateAll();
      }
    };
    addListener(cacheUpdater);
    addStateChangeListener(statusListener);
  }

  @Override
  public CompletableFuture<Leadership<T>> getLeadership(String topic) {
    return cache.getUnchecked(topic)
        .whenComplete((r, e) -> {
          if (e != null) {
            cache.invalidate(topic);
          }
        });
  }

  @Override
  public CompletableFuture<Leadership<T>> run(String topic, T identifier) {
    return super.run(topic, identifier).whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, T identifier) {
    return super.withdraw(topic, identifier).whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, T nodeId) {
    return super.anoint(topic, nodeId).whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, T nodeId) {
    return super.promote(topic, nodeId).whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Void> evict(T nodeId) {
    return super.evict(nodeId).whenComplete((r, e) -> cache.invalidateAll());
  }
}
