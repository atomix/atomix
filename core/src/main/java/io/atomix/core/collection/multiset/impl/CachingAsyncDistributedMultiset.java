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
package io.atomix.core.collection.multiset.impl;

import com.google.common.collect.Multiset;
import io.atomix.core.collection.impl.CachingAsyncDistributedCollection;
import io.atomix.core.collection.multiset.AsyncDistributedMultiset;
import io.atomix.core.collection.multiset.DistributedMultiset;
import io.atomix.core.collection.set.AsyncDistributedSet;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Caching asynchronous distributed multiset.
 */
public class CachingAsyncDistributedMultiset<E> extends CachingAsyncDistributedCollection<E> implements AsyncDistributedMultiset<E> {
  private final AsyncDistributedMultiset<E> backingMultiset;

  public CachingAsyncDistributedMultiset(AsyncDistributedMultiset<E> backingCollection) {
    super(backingCollection);
    this.backingMultiset = backingCollection;
  }

  public CachingAsyncDistributedMultiset(AsyncDistributedMultiset<E> backingCollection, int cacheSize) {
    super(backingCollection, cacheSize);
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
