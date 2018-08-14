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

package io.atomix.core.multimap.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.impl.BlockingDistributedMap;
import io.atomix.core.multimap.AsyncDistributedMultimap;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.impl.BlockingDistributedMultiset;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link DistributedMultimap} providing synchronous access to {@link AsyncDistributedMultimap}.
 */
public class BlockingDistributedMultimap<K, V>
    extends Synchronous<AsyncDistributedMultimap<K, V>>
    implements DistributedMultimap<K, V> {

  private final AsyncDistributedMultimap<K, V> asyncMultimap;
  private final long operationTimeoutMillis;

  public BlockingDistributedMultimap(
      AsyncDistributedMultimap<K, V> asyncMultimap,
      long operationTimeoutMillis) {
    super(asyncMultimap);
    this.asyncMultimap = asyncMultimap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int size() {
    return complete(asyncMultimap.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(asyncMultimap.isEmpty());
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    return complete(asyncMultimap.containsKey((K) key));
  }

  @Override
  public boolean containsValue(@Nullable Object value) {
    return complete(asyncMultimap.containsValue((V) value));
  }

  @Override
  public boolean containsEntry(@Nullable Object key, @Nullable Object value) {
    return complete(asyncMultimap.containsEntry((K) key, (V) value));
  }

  @Override
  public boolean put(@Nullable K key, @Nullable V value) {
    return complete(asyncMultimap.put(key, value));
  }

  @Override
  public boolean remove(@Nullable Object key, @Nullable Object value) {
    return complete(asyncMultimap.remove((K) key, (V) value));
  }

  @Override
  public boolean putAll(@Nullable K key, Iterable<? extends V> values) {
    return complete(asyncMultimap.putAll(key, values instanceof Collection ? (Collection<V>) values : Lists.newArrayList(values)));
  }

  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> replaceValues(@Nullable K key, Iterable<? extends V> values) {
    return complete(asyncMultimap.replaceValues(key, values instanceof Collection ? (Collection<V>) values : Lists.<V>newArrayList(values)));
  }

  @Override
  public Collection<V> removeAll(@Nullable Object key) {
    return complete(asyncMultimap.removeAll((K) key));
  }

  @Override
  public void clear() {
    complete(asyncMultimap.clear());
  }

  @Override
  public Collection<V> get(@Nullable K key) {
    return complete(asyncMultimap.get(key));
  }

  @Override
  public DistributedSet<K> keySet() {
    return new BlockingDistributedSet<>(asyncMultimap.keySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedMultiset<K> keys() {
    return new BlockingDistributedMultiset<>(asyncMultimap.keys(), operationTimeoutMillis);
  }

  @Override
  public DistributedMultiset<V> values() {
    return new BlockingDistributedMultiset<>(asyncMultimap.values(), operationTimeoutMillis);
  }

  @Override
  public DistributedCollection<Map.Entry<K, V>> entries() {
    return new BlockingDistributedCollection<>(asyncMultimap.entries(), operationTimeoutMillis);
  }

  @Override
  public DistributedMap<K, Collection<V>> asMap() {
    return new BlockingDistributedMap<>(asyncMultimap.asMap(), operationTimeoutMillis);
  }

  @Override
  public void addListener(MultimapEventListener<K, V> listener, Executor executor) {
    complete(asyncMultimap.addListener(listener, executor));
  }

  @Override
  public void removeListener(MultimapEventListener<K, V> listener) {
    complete(asyncMultimap.removeListener(listener));
  }

  @Override
  public AsyncDistributedMultimap<K, V> async() {
    return asyncMultimap;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
