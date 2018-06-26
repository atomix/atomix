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
package io.atomix.core.collection.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Partitioned distributed collection proxy.
 */
public abstract class PartitionedDistributedCollectionProxy<A extends AsyncDistributedCollection<String>, S extends DistributedCollectionService>
    extends AbstractAsyncPrimitive<A, S>
    implements AsyncDistributedCollection<String>, DistributedCollectionClient {

  private final Set<CollectionEventListener<String>> eventListeners = Sets.newIdentityHashSet();

  public PartitionedDistributedCollectionProxy(ProxyClient<S> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public void onEvent(CollectionEvent<String> event) {
    eventListeners.forEach(l -> l.onEvent(event));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxyClient().applyAll(service -> service.size())
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return getProxyClient().applyAll(service -> service.isEmpty())
        .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Boolean> add(String element) {
    return getProxyClient().applyBy(element, service -> service.add(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return getProxyClient().applyBy(element, service -> service.remove(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return getProxyClient().applyBy(element, service -> service.contains(element));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
    c.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getProxyClient()
            .applyOn(entry.getKey(), service -> service.addAll(entry.getValue()))
            .thenCompose(result -> checkLocked(result)))
        .collect(Collectors.toList()))
        .thenApply(results -> results.stream().reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
    c.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getProxyClient()
            .applyOn(entry.getKey(), service -> service.containsAll(entry.getValue())))
        .collect(Collectors.toList()))
        .thenApply(results -> results.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
    c.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getProxyClient()
            .applyOn(entry.getKey(), service -> service.retainAll(entry.getValue()))
            .thenCompose(result -> checkLocked(result)))
        .collect(Collectors.toList()))
        .thenApply(results -> results.stream().reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
    c.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getProxyClient()
            .applyOn(entry.getKey(), service -> service.removeAll(entry.getValue()))
            .thenCompose(result -> checkLocked(result)))
        .collect(Collectors.toList()))
        .thenApply(results -> results.stream().reduce(Boolean::logicalOr).orElse(false));
  }

  protected <T> CompletableFuture<T> checkLocked(CollectionUpdateResult<T> result) {
    if (result.status() == CollectionUpdateResult.Status.WRITE_LOCK_CONFLICT) {
      return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
    }
    return CompletableFuture.completedFuture(result.result());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener) {
    if (eventListeners.isEmpty()) {
      eventListeners.add(listener);
      return getProxyClient().acceptAll(service -> service.listen()).thenApply(v -> null);
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !eventListeners.isEmpty();
  }

  @Override
  public CompletableFuture<AsyncIterator<String>> iterator() {
    return Futures.allOf(getProxyClient().getPartitionIds().stream()
        .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterate())
            .thenApply(iteratorId -> new DistributedCollectionPartitionIterator(partitionId, iteratorId))))
        .thenApply(iterators -> new PartitionedDistributedCollectionIterator(iterators.collect(Collectors.toList())));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptBy(name(), service -> service.clear());
  }

  @Override
  public CompletableFuture<A> connect() {
    return super.connect()
        .thenRun(() -> getProxyClient().getPartitions().forEach(partition -> {
          partition.addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              partition.accept(service -> service.listen());
            }
          });
        }))
        .thenApply(v -> (A) this);
  }

  /**
   * Partitioned distributed collection iterator.
   */
  private class PartitionedDistributedCollectionIterator implements AsyncIterator<String> {
    private final Iterator<AsyncIterator<String>> iterators;
    private volatile AsyncIterator<String> iterator;

    public PartitionedDistributedCollectionIterator(Collection<AsyncIterator<String>> iterators) {
      this.iterators = iterators.iterator();
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      if (iterator == null && iterators.hasNext()) {
        iterator = iterators.next();
      }
      if (iterator == null) {
        return CompletableFuture.completedFuture(false);
      }
      return iterator.hasNext()
          .thenCompose(hasNext -> {
            if (!hasNext) {
              iterator = null;
              return hasNext();
            }
            return CompletableFuture.completedFuture(true);
          });
    }

    @Override
    public CompletableFuture<String> next() {
      if (iterator == null && iterators.hasNext()) {
        iterator = iterators.next();
      }
      if (iterator == null) {
        return Futures.exceptionalFuture(new NoSuchElementException());
      }
      return iterator.next();
    }
  }

  /**
   * Distributed collection partition iterator.
   */
  private class DistributedCollectionPartitionIterator implements AsyncIterator<String> {
    private final PartitionId partitionId;
    private final long iteratorId;
    private volatile CompletableFuture<DistributedCollectionService.Batch<String>> batch;
    private volatile CompletableFuture<Void> closeFuture;

    DistributedCollectionPartitionIterator(PartitionId partitionId, long iteratorId) {
      this.partitionId = partitionId;
      this.iteratorId = iteratorId;
      this.batch = CompletableFuture.completedFuture(
          new DistributedCollectionService.Batch<>(0, Collections.emptyList()));
    }

    /**
     * Returns the current batch iterator or lazily fetches the next batch from the cluster.
     *
     * @return the next batch iterator
     */
    private CompletableFuture<Iterator<String>> batch() {
      return batch.thenCompose(iterator -> {
        if (iterator != null && !iterator.hasNext()) {
          batch = fetch(iterator.position());
          return batch.thenApply(Function.identity());
        }
        return CompletableFuture.completedFuture(iterator);
      });
    }

    /**
     * Fetches the next batch of entries from the cluster.
     *
     * @param position the position from which to fetch the next batch
     * @return the next batch of entries from the cluster
     */
    private CompletableFuture<DistributedCollectionService.Batch<String>> fetch(int position) {
      return getProxyClient().applyOn(partitionId, service -> service.next(iteratorId, position))
          .thenCompose(batch -> {
            if (batch == null) {
              return close().thenApply(v -> null);
            }
            return CompletableFuture.completedFuture(batch);
          });
    }

    /**
     * Closes the iterator.
     *
     * @return future to be completed once the iterator has been closed
     */
    private CompletableFuture<Void> close() {
      if (closeFuture == null) {
        synchronized (this) {
          if (closeFuture == null) {
            closeFuture = getProxyClient().acceptOn(partitionId, service -> service.close(iteratorId));
          }
        }
      }
      return closeFuture;
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      return batch().thenApply(iterator -> iterator != null && iterator.hasNext());
    }

    @Override
    public CompletableFuture<String> next() {
      return batch().thenCompose(iterator -> {
        if (iterator == null) {
          return Futures.exceptionalFuture(new NoSuchElementException());
        }
        return CompletableFuture.completedFuture(iterator.next());
      });
    }
  }
}
