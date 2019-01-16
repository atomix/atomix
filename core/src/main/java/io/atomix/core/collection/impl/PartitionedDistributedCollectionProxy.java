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
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedProxyIterator;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Partitioned distributed collection proxy.
 */
public abstract class PartitionedDistributedCollectionProxy<A extends AsyncDistributedCollection<String>, S extends DistributedCollectionService<String>>
    extends AbstractAsyncPrimitive<A, S>
    implements AsyncDistributedCollection<String>, DistributedCollectionClient<String> {

  private final Map<CollectionEventListener<String>, Executor> eventListeners = Maps.newConcurrentMap();

  public PartitionedDistributedCollectionProxy(ProxyClient<S> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public void onEvent(CollectionEvent<String> event) {
    eventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
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
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
    if (eventListeners.putIfAbsent(listener, executor) == null) {
      return getProxyClient().acceptAll(service -> service.listen()).thenApply(v -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !eventListeners.isEmpty();
  }

  @Override
  public AsyncIterator<String> iterator() {
    return new PartitionedProxyIterator<>(
        getProxyClient(),
        DistributedCollectionService::iterate,
        DistributedCollectionService::next,
        DistributedCollectionService::close);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptAll(service -> service.clear());
  }

  @Override
  public CompletableFuture<A> connect() {
    return super.connect()
        .thenCompose(v -> Futures.allOf(getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenRun(() -> getProxyClient().getPartitions().forEach(partition -> {
          partition.addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              partition.accept(service -> service.listen());
            }
          });
        }))
        .thenApply(v -> (A) this);
  }
}
