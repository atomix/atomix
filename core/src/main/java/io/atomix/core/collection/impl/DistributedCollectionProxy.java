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

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.ProxyIterator;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Distributed collection proxy.
 */
public abstract class DistributedCollectionProxy<A extends AsyncDistributedCollection<E>, S extends DistributedCollectionService<E>, E>
    extends AbstractAsyncPrimitive<A, S>
    implements AsyncDistributedCollection<E>, DistributedCollectionClient<E> {

  private final Map<CollectionEventListener<E>, Executor> eventListeners = Maps.newConcurrentMap();

  public DistributedCollectionProxy(ProxyClient<S> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public void onEvent(CollectionEvent<E> event) {
    eventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxyClient().applyBy(name(), service -> service.size());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return getProxyClient().applyBy(name(), service -> service.isEmpty());
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return getProxyClient().applyBy(name(), service -> service.add(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return getProxyClient().applyBy(name(), service -> service.remove(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return getProxyClient().applyBy(name(), service -> service.contains(element));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return getProxyClient().applyBy(name(), service -> service.addAll(c))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return getProxyClient().applyBy(name(), service -> service.containsAll(c));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return getProxyClient().applyBy(name(), service -> service.removeAll(c))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return getProxyClient().applyBy(name(), service -> service.removeAll(c))
        .thenCompose(result -> checkLocked(result));
  }

  protected <T> CompletableFuture<T> checkLocked(CollectionUpdateResult<T> result) {
    if (result.status() == CollectionUpdateResult.Status.WRITE_LOCK_CONFLICT) {
      return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
    }
    return CompletableFuture.completedFuture(result.result());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor) {
    if (eventListeners.putIfAbsent(listener, executor) == null) {
      return getProxyClient().acceptBy(name(), service -> service.listen()).thenApply(v -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
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
  public AsyncIterator<E> iterator() {
    return new ProxyIterator<>(
        getProxyClient(),
        getProxyClient().getPartitionId(name()),
        DistributedCollectionService::iterate,
        DistributedCollectionService::next,
        DistributedCollectionService::close);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptBy(name(), service -> service.clear());
  }

  @Override
  public CompletableFuture<A> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenRun(() -> {
          ProxySession<S> partition = getProxyClient().getPartition(name());
          partition.addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              partition.accept(service -> service.listen());
            }
          });
        })
        .thenApply(v -> (A) this);
  }
}
