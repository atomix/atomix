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

import com.google.common.collect.Sets;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Distributed collection proxy.
 */
public abstract class DistributedCollectionProxy<A extends AsyncDistributedCollection<String>, S extends DistributedCollectionService>
    extends AbstractAsyncPrimitive<A, S>
    implements AsyncDistributedCollection<String>, DistributedCollectionClient {

  private final Set<CollectionEventListener<String>> eventListeners = Sets.newIdentityHashSet();

  public DistributedCollectionProxy(ProxyClient<S> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public void onEvent(CollectionEvent<String> event) {
    eventListeners.forEach(l -> l.onEvent(event));
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
  public CompletableFuture<Boolean> add(String element) {
    return getProxyClient().applyBy(name(), service -> service.add(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return getProxyClient().applyBy(name(), service -> service.remove(element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return getProxyClient().applyBy(name(), service -> service.contains(element));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    return getProxyClient().applyBy(name(), service -> service.addAll(c))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    return getProxyClient().applyBy(name(), service -> service.containsAll(c));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    return getProxyClient().applyBy(name(), service -> service.removeAll(c))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
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
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener) {
    if (eventListeners.isEmpty()) {
      eventListeners.add(listener);
      return getProxyClient().acceptBy(name(), service -> service.listen()).thenApply(v -> null);
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
    return getProxyClient().applyBy(name(), service -> service.iterate())
        .thenApply(DistributedCollectionIterator::new);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptBy(name(), service -> service.clear());
  }

  @Override
  public CompletableFuture<A> connect() {
    return super.connect()
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

  /**
   * Distributed collection iterator.
   */
  private class DistributedCollectionIterator implements AsyncIterator<String> {
    private final long id;
    private volatile CompletableFuture<DistributedCollectionService.Batch> batch;
    private volatile CompletableFuture<Void> closeFuture;

    DistributedCollectionIterator(long id) {
      this.id = id;
      this.batch = CompletableFuture.completedFuture(
          new DistributedCollectionService.Batch(0, Collections.emptyList()));
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
    private CompletableFuture<DistributedCollectionService.Batch> fetch(int position) {
      return getProxyClient().applyBy(name(), service -> service.next(id, position))
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
            closeFuture = getProxyClient().acceptBy(name(), service -> service.close(id));
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
