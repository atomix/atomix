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
package io.atomix.core.set.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.AsyncJavaIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.set.SetProtocol;
import io.atomix.primitive.protocol.set.SetProtocolEventListener;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Gossip-based distributed set implementation.
 */
public class GossipDistributedSet<E> implements AsyncDistributedSet<E> {
  private final String name;
  private final PrimitiveProtocol protocol;
  private final SetProtocol<E> set;

  private final Map<CollectionEventListener<E>, SetProtocolEventListener<E>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedSet(String name, PrimitiveProtocol protocol, SetProtocol<E> set) {
    this.name = name;
    this.protocol = protocol;
    this.set = set;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return DistributedSetType.instance();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return protocol;
  }

  @Override
  public CompletableFuture<Boolean> add(E element) {
    return complete(() -> set.add(element));
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return complete(() -> set.remove(element));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return complete(() -> set.size());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return complete(() -> set.isEmpty());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return complete(() -> set.clear());
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return complete(() -> set.contains(element));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return complete(() -> set.addAll(c));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return complete(() -> set.containsAll(c));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return complete(() -> set.retainAll(c));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return complete(() -> set.removeAll(c));
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
    SetProtocolEventListener<E> eventListener = event -> {
      switch (event.type()) {
        case ADD:
          listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.element()));
          break;
        case REMOVE:
          listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.element()));
          break;
        default:
          throw new AssertionError();
      }
    };
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      set.addListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    SetProtocolEventListener<E> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      set.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AsyncIterator<E> iterator() {
    return new AsyncJavaIterator<>(set.iterator());
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> close() {
    return complete(() -> set.close());
  }

  private CompletableFuture<Void> complete(Runnable runnable) {
    try {
      runnable.run();
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  private <T> CompletableFuture<T> complete(Supplier<T> supplier) {
    try {
      return CompletableFuture.completedFuture(supplier.get());
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
