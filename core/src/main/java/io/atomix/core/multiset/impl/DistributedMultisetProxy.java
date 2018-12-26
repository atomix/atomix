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
package io.atomix.core.multiset.impl;

import com.google.common.collect.Multiset;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.impl.PartitionedDistributedCollectionProxy;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedProxyIterator;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Distributed multiset proxy.
 */
public class DistributedMultisetProxy
    extends PartitionedDistributedCollectionProxy<AsyncDistributedMultiset<String>, DistributedMultisetService>
    implements AsyncDistributedMultiset<String> {
  public DistributedMultisetProxy(ProxyClient<DistributedMultisetService> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<Integer> count(Object element) {
    return getProxyClient().applyBy((String) element, service -> service.count(element));
  }

  @Override
  public CompletableFuture<Integer> add(String element, int occurrences) {
    return getProxyClient().applyBy(element, service -> service.add(element, occurrences))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Integer> remove(Object element, int occurrences) {
    return getProxyClient().applyBy((String) element, service -> service.remove(element, occurrences))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Integer> setCount(String element, int count) {
    return getProxyClient().applyBy(element, service -> service.setCount(element, count))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Boolean> setCount(String element, int oldCount, int newCount) {
    return getProxyClient().applyBy(element, service -> service.setCount(element, oldCount, newCount))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public AsyncDistributedSet<String> elementSet() {
    return new DistributedMultisetElementSet();
  }

  @Override
  public AsyncDistributedSet<Multiset.Entry<String>> entrySet() {
    return new DistributedMultisetEntrySet();
  }

  @Override
  public DistributedMultiset<String> sync(Duration operationTimeout) {
    return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
  }

  /**
   * Distributed multiset element set.
   */
  private class DistributedMultisetElementSet implements AsyncDistributedSet<String> {
    @Override
    public String name() {
      return DistributedMultisetProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return DistributedMultisetProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return setCount(element, 0, 1);
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return setCount(element, 0).thenApply(count -> count > 0);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyAll(service -> service.elements())
          .thenApply(results -> results.reduce(Math::addExact).orElse(0));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return DistributedMultisetProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return DistributedMultisetProxy.this.contains(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
      return DistributedMultisetProxy.this.containsAll(c);
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AsyncIterator<String> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          DistributedMultisetService::iterateElements,
          DistributedMultisetService::nextElements,
          DistributedMultisetService::closeElements);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return DistributedMultisetProxy.this.clear();
    }

    @Override
    public CompletableFuture<Void> close() {
      return DistributedMultisetProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return DistributedMultisetProxy.this.delete();
    }

    @Override
    public DistributedSet<String> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }
  }

  /**
   * Distributed multiset entry set.
   */
  private class DistributedMultisetEntrySet implements AsyncDistributedSet<Multiset.Entry<String>> {
    @Override
    public String name() {
      return DistributedMultisetProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return DistributedMultisetProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Multiset.Entry<String> element) {
      return DistributedMultisetProxy.this.add(element.getElement(), element.getCount())
          .thenApply(v -> true);
    }

    @Override
    public CompletableFuture<Boolean> remove(Multiset.Entry<String> element) {
      return DistributedMultisetProxy.this.remove(element.getElement(), element.getCount())
          .thenApply(count -> count > 0);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return DistributedMultisetProxy.this.elementSet().size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return DistributedMultisetProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Boolean> contains(Multiset.Entry<String> element) {
      return count(element.getElement()).thenApply(count -> count >= element.getCount());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Multiset.Entry<String>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Multiset.Entry<String>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Multiset.Entry<String>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Multiset.Entry<String>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Multiset.Entry<String>> listener, Executor executor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Multiset.Entry<String>> listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AsyncIterator<Multiset.Entry<String>> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          DistributedMultisetService::iterateEntries,
          DistributedMultisetService::nextEntries,
          DistributedMultisetService::closeEntries);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Multiset.Entry<String>>> transactionLog) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return DistributedMultisetProxy.this.clear();
    }

    @Override
    public CompletableFuture<Void> close() {
      return DistributedMultisetProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return DistributedMultisetProxy.this.delete();
    }

    @Override
    public DistributedSet<Multiset.Entry<String>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }
  }
}
