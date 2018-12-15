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
package io.atomix.core.list.impl;

import io.atomix.core.collection.impl.DistributedCollectionProxy;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed list proxy.
 */
public class DistributedListProxy extends DistributedCollectionProxy<AsyncDistributedList<String>, DistributedListService, String> implements AsyncDistributedList<String> {
  public DistributedListProxy(ProxyClient<DistributedListService> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends String> c) {
    return getProxyClient().applyBy(name(), service -> service.addAll(index, c))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<String> get(int index) {
    return getProxyClient().applyBy(name(), service -> service.get(index));
  }

  @Override
  public CompletableFuture<String> set(int index, String element) {
    return getProxyClient().applyBy(name(), service -> service.set(index, element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Void> add(int index, String element) {
    return getProxyClient().applyBy(name(), service -> service.add(index, element))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<String> remove(int index) {
    return getProxyClient().applyBy(name(), service -> service.remove(index))
        .thenCompose(result -> checkLocked(result));
  }

  @Override
  public CompletableFuture<Integer> indexOf(Object o) {
    return getProxyClient().applyBy(name(), service -> service.indexOf(o));
  }

  @Override
  public CompletableFuture<Integer> lastIndexOf(Object o) {
    return getProxyClient().applyBy(name(), service -> service.lastIndexOf(o));
  }

  @Override
  public CompletableFuture<AsyncDistributedList<String>> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenApply(v -> this);
  }

  @Override
  public DistributedList<String> sync(Duration operationTimeout) {
    return new BlockingDistributedList<>(this, operationTimeout.toMillis());
  }
}
