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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.atomix.core.multimap.AsyncConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.MultimapEvent;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Set based implementation of the {@link AsyncConsistentMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class ConsistentSetMultimapProxy
    extends AbstractAsyncPrimitive<AsyncConsistentMultimap<String, byte[]>, ConsistentSetMultimapService>
    implements AsyncConsistentMultimap<String, byte[]>, ConsistentSetMultimapClient {

  private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  public ConsistentSetMultimapProxy(ProxyClient<ConsistentSetMultimapService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void onChange(String key, byte[] oldValue, byte[] newValue) {
    MultimapEvent<String, byte[]> event = new MultimapEvent<>(name(), key, newValue, oldValue);
    mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxyClient().applyAll(service -> service.size())
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return getProxyClient().applyAll(service -> service.isEmpty())
        .thenApply(results -> results.allMatch(Predicate.isEqual(true)));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return getProxyClient().applyBy(key, service -> service.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return getProxyClient().applyAll(service -> service.containsValue(value))
        .thenApply(results -> results.anyMatch(Predicate.isEqual(true)));
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(String key, byte[] value) {
    return getProxyClient().applyBy(key, service -> service.containsEntry(key, value));
  }

  @Override
  public CompletableFuture<Boolean> put(String key, byte[] value) {
    return getProxyClient().applyBy(key, service -> service.put(key, value));
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return getProxyClient().applyBy(key, service -> service.remove(key, value));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(String key, Collection<? extends byte[]> values) {
    return getProxyClient().applyBy(key, service -> service.removeAll(key, values));
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(String key) {
    return getProxyClient().applyBy(key, service -> service.removeAll(key));
  }

  @Override
  public CompletableFuture<Boolean> putAll(String key, Collection<? extends byte[]> values) {
    return getProxyClient().applyBy(key, service -> service.putAll(key, values));
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
      String key, Collection<byte[]> values) {
    return getProxyClient().applyBy(key, service -> service.replaceValues(key, values));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptAll(service -> service.clear());
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(String key) {
    return getProxyClient().applyBy(key, service -> service.get(key));
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return getProxyClient().applyAll(service -> service.keySet())
        .thenApply(results -> results.flatMap(Collection::stream).collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Multiset<String>> keys() {
    return getProxyClient().applyAll(service -> service.keys())
        .thenApply(results -> results.reduce(Multisets::sum).orElse(HashMultiset.create()));
  }

  @Override
  public CompletableFuture<Multiset<byte[]>> values() {
    return getProxyClient().applyAll(service -> service.values())
        .thenApply(results -> results.reduce(Multisets::sum).orElse(HashMultiset.create()));
  }

  @Override
  public CompletableFuture<Collection<Map.Entry<String, byte[]>>> entries() {
    return getProxyClient().applyAll(service -> service.entries())
        .thenApply(results -> results.flatMap(Collection::stream).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Void> addListener(MultimapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.listen());
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(MultimapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten());
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Map<String, Collection<byte[]>>> asMap() {
    throw new UnsupportedOperationException("Expensive operation.");
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }

  @Override
  public CompletableFuture<AsyncConsistentMultimap<String, byte[]>> connect() {
    return super.connect()
        .thenRun(() -> getProxyClient().getPartitionIds().forEach(partition -> {
          getProxyClient().getPartition(partition).addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              getProxyClient().acceptOn(partition, service -> service.listen());
            }
          });
        }))
        .thenApply(v -> this);
  }

  @Override
  public ConsistentMultimap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingConsistentMultimap<>(this, operationTimeout.toMillis());
  }
}