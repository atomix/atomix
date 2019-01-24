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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.DistributedCollectionType;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedProxyIterator;
import io.atomix.core.iterator.impl.TranscodingIterator;
import io.atomix.core.map.AsyncDistributedMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.map.impl.BlockingDistributedMap;
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapEvent;
import io.atomix.core.multimap.AtomicMultimapEventListener;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.core.multiset.impl.BlockingDistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Set based implementation of the {@link AsyncAtomicMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class AtomicMultimapProxy
    extends AbstractAsyncPrimitive<AsyncAtomicMultimap<String, byte[]>, AtomicMultimapService>
    implements AsyncAtomicMultimap<String, byte[]>, AtomicMultimapClient {

  private final Map<AtomicMultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  public AtomicMultimapProxy(ProxyClient<AtomicMultimapService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void onChange(String key, byte[] oldValue, byte[] newValue) {
    AtomicMultimapEvent<String, byte[]> event = new AtomicMultimapEvent<>(
        newValue != null ? AtomicMultimapEvent.Type.INSERT : AtomicMultimapEvent.Type.REMOVE, key, newValue, oldValue);
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
  public CompletableFuture<Versioned<Collection<byte[]>>> removeAll(String key) {
    return getProxyClient().applyBy(key, service -> service.removeAll(key));
  }

  @Override
  public CompletableFuture<Boolean> putAll(String key, Collection<? extends byte[]> values) {
    return getProxyClient().applyBy(key, service -> service.putAll(key, values));
  }

  @Override
  public CompletableFuture<Versioned<Collection<byte[]>>> replaceValues(
      String key, Collection<byte[]> values) {
    return getProxyClient().applyBy(key, service -> service.replaceValues(key, values));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxyClient().acceptAll(service -> service.clear());
  }

  @Override
  public CompletableFuture<Versioned<Collection<byte[]>>> get(String key) {
    return getProxyClient().applyBy(key, service -> service.get(key));
  }

  @Override
  public AsyncDistributedSet<String> keySet() {
    return new KeySet();
  }

  @Override
  public AsyncDistributedMultiset<String> keys() {
    return new Keys();
  }

  @Override
  public AsyncDistributedMultiset<byte[]> values() {
    return new Values();
  }

  @Override
  public AsyncDistributedCollection<Map.Entry<String, byte[]>> entries() {
    return new Entries();
  }

  @Override
  public AsyncDistributedMap<String, Versioned<Collection<byte[]>>> asMap() {
    return new AsMap();
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMultimapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      mapEventListeners.put(listener, executor);
      return getProxyClient().acceptAll(service -> service.listen());
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMultimapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return getProxyClient().acceptAll(service -> service.unlisten());
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }

  @Override
  public CompletableFuture<AsyncAtomicMultimap<String, byte[]>> connect() {
    return super.connect()
        .thenCompose(v -> Futures.allOf(getProxyClient().getPartitions().stream().map(ProxySession::connect)))
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
  public AtomicMultimap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMultimap<>(this, operationTimeout.toMillis());
  }

  private class AsMap implements AsyncDistributedMap<String, Versioned<Collection<byte[]>>> {
    private final Map<MapEventListener<String, Versioned<Collection<byte[]>>>, MultimapEventListener<String, byte[]>> listenerMap = Maps.newConcurrentMap();

    @Override
    public String name() {
      return AtomicMultimapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedMapType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyAll(service -> service.keyCount())
          .thenApply(results -> results.reduce(Math::addExact).orElse(0));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return size().thenApply(size -> size == 0);
    }

    @Override
    public CompletableFuture<Boolean> containsKey(String key) {
      return AtomicMultimapProxy.this.containsKey(key);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(Versioned<Collection<byte[]>> values) {
      return Futures.allOf(values.value().stream()
          .map(value -> AtomicMultimapProxy.this.containsValue(value)))
          .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(true));
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> get(String key) {
      return AtomicMultimapProxy.this.get(key);
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> put(String key, Versioned<Collection<byte[]>> value) {
      return replaceValues(key, value.value());
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> remove(String key) {
      return removeAll(key);
    }

    @Override
    public CompletableFuture<Void> putAll(Map<? extends String, ? extends Versioned<Collection<byte[]>>> map) {
      return Futures.allOf(map.entrySet().stream()
          .map(entry -> AtomicMultimapProxy.this.putAll(entry.getKey(), entry.getValue().value())))
          .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AtomicMultimapProxy.this.clear();
    }

    @Override
    public AsyncDistributedSet<String> keySet() {
      return AtomicMultimapProxy.this.keySet();
    }

    @Override
    public AsyncDistributedCollection<Versioned<Collection<byte[]>>> values() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AsyncDistributedSet<Map.Entry<String, Versioned<Collection<byte[]>>>> entrySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> getOrDefault(String key, Versioned<Collection<byte[]>> defaultValue) {
      return AtomicMultimapProxy.this.get(key).thenApply(value -> value == null ? defaultValue : value);
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> putIfAbsent(String key, Versioned<Collection<byte[]>> value) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String key, Versioned<Collection<byte[]>> value) {
      return removeAll(key, value.value());
    }

    @Override
    public CompletableFuture<Boolean> replace(String key, Versioned<Collection<byte[]>> oldValue, Versioned<Collection<byte[]>> newValue) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> replace(String key, Versioned<Collection<byte[]>> value) {
      return replaceValues(key, value.value());
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> computeIfAbsent(
        String key, Function<? super String, ? extends Versioned<Collection<byte[]>>> mappingFunction) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> computeIfPresent(
        String key,
        BiFunction<? super String, ? super Versioned<Collection<byte[]>>, ? extends Versioned<Collection<byte[]>>> remappingFunction) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Versioned<Collection<byte[]>>> compute(
        String key,
        BiFunction<? super String, ? super Versioned<Collection<byte[]>>, ? extends Versioned<Collection<byte[]>>> remappingFunction) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(MapEventListener<String, Versioned<Collection<byte[]>>> listener, Executor executor) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> removeListener(MapEventListener<String, Versioned<Collection<byte[]>>> listener) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public DistributedMap<String, Versioned<Collection<byte[]>>> sync(Duration operationTimeout) {
      return new BlockingDistributedMap<>(this, operationTimeout.toMillis());
    }

    @Override
    public CompletableFuture<Void> close() {
      return AtomicMultimapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicMultimapProxy.this.delete();
    }
  }

  /**
   * Multimap key set.
   */
  private class KeySet implements AsyncDistributedSet<String> {
    private final Map<CollectionEventListener<String>, AtomicMultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AtomicMultimapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedSetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return AtomicMultimapProxy.this.removeAll(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyAll(service -> service.keyCount())
          .thenApply(results -> results.reduce(Math::addExact).orElse(0));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AtomicMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AtomicMultimapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> keys) {
      Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
      keys.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
      return Futures.allOf(partitions.entrySet().stream()
          .map(entry -> getProxyClient()
              .applyOn(entry.getKey(), service -> service.containsKeys(entry.getValue())))
          .collect(Collectors.toList()))
          .thenApply(results -> results.stream().reduce(Boolean::logicalAnd).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<String> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMultimapService::iterateKeySet,
          AtomicMultimapService::nextKeySet,
          AtomicMultimapService::closeKeySet);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
      AtomicMultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return AtomicMultimapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      AtomicMultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AtomicMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return AtomicMultimapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicMultimapProxy.this.delete();
    }

    @Override
    public DistributedSet<String> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
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
  }

  private class Keys implements AsyncDistributedMultiset<String> {
    private final Map<CollectionEventListener<String>, AtomicMultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AtomicMultimapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedMultisetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return AtomicMultimapProxy.this.removeAll(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return AtomicMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AtomicMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AtomicMultimapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    public CompletableFuture<Integer> count(Object element) {
      return get((String) element).thenApply(value -> value == null ? 0 : value.value().size());
    }

    @Override
    public CompletableFuture<Integer> add(String element, int occurrences) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> remove(Object element, int occurrences) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> setCount(String element, int count) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> setCount(String element, int oldCount, int newCount) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncDistributedSet<String> elementSet() {
      return new KeySet();
    }

    @Override
    public AsyncDistributedSet<Multiset.Entry<String>> entrySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> keys) {
      Map<PartitionId, Collection<String>> partitions = Maps.newHashMap();
      keys.forEach(key -> partitions.computeIfAbsent(getProxyClient().getPartitionId(key), k -> Lists.newArrayList()).add(key));
      return Futures.allOf(partitions.entrySet().stream()
          .map(entry -> getProxyClient()
              .applyOn(entry.getKey(), service -> service.containsKeys(entry.getValue())))
          .collect(Collectors.toList()))
          .thenApply(results -> results.stream().reduce(Boolean::logicalAnd).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<String> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMultimapService::iterateKeys,
          AtomicMultimapService::nextKeys,
          AtomicMultimapService::closeKeys);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
      AtomicMultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return AtomicMultimapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      AtomicMultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AtomicMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return AtomicMultimapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicMultimapProxy.this.delete();
    }

    @Override
    public DistributedMultiset<String> sync(Duration operationTimeout) {
      return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
    }
  }

  private class Values implements AsyncDistributedMultiset<byte[]> {
    private final Map<CollectionEventListener<byte[]>, AtomicMultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AtomicMultimapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedMultisetType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(byte[] element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return AtomicMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AtomicMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AtomicMultimapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(byte[] element) {
      return containsValue(element);
    }

    @Override
    public CompletableFuture<Integer> count(Object element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> add(byte[] element, int occurrences) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> remove(Object element, int occurrences) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> setCount(byte[] element, int count) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> setCount(byte[] element, int oldCount, int newCount) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncDistributedSet<byte[]> elementSet() {
      return new ElementSet();
    }

    @Override
    public AsyncDistributedSet<Multiset.Entry<byte[]>> entrySet() {
      return new EntrySet();
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends byte[]> values) {
      return Futures.allOf(values.stream()
          .map(value -> containsValue(value)))
          .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends byte[]> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<byte[]> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMultimapService::iterateValues,
          AtomicMultimapService::nextValues,
          AtomicMultimapService::closeValues);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<byte[]> listener, Executor executor) {
      AtomicMultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, event.newValue()));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, event.oldValue()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return AtomicMultimapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<byte[]> listener) {
      AtomicMultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AtomicMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return AtomicMultimapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicMultimapProxy.this.delete();
    }

    @Override
    public DistributedMultiset<byte[]> sync(Duration operationTimeout) {
      return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
    }

    private class ElementSet implements AsyncDistributedSet<byte[]> {
      @Override
      public String name() {
        return AtomicMultimapProxy.this.name();
      }

      @Override
      public PrimitiveType type() {
        return DistributedSetType.instance();
      }

      @Override
      public PrimitiveProtocol protocol() {
        return AtomicMultimapProxy.this.protocol();
      }

      @Override
      public CompletableFuture<Boolean> add(byte[] element) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> remove(byte[] element) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Integer> size() {
        return Values.this.size();
      }

      @Override
      public CompletableFuture<Boolean> isEmpty() {
        return AtomicMultimapProxy.this.isEmpty();
      }

      @Override
      public CompletableFuture<Void> clear() {
        return AtomicMultimapProxy.this.clear();
      }

      @Override
      public CompletableFuture<Boolean> contains(byte[] element) {
        return Values.this.contains(element);
      }

      @Override
      public CompletableFuture<Boolean> addAll(Collection<? extends byte[]> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> containsAll(Collection<? extends byte[]> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> retainAll(Collection<? extends byte[]> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> removeAll(Collection<? extends byte[]> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Void> addListener(CollectionEventListener<byte[]> listener, Executor executor) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Void> removeListener(CollectionEventListener<byte[]> listener) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public AsyncIterator<byte[]> iterator() {
        return new TranscodingIterator<>(new PartitionedProxyIterator<>(
            getProxyClient(),
            AtomicMultimapService::iterateValuesSet,
            AtomicMultimapService::nextValuesSet,
            AtomicMultimapService::closeValuesSet), e -> e.getElement());
      }

      @Override
      public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<byte[]>> transactionLog) {
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
        return AtomicMultimapProxy.this.close();
      }

      @Override
      public CompletableFuture<Void> delete() {
        return AtomicMultimapProxy.this.delete();
      }

      @Override
      public DistributedSet<byte[]> sync(Duration operationTimeout) {
        return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
      }
    }

    private class EntrySet implements AsyncDistributedSet<Multiset.Entry<byte[]>> {
      @Override
      public String name() {
        return AtomicMultimapProxy.this.name();
      }

      @Override
      public PrimitiveType type() {
        return DistributedSetType.instance();
      }

      @Override
      public PrimitiveProtocol protocol() {
        return AtomicMultimapProxy.this.protocol();
      }

      @Override
      public CompletableFuture<Boolean> add(Multiset.Entry<byte[]> element) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> remove(Multiset.Entry<byte[]> element) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Integer> size() {
        return AtomicMultimapProxy.this.size();
      }

      @Override
      public CompletableFuture<Boolean> isEmpty() {
        return AtomicMultimapProxy.this.isEmpty();
      }

      @Override
      public CompletableFuture<Void> clear() {
        return AtomicMultimapProxy.this.clear();
      }

      @Override
      public CompletableFuture<Boolean> contains(Multiset.Entry<byte[]> element) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> addAll(Collection<? extends Multiset.Entry<byte[]>> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> containsAll(Collection<? extends Multiset.Entry<byte[]>> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> retainAll(Collection<? extends Multiset.Entry<byte[]>> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Boolean> removeAll(Collection<? extends Multiset.Entry<byte[]>> c) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Void> addListener(CollectionEventListener<Multiset.Entry<byte[]>> listener, Executor executor) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public CompletableFuture<Void> removeListener(CollectionEventListener<Multiset.Entry<byte[]>> listener) {
        return Futures.exceptionalFuture(new UnsupportedOperationException());
      }

      @Override
      public AsyncIterator<Multiset.Entry<byte[]>> iterator() {
        return new PartitionedProxyIterator<>(
            getProxyClient(),
            AtomicMultimapService::iterateValuesSet,
            AtomicMultimapService::nextValuesSet,
            AtomicMultimapService::closeValuesSet);
      }

      @Override
      public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Multiset.Entry<byte[]>>> transactionLog) {
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
        return AtomicMultimapProxy.this.close();
      }

      @Override
      public CompletableFuture<Void> delete() {
        return AtomicMultimapProxy.this.delete();
      }

      @Override
      public DistributedSet<Multiset.Entry<byte[]>> sync(Duration operationTimeout) {
        return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
      }
    }
  }

  private class Entries implements AsyncDistributedCollection<Map.Entry<String, byte[]>> {
    private final Map<CollectionEventListener<Map.Entry<String, byte[]>>, AtomicMultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return AtomicMultimapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedCollectionType.instance();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<String, byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<String, byte[]> element) {
      return AtomicMultimapProxy.this.remove(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return AtomicMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return AtomicMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return AtomicMultimapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Map.Entry<String, byte[]> element) {
      return containsEntry(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Map.Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Map.Entry<String, byte[]>> entries) {
      return Futures.allOf(entries.stream()
          .map(entry -> containsEntry(entry.getKey(), entry.getValue())))
          .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(true));
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Map.Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Map.Entry<String, byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public AsyncIterator<Map.Entry<String, byte[]>> iterator() {
      return new PartitionedProxyIterator<>(
          getProxyClient(),
          AtomicMultimapService::iterateEntries,
          AtomicMultimapService::nextEntries,
          AtomicMultimapService::closeEntries);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<String, byte[]>> listener, Executor executor) {
      AtomicMultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADD, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case REMOVE:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVE, Maps.immutableEntry(event.key(), event.oldValue())));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return AtomicMultimapProxy.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<String, byte[]>> listener) {
      AtomicMultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return AtomicMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicMultimapProxy.this.delete();
    }

    @Override
    public DistributedCollection<Map.Entry<String, byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }
  }
}
