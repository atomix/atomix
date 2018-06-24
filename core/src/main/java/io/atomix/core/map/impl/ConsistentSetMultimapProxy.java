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

package io.atomix.core.map.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.map.AsyncConsistentMultimap;
import io.atomix.core.map.ConsistentMultimap;
import io.atomix.core.map.MultimapEvent;
import io.atomix.core.map.MultimapEventListener;
import io.atomix.core.set.AsyncDistributedMultiset;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedMultiset;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.SetEvent;
import io.atomix.core.set.SetEventListener;
import io.atomix.core.set.impl.BlockingDistributedMultiset;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
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
  public AsyncDistributedSet<String> keySet() {
    return new ConsistentSetMultimapKeySet();
  }

  @Override
  public AsyncDistributedMultiset<String> keys() {
    return new ConsistentSetMultimapKeys();
  }

  @Override
  public AsyncDistributedMultiset<byte[]> values() {
    return new ConsistentSetMultimapValues();
  }

  @Override
  public AsyncDistributedCollection<Map.Entry<String, byte[]>> entries() {
    return new ConsistentSetMultimapEntries();
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

  /**
   * Multimap key set.
   */
  private class ConsistentSetMultimapKeySet implements AsyncDistributedSet<String> {
    private final Map<SetEventListener<String>, MultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return ConsistentSetMultimapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ConsistentSetMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return ConsistentSetMultimapProxy.this.removeAll(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyAll(service -> service.keyCount())
          .thenApply(results -> results.reduce(Math::addExact).orElse(0));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ConsistentSetMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ConsistentSetMultimapProxy.this.clear();
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
    public CompletableFuture<AsyncIterator<String>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateKeySet())
              .thenApply(iteratorId -> new ConsistentMultimapPartitionIterator<String>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextKeySet(iteratorId, position),
                  service -> service.closeKeySet(iteratorId)))))
          .thenApply(iterators -> new ConsistentMultimapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(SetEventListener<String> listener) {
      MultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new SetEvent<>(name(), SetEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new SetEvent<>(name(), SetEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      eventListeners.put(listener, mapListener);
      return ConsistentSetMultimapProxy.this.addListener(mapListener);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(SetEventListener<String> listener) {
      MultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return ConsistentSetMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public DistributedSet<String> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }
  }

  private class ConsistentSetMultimapKeys implements AsyncDistributedMultiset<String> {
    private final Map<SetEventListener<String>, MultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return ConsistentSetMultimapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ConsistentSetMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(String element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return ConsistentSetMultimapProxy.this.removeAll(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return ConsistentSetMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ConsistentSetMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ConsistentSetMultimapProxy.this.clear();
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
    public CompletableFuture<AsyncIterator<String>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateKeys())
              .thenApply(iteratorId -> new ConsistentMultimapPartitionIterator<String>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextKeys(iteratorId, position),
                  service -> service.closeKeys(iteratorId)))))
          .thenApply(iterators -> new ConsistentMultimapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(SetEventListener<String> listener) {
      MultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new SetEvent<>(name(), SetEvent.Type.ADD, event.key()));
            break;
          case REMOVE:
            listener.event(new SetEvent<>(name(), SetEvent.Type.REMOVE, event.key()));
            break;
          default:
            break;
        }
      };
      eventListeners.put(listener, mapListener);
      return ConsistentSetMultimapProxy.this.addListener(mapListener);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(SetEventListener<String> listener) {
      MultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return ConsistentSetMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public DistributedMultiset<String> sync(Duration operationTimeout) {
      return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
    }
  }

  private class ConsistentSetMultimapValues implements AsyncDistributedMultiset<byte[]> {
    private final Map<SetEventListener<byte[]>, MultimapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return ConsistentSetMultimapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ConsistentSetMultimapProxy.this.protocol();
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
      return ConsistentSetMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ConsistentSetMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ConsistentSetMultimapProxy.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(byte[] element) {
      return containsValue(element);
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
    public CompletableFuture<AsyncIterator<byte[]>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateValues())
              .thenApply(iteratorId -> new ConsistentMultimapPartitionIterator<byte[]>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextValues(iteratorId, position),
                  service -> service.closeValues(iteratorId)))))
          .thenApply(iterators -> new ConsistentMultimapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(SetEventListener<byte[]> listener) {
      MultimapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERT:
            listener.event(new SetEvent<>(name(), SetEvent.Type.ADD, event.newValue()));
            break;
          case REMOVE:
            listener.event(new SetEvent<>(name(), SetEvent.Type.REMOVE, event.oldValue()));
            break;
          default:
            break;
        }
      };
      eventListeners.put(listener, mapListener);
      return ConsistentSetMultimapProxy.this.addListener(mapListener);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(SetEventListener<byte[]> listener) {
      MultimapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return ConsistentSetMultimapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public DistributedMultiset<byte[]> sync(Duration operationTimeout) {
      return new BlockingDistributedMultiset<>(this, operationTimeout.toMillis());
    }
  }

  private class ConsistentSetMultimapEntries implements AsyncDistributedCollection<Map.Entry<String, byte[]>> {
    @Override
    public String name() {
      return ConsistentSetMultimapProxy.this.name();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return ConsistentSetMultimapProxy.this.protocol();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<String, byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<String, byte[]> element) {
      return ConsistentSetMultimapProxy.this.remove(element.getKey(), element.getValue());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return ConsistentSetMultimapProxy.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return ConsistentSetMultimapProxy.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return ConsistentSetMultimapProxy.this.clear();
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
    public CompletableFuture<AsyncIterator<Map.Entry<String, byte[]>>> iterator() {
      return Futures.allOf(getProxyClient().getPartitionIds().stream()
          .map(partitionId -> getProxyClient().applyOn(partitionId, service -> service.iterateEntries())
              .thenApply(iteratorId -> new ConsistentMultimapPartitionIterator<Map.Entry<String, byte[]>>(
                  partitionId,
                  iteratorId,
                  (service, position) -> service.nextEntries(iteratorId, position),
                  service -> service.closeEntries(iteratorId)))))
          .thenApply(iterators -> new ConsistentMultimapIterator<>(iterators.collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public DistributedCollection<Map.Entry<String, byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }
  }

  /**
   * Consistent multimap iterator.
   */
  private class ConsistentMultimapIterator<T> implements AsyncIterator<T> {
    private final Iterator<AsyncIterator<T>> iterators;
    private volatile AsyncIterator<T> iterator;

    public ConsistentMultimapIterator(Collection<AsyncIterator<T>> iterators) {
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
    public CompletableFuture<T> next() {
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
   * Consistent multimap partition iterator.
   */
  private class ConsistentMultimapPartitionIterator<T> implements AsyncIterator<T> {
    private final PartitionId partitionId;
    private final long iteratorId;
    private final BiFunction<ConsistentSetMultimapService, Integer, ConsistentSetMultimapService.Batch<T>> nextFunction;
    private final Consumer<ConsistentSetMultimapService> closeFunction;
    private volatile CompletableFuture<ConsistentSetMultimapService.Batch<T>> batch;
    private volatile CompletableFuture<Void> closeFuture;

    ConsistentMultimapPartitionIterator(
        PartitionId partitionId,
        long iteratorId,
        BiFunction<ConsistentSetMultimapService, Integer, ConsistentSetMultimapService.Batch<T>> nextFunction,
        Consumer<ConsistentSetMultimapService> closeFunction) {
      this.partitionId = partitionId;
      this.iteratorId = iteratorId;
      this.nextFunction = nextFunction;
      this.closeFunction = closeFunction;
      this.batch = CompletableFuture.completedFuture(
          new ConsistentSetMultimapService.Batch<T>(0, Collections.emptyList()));
    }

    /**
     * Returns the current batch iterator or lazily fetches the next batch from the cluster.
     *
     * @return the next batch iterator
     */
    private CompletableFuture<Iterator<T>> batch() {
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
    private CompletableFuture<ConsistentSetMultimapService.Batch<T>> fetch(int position) {
      return getProxyClient().applyOn(partitionId, service -> nextFunction.apply(service, position))
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
            closeFuture = getProxyClient().acceptOn(partitionId, service -> closeFunction.accept(service));
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
    public CompletableFuture<T> next() {
      return batch().thenCompose(iterator -> {
        if (iterator == null) {
          return Futures.exceptionalFuture(new NoSuchElementException());
        }
        return CompletableFuture.completedFuture(iterator.next());
      });
    }
  }
}