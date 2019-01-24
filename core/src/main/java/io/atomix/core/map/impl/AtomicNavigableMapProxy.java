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
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.ProxyIterator;
import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedNavigableSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.set.impl.DescendingAsyncDistributedNavigableSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link AsyncAtomicNavigableMap}.
 */
public class AtomicNavigableMapProxy<K extends Comparable<K>> extends AbstractAtomicMapProxy<AsyncAtomicNavigableMap<K, byte[]>, AtomicTreeMapService<K>, K> implements AsyncAtomicNavigableMap<K, byte[]> {
  public AtomicNavigableMapProxy(ProxyClient<AtomicTreeMapService<K>> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return getProxyClient().applyBy(name(), service -> service.firstKey());
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return getProxyClient().applyBy(name(), service -> service.lastKey());
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> ceilingEntry(K key) {
    return getProxyClient().applyBy(name(), service -> service.ceilingEntry(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> floorEntry(K key) {
    return getProxyClient().applyBy(name(), service -> service.floorEntry(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> higherEntry(K key) {
    return getProxyClient().applyBy(name(), service -> service.higherEntry(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lowerEntry(K key) {
    return getProxyClient().applyBy(name(), service -> service.lowerEntry(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> firstEntry() {
    return getProxyClient().applyBy(name(), service -> service.firstEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lastEntry() {
    return getProxyClient().applyBy(name(), service -> service.lastEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollFirstEntry() {
    return getProxyClient().applyBy(name(), service -> service.pollFirstEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollLastEntry() {
    return getProxyClient().applyBy(name(), service -> service.pollLastEntry());
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return getProxyClient().applyBy(name(), service -> service.lowerKey(key));
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return getProxyClient().applyBy(name(), service -> service.floorKey(key));
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return getProxyClient().applyBy(name(), service -> service.ceilingKey(key));
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return getProxyClient().applyBy(name(), service -> service.higherKey(key));
  }

  @Override
  public AsyncAtomicNavigableMap<K, byte[]> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new SubMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public AsyncAtomicNavigableMap<K, byte[]> headMap(K toKey, boolean inclusive) {
    return new SubMap(null, false, toKey, inclusive);
  }

  @Override
  public AsyncAtomicNavigableMap<K, byte[]> tailMap(K fromKey, boolean inclusive) {
    return new SubMap(fromKey, inclusive, null, false);
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return new KeySet(null, false, null, false);
  }

  @Override
  public AsyncAtomicNavigableMap<K, byte[]> descendingMap() {
    return new DescendingAsyncAtomicNavigableMap<>(this);
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return navigableKeySet().descendingSet();
  }

  @Override
  public AtomicNavigableMap<K, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicNavigableMap<>(this, operationTimeout.toMillis());
  }

  protected abstract class SubSet implements AsyncPrimitive {
    protected final K fromKey;
    protected final boolean fromInclusive;
    protected final K toKey;
    protected final boolean toInclusive;

    public SubSet(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      this.fromKey = fromKey;
      this.fromInclusive = fromInclusive;
      this.toKey = toKey;
      this.toInclusive = toInclusive;
    }

    @Override
    public String name() {
      return AtomicNavigableMapProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return AtomicNavigableMapProxy.this.type();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return AtomicNavigableMapProxy.this.protocol();
    }

    protected boolean isInBounds(K key) {
      return key != null && isInLowerBounds(key) && isInUpperBounds(key);
    }

    protected boolean isInLowerBounds(K key) {
      if (fromKey != null) {
        int lower = key.compareTo(fromKey);
        if (!fromInclusive && lower <= 0 || fromInclusive && lower < 0) {
          return false;
        }
      }
      return true;
    }

    protected boolean isInUpperBounds(K key) {
      if (toKey != null) {
        int upper = key.compareTo(toKey);
        if (!toInclusive && upper >= 0 || toInclusive && upper > 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    public CompletableFuture<Void> close() {
      return AtomicNavigableMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicNavigableMapProxy.this.delete();
    }
  }

  private class KeySet extends SubSet implements AsyncDistributedNavigableSet<K> {
    private final Map<CollectionEventListener<K>, AtomicMapEventListener<K, byte[]>> listenerMap = Maps.newConcurrentMap();

    KeySet(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public CompletableFuture<K> lower(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapLowerKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> floor(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapFloorKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> ceiling(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapCeilingKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> higher(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapHigherKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> pollFirst() {
      return getProxyClient().applyBy(name(), service -> service.subMapPollFirstKey(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> pollLast() {
      return getProxyClient().applyBy(name(), service -> service.subMapPollLastKey(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public AsyncDistributedNavigableSet<K> descendingSet() {
      return new DescendingAsyncDistributedNavigableSet<>(this);
    }

    @Override
    public AsyncIterator<K> iterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.subMapIterateKeys(fromKey, fromInclusive, toKey, toInclusive),
          AtomicTreeMapService::nextKeys,
          AtomicTreeMapService::closeKeys);
    }

    @Override
    public AsyncIterator<K> descendingIterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.subMapIterateDescendingKeys(fromKey, fromInclusive, toKey, toInclusive),
          AtomicTreeMapService::nextKeys,
          AtomicTreeMapService::closeKeys);
    }

    @Override
    public AsyncDistributedNavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
      checkNotNull(fromElement);
      checkNotNull(toElement);

      if (this.fromKey != null) {
        int order = this.fromKey.compareTo(fromElement);
        if (order == 0) {
          fromInclusive = this.fromInclusive && fromInclusive;
        } else if (order > 0) {
          fromElement = this.fromKey;
          fromInclusive = this.fromInclusive;
        }
      }

      if (this.toKey != null) {
        int order = this.toKey.compareTo(toElement);
        if (order == 0) {
          toInclusive = this.toInclusive && toInclusive;
        } else if (order < 0) {
          toElement = this.toKey;
          toInclusive = this.toInclusive;
        }
      }
      return new KeySet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public AsyncDistributedNavigableSet<K> headSet(K toElement, boolean inclusive) {
      checkNotNull(toElement);

      if (this.toKey != null) {
        int order = this.toKey.compareTo(toElement);
        if (order == 0) {
          inclusive = this.toInclusive && inclusive;
        } else if (order < 0) {
          toElement = this.toKey;
          inclusive = this.toInclusive;
        }
      }
      return new KeySet(fromKey, fromInclusive, toElement, inclusive);
    }

    @Override
    public AsyncDistributedNavigableSet<K> tailSet(K fromElement, boolean inclusive) {
      checkNotNull(fromElement);

      if (this.fromKey != null) {
        int order = this.fromKey.compareTo(fromElement);
        if (order == 0) {
          inclusive = this.fromInclusive && inclusive;
        } else if (order > 0) {
          fromElement = this.fromKey;
          inclusive = this.fromInclusive;
        }
      }
      return new KeySet(fromElement, inclusive, toKey, toInclusive);
    }

    @Override
    public AsyncDistributedSortedSet<K> subSet(K fromElement, K toElement) {
      return subSet(fromElement, true, toElement, false);
    }

    @Override
    public AsyncDistributedSortedSet<K> headSet(K toElement) {
      return headSet(toElement, false);
    }

    @Override
    public AsyncDistributedSortedSet<K> tailSet(K fromElement) {
      return tailSet(fromElement, true);
    }

    @Override
    public CompletableFuture<K> first() {
      return getProxyClient().applyBy(name(), service -> service.subMapFirstKey(fromKey, fromInclusive, toKey, toInclusive))
          .thenCompose(result -> result != null ? Futures.completedFuture(result) : Futures.exceptionalFuture(new NoSuchElementException()));
    }

    @Override
    public CompletableFuture<K> last() {
      return getProxyClient().applyBy(name(), service -> service.subMapLastKey(fromKey, fromInclusive, toKey, toInclusive))
          .thenCompose(result -> result != null ? Futures.completedFuture(result) : Futures.exceptionalFuture(new NoSuchElementException()));
    }

    @Override
    public CompletableFuture<Boolean> add(K element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(K element) {
      if (!isInBounds(element)) {
        return CompletableFuture.completedFuture(false);
      }
      return AtomicNavigableMapProxy.this.remove(element).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyBy(name(), service -> service.subMapSize(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return size().thenApply(size -> size == 0);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return getProxyClient().acceptBy(name(), service -> service.subMapClear(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> contains(K element) {
      return !isInBounds(element) ? CompletableFuture.completedFuture(false) : containsKey(element);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends K> c) {
      if (c.stream().map(this::isInBounds).reduce(Boolean::logicalAnd).orElse(true)) {
        return getProxyClient().applyBy(name(), service -> service.containsKeys(c));
      }
      return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends K> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<K> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> mapListener = event -> {
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
      if (listenerMap.putIfAbsent(listener, mapListener) == null) {
        return AtomicNavigableMapProxy.this.addListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<K> listener) {
      AtomicMapEventListener<K, byte[]> mapListener = listenerMap.remove(listener);
      if (mapListener != null) {
        return AtomicNavigableMapProxy.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<K>> transactionLog) {
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
      return AtomicNavigableMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicNavigableMapProxy.this.delete();
    }

    @Override
    public DistributedNavigableSet<K> sync(Duration operationTimeout) {
      return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
    }
  }

  private class SubMap extends SubSet implements AsyncAtomicNavigableMap<K, byte[]> {
    private final Map<AtomicMapEventListener<K, byte[]>, AtomicMapEventListener<K, byte[]>> listenerMap = Maps.newConcurrentMap();

    SubMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lowerEntry(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapLowerEntry(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> lowerKey(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapLowerKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> floorEntry(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapFloorEntry(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> floorKey(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapFloorKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> ceilingEntry(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapCeilingEntry(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> ceilingKey(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapCeilingKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> higherEntry(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapHigherEntry(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<K> higherKey(K key) {
      return getProxyClient().applyBy(name(), service -> service.subMapHigherKey(key, fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> firstEntry() {
      return getProxyClient().applyBy(name(), service -> service.subMapFirstEntry(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> lastEntry() {
      return getProxyClient().applyBy(name(), service -> service.subMapLastEntry(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollFirstEntry() {
      return getProxyClient().applyBy(name(), service -> service.subMapPollFirstEntry(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Map.Entry<K, Versioned<byte[]>>> pollLastEntry() {
      return getProxyClient().applyBy(name(), service -> service.subMapPollLastEntry(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public AsyncAtomicNavigableMap<K, byte[]> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      checkNotNull(fromKey);
      checkNotNull(toKey);

      if (this.fromKey != null) {
        int order = this.fromKey.compareTo(fromKey);
        if (order == 0) {
          fromInclusive = this.fromInclusive && fromInclusive;
        } else if (order > 0) {
          fromKey = this.fromKey;
          fromInclusive = this.fromInclusive;
        }
      }

      if (this.toKey != null) {
        int order = this.toKey.compareTo(toKey);
        if (order == 0) {
          toInclusive = this.toInclusive && toInclusive;
        } else if (order < 0) {
          toKey = this.toKey;
          toInclusive = this.toInclusive;
        }
      }
      return new SubMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public AsyncAtomicNavigableMap<K, byte[]> headMap(K toKey, boolean inclusive) {
      checkNotNull(toKey);
      if (this.toKey != null) {
        int order = this.toKey.compareTo(toKey);
        if (order == 0) {
          inclusive = this.toInclusive && inclusive;
        } else if (order < 0) {
          toKey = this.toKey;
          inclusive = this.toInclusive;
        }
      }
      return new SubMap(fromKey, fromInclusive, toKey, inclusive);
    }

    @Override
    public AsyncAtomicNavigableMap<K, byte[]> tailMap(K fromKey, boolean inclusive) {
      checkNotNull(fromKey);
      if (this.fromKey != null) {
        int order = this.fromKey.compareTo(fromKey);
        if (order == 0) {
          inclusive = this.fromInclusive && inclusive;
        } else if (order > 0) {
          fromKey = this.fromKey;
          inclusive = this.fromInclusive;
        }
      }
      return new SubMap(fromKey, inclusive, toKey, toInclusive);
    }

    @Override
    public AsyncAtomicNavigableMap<K, byte[]> descendingMap() {
      return new DescendingAsyncAtomicNavigableMap<>(this);
    }

    @Override
    public AsyncDistributedNavigableSet<K> navigableKeySet() {
      return new KeySet(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public AsyncDistributedNavigableSet<K> descendingKeySet() {
      return navigableKeySet().descendingSet();
    }

    @Override
    public CompletableFuture<K> firstKey() {
      return getProxyClient().applyBy(name(), service -> service.subMapFirstKey(fromKey, fromInclusive, toKey, toInclusive))
          .thenCompose(result -> result != null ? Futures.completedFuture(result) : Futures.exceptionalFuture(new NoSuchElementException()));
    }

    @Override
    public CompletableFuture<K> lastKey() {
      return getProxyClient().applyBy(name(), service -> service.subMapLastKey(fromKey, fromInclusive, toKey, toInclusive))
          .thenCompose(result -> result != null ? Futures.completedFuture(result) : Futures.exceptionalFuture(new NoSuchElementException()));
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyBy(name(), service -> service.subMapSize(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> containsKey(K key) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(false) : AtomicNavigableMapProxy.this.containsKey(key);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(byte[] value) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> get(K key) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.get(key);
    }

    @Override
    public CompletableFuture<Map<K, Versioned<byte[]>>> getAllPresent(Iterable<K> keys) {
      return AtomicNavigableMapProxy.this.getAllPresent(Lists.newArrayList(keys).stream().filter(this::isInBounds).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> getOrDefault(K key, byte[] defaultValue) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.getOrDefault(key, defaultValue);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> computeIf(
        K key, Predicate<? super byte[]> condition, BiFunction<? super K, ? super byte[], ? extends byte[]> remappingFunction) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.computeIf(key, condition, remappingFunction);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> put(K key, byte[] value, Duration ttl) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.put(key, value, ttl);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> putAndGet(K key, byte[] value, Duration ttl) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.putAndGet(key, value, ttl);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> remove(K key) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.remove(key);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return getProxyClient().acceptBy(name(), service -> service.subMapClear(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public AsyncDistributedSet<K> keySet() {
      return new KeySet(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public AsyncDistributedCollection<Versioned<byte[]>> values() {
      return new Values(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public AsyncDistributedSet<Map.Entry<K, Versioned<byte[]>>> entrySet() {
      return new EntrySet(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> putIfAbsent(K key, byte[] value, Duration ttl) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.putIfAbsent(key, value, ttl);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, byte[] value) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(false) : AtomicNavigableMapProxy.this.remove(key, value);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, long version) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(false) : AtomicNavigableMapProxy.this.remove(key, version);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> replace(K key, byte[] value) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(null) : AtomicNavigableMapProxy.this.replace(key, value);
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, byte[] oldValue, byte[] newValue) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(false) : AtomicNavigableMapProxy.this.replace(key, oldValue, newValue);
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, long oldVersion, byte[] newValue) {
      return !isInBounds(key) ? CompletableFuture.completedFuture(false) : AtomicNavigableMapProxy.this.replace(key, oldVersion, newValue);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(AtomicMapEventListener<K, byte[]> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> boundedListener = event -> {
        if (isInBounds(event.key())) {
          listener.event(event);
        }
      };
      if (listenerMap.putIfAbsent(listener, boundedListener) == null) {
        return AtomicNavigableMapProxy.this.addListener(boundedListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(AtomicMapEventListener<K, byte[]> listener) {
      AtomicMapEventListener<K, byte[]> boundedListener = listenerMap.remove(listener);
      if (boundedListener != null) {
        return AtomicNavigableMapProxy.this.removeListener(boundedListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
      return AtomicNavigableMapProxy.this.prepare(transactionLog);
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      return AtomicNavigableMapProxy.this.commit(transactionId);
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      return AtomicNavigableMapProxy.this.rollback(transactionId);
    }

    @Override
    public AtomicNavigableMap<K, byte[]> sync(Duration operationTimeout) {
      return new BlockingAtomicNavigableMap<>(this, operationTimeout.toMillis());
    }
  }

  private class EntrySet extends SubSet implements AsyncDistributedSet<Map.Entry<K, Versioned<byte[]>>> {
    private final Map<CollectionEventListener<Map.Entry<K, Versioned<byte[]>>>, AtomicMapEventListener<K, byte[]>> listenerMap = Maps.newConcurrentMap();

    EntrySet(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<K, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<K, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyBy(name(), service -> service.subMapSize(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return size().thenApply(size -> size == 0);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return getProxyClient().acceptBy(name(), service -> service.subMapClear(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> contains(Map.Entry<K, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Map.Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Map.Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Map.Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Map.Entry<K, Versioned<byte[]>>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<K, Versioned<byte[]>>> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> boundedListener = event -> {
        if (isInBounds(event.key())) {
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
        }
      };
      if (listenerMap.putIfAbsent(listener, boundedListener) == null) {
        return AtomicNavigableMapProxy.this.addListener(boundedListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<K, Versioned<byte[]>>> listener) {
      AtomicMapEventListener<K, byte[]> boundedListener = listenerMap.remove(listener);
      if (boundedListener != null) {
        return AtomicNavigableMapProxy.this.removeListener(boundedListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<K, Versioned<byte[]>>> iterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.subMapIterateEntries(fromKey, fromInclusive, toKey, toInclusive),
          AtomicTreeMapService::nextEntries,
          AtomicTreeMapService::closeEntries);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<Map.Entry<K, Versioned<byte[]>>>> transactionLog) {
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
      return AtomicNavigableMapProxy.this.close();
    }

    @Override
    public CompletableFuture<Void> delete() {
      return AtomicNavigableMapProxy.this.delete();
    }

    @Override
    public DistributedSet<Map.Entry<K, Versioned<byte[]>>> sync(Duration operationTimeout) {
      return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
    }
  }

  private class Values extends SubSet implements AsyncDistributedCollection<Versioned<byte[]>> {
    private final Map<CollectionEventListener<Versioned<byte[]>>, AtomicMapEventListener<K, byte[]>> listenerMap = Maps.newConcurrentMap();

    Values(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public CompletableFuture<Boolean> add(Versioned<byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Versioned<byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyBy(name(), service -> service.subMapSize(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return size().thenApply(size -> size == 0);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return getProxyClient().acceptBy(name(), service -> service.subMapClear(fromKey, fromInclusive, toKey, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> contains(Versioned<byte[]> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends Versioned<byte[]>> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<Versioned<byte[]>> listener, Executor executor) {
      AtomicMapEventListener<K, byte[]> boundedListener = event -> {
        if (isInBounds(event.key())) {
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
        }
      };
      if (listenerMap.putIfAbsent(listener, boundedListener) == null) {
        return AtomicNavigableMapProxy.this.addListener(boundedListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<byte[]>> listener) {
      AtomicMapEventListener<K, byte[]> boundedListener = listenerMap.remove(listener);
      if (boundedListener != null) {
        return AtomicNavigableMapProxy.this.removeListener(boundedListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Versioned<byte[]>> iterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.subMapIterateValues(fromKey, fromInclusive, toKey, toInclusive),
          AtomicTreeMapService::nextValues,
          AtomicTreeMapService::closeValues);
    }

    @Override
    public DistributedCollection<Versioned<byte[]>> sync(Duration operationTimeout) {
      return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
    }
  }
}
