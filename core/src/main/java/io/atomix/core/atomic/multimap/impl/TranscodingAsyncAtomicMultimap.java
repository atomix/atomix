/*
 * Copyright 2016 Open Networking Foundation
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

package io.atomix.core.atomic.multimap.impl;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.multiset.AsyncDistributedMultiset;
import io.atomix.core.collection.set.AsyncDistributedSet;
import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.collection.multiset.impl.TranscodingAsyncDistributedMultiset;
import io.atomix.core.collection.set.impl.TranscodingAsyncDistributedSet;
import io.atomix.core.atomic.multimap.AsyncAtomicMultimap;
import io.atomix.core.atomic.multimap.AtomicMultimap;
import io.atomix.core.atomic.multimap.AtomicMultimapEvent;
import io.atomix.core.atomic.multimap.AtomicMultimapEventListener;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * An {@link AsyncAtomicMultimap} that maps its operation to operations to
 * a differently typed {@link AsyncAtomicMultimap} by transcoding operation
 * inputs and outputs while maintaining version numbers.
 *
 * @param <K2> key type of other map
 * @param <V2> value type of other map
 * @param <K1> key type of this map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncAtomicMultimap<K1, V1, K2, V2> implements AsyncAtomicMultimap<K1, V1> {

  private final AsyncAtomicMultimap<K2, V2> backingMap;
  private final Function<K1, K2> keyEncoder;
  private final Function<K2, K1> keyDecoder;
  private final Function<V2, V1> valueDecoder;
  private final Function<V1, V2> valueEncoder;
  private final Function<Map.Entry<K1, V1>, Map.Entry<K2, V2>> entryEncoder;
  private final Function<Map.Entry<K2, V2>, Map.Entry<K1, V1>> entryDecoder;
  private final Function<? extends Versioned<V2>,
      ? extends Versioned<V1>> versionedValueTransform;
  private final Function<Versioned<Collection<? extends V2>>,
      Versioned<Collection<? extends V1>>> versionedValueCollectionDecode;
  private final Function<Collection<? extends V1>, Collection<V2>>
      valueCollectionEncode;
  private final Map<AtomicMultimapEventListener<K1, V1>, InternalBackingAtomicMultimapEventListener> listeners =
      Maps.newIdentityHashMap();

  public TranscodingAsyncAtomicMultimap(
      AsyncAtomicMultimap<K2, V2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
    this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
    this.entryEncoder = e -> Maps.immutableEntry(this.keyEncoder.apply(e.getKey()), this.valueEncoder.apply(e.getValue()));
    this.entryDecoder = e -> Maps.immutableEntry(this.keyDecoder.apply(e.getKey()), this.valueDecoder.apply(e.getValue()));
    this.versionedValueTransform = v -> v == null ? null :
        v.map(valueDecoder);
    this.versionedValueCollectionDecode = v -> v == null ? null :
        new Versioned<>(
            v.value()
                .stream()
                .map(valueDecoder)
                .collect(Collectors.toSet()),
            v.version(),
            v.creationTime());
    this.valueCollectionEncode = v -> v == null ? null :
        v.stream().map(valueEncoder).collect(Collectors.toSet());
  }

  @Override
  public String name() {
    return backingMap.name();
  }

  @Override
  public PrimitiveType type() {
    return backingMap.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return backingMap.protocol();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return backingMap.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K1 key) {
    try {
      return backingMap.containsKey(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V1 value) {
    try {
      return backingMap.containsValue(valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(K1 key, V1 value) {
    try {
      return backingMap.containsEntry(keyEncoder.apply(key),
          valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> put(K1 key, V1 value) {
    try {
      return backingMap.put(keyEncoder.apply(key),
          valueEncoder.apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, V1 value) {
    try {
      return backingMap.remove(keyEncoder.apply(key), valueEncoder
          .apply(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> removeAll(
      K1 key, Collection<? extends V1> values) {
    try {
      return backingMap.removeAll(
          keyEncoder.apply(key),
          values.stream().map(valueEncoder).collect(
              Collectors.toSet()));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V1>>>
  removeAll(K1 key) {
    try {
      return backingMap.removeAll(keyEncoder.apply(key))
          .thenApply(versionedValueCollectionDecode);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean>
  putAll(K1 key, Collection<? extends V1> values) {
    try {
      return backingMap.putAll(keyEncoder.apply(key),
          valueCollectionEncode.apply(values));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V1>>>
  replaceValues(K1 key, Collection<V1> values) {
    try {
      return backingMap.replaceValues(keyEncoder.apply(key),
          valueCollectionEncode.apply(values))
          .thenApply(versionedValueCollectionDecode);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> clear() {
    return backingMap.clear();
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends V1>>> get(K1 key) {
    try {
      return backingMap.get(keyEncoder.apply(key))
          .thenApply(versionedValueCollectionDecode);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public AsyncDistributedSet<K1> keySet() {
    return new TranscodingAsyncDistributedSet<>(backingMap.keySet(), keyEncoder, keyDecoder);
  }

  @Override
  public AsyncDistributedMultiset<K1> keys() {
    return new TranscodingAsyncDistributedMultiset<>(backingMap.keys(), keyEncoder, keyDecoder);
  }

  @Override
  public AsyncDistributedMultiset<V1> values() {
    return new TranscodingAsyncDistributedMultiset<>(backingMap.values(), valueEncoder, valueDecoder);
  }

  @Override
  public AsyncDistributedCollection<Map.Entry<K1, V1>> entries() {
    return new TranscodingAsyncDistributedCollection<>(backingMap.entries(), entryEncoder, entryDecoder);
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMultimapEventListener<K1, V1> listener, Executor executor) {
    synchronized (listeners) {
      InternalBackingAtomicMultimapEventListener backingMapListener =
          listeners.computeIfAbsent(listener, k -> new InternalBackingAtomicMultimapEventListener(listener));
      return backingMap.addListener(backingMapListener, executor);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMultimapEventListener<K1, V1> listener) {
    synchronized (listeners) {
      InternalBackingAtomicMultimapEventListener backingMapListener = listeners.remove(listener);
      if (backingMapListener != null) {
        return backingMap.removeListener(backingMapListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    backingMap.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    backingMap.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingMap.close();
  }

  @Override
  public AtomicMultimap<K1, V1> sync(Duration operationTimeout) {
    return new BlockingAtomicMultimap<>(this, operationTimeout.toMillis());
  }

  private class MultisetCollector<T> implements Collector<T,
      ImmutableMultiset.Builder<T>,
      Multiset<T>> {

    @Override
    public Supplier<ImmutableMultiset.Builder<T>> supplier() {
      return ImmutableMultiset::builder;
    }

    @Override
    public BiConsumer<ImmutableMultiset.Builder<T>, T> accumulator() {
      return ((builder, t) -> builder.add(t));
    }

    @Override
    public BinaryOperator<ImmutableMultiset.Builder<T>> combiner() {
      return (a, b) -> {
        a.addAll(b.build());
        return a;
      };
    }

    @Override
    public Function<ImmutableMultiset.Builder<T>, Multiset<T>> finisher() {
      return ImmutableMultiset.Builder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }

  private class InternalBackingAtomicMultimapEventListener implements AtomicMultimapEventListener<K2, V2> {

    private final AtomicMultimapEventListener<K1, V1> listener;

    InternalBackingAtomicMultimapEventListener(AtomicMultimapEventListener<K1, V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(AtomicMultimapEvent<K2, V2> event) {
      listener.event(new AtomicMultimapEvent(event.name(),
          keyDecoder.apply(event.key()),
          valueDecoder.apply(event.newValue()),
          valueDecoder.apply(event.oldValue())));
    }
  }
}
