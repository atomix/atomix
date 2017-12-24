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

package io.atomix.core.multimap.impl;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;

import io.atomix.core.multimap.AsyncConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.MultimapEvent;
import io.atomix.core.multimap.MultimapEventListener;
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
 * An {@link AsyncConsistentMultimap} that maps its operation to operations to
 * a differently typed {@link AsyncConsistentMultimap} by transcoding operation
 * inputs and outputs while maintaining version numbers.
 *
 * @param <K2> key type of other map
 * @param <V2> value type of other map
 * @param <K1> key type of this map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncConsistentMultimap<K1, V1, K2, V2> implements AsyncConsistentMultimap<K1, V1> {

  private final AsyncConsistentMultimap<K2, V2> backingMap;
  private final Function<K1, K2> keyEncoder;
  private final Function<K2, K1> keyDecoder;
  private final Function<V2, V1> valueDecoder;
  private final Function<V1, V2> valueEncoder;
  private final Function<? extends Versioned<V2>,
      ? extends Versioned<V1>> versionedValueTransform;
  private final Function<Versioned<Collection<? extends V2>>,
      Versioned<Collection<? extends V1>>> versionedValueCollectionDecode;
  private final Function<Collection<? extends V1>, Collection<V2>>
      valueCollectionEncode;
  private final Map<MultimapEventListener<K1, V1>, InternalBackingMultimapEventListener> listeners =
      Maps.newIdentityHashMap();

  public TranscodingAsyncConsistentMultimap(
      AsyncConsistentMultimap<K2, V2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
    this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
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
  public CompletableFuture<Set<K1>> keySet() {
    return backingMap.keySet().thenApply(s -> s.stream()
        .map(keyDecoder)
        .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Multiset<K1>> keys() {
    return backingMap.keys().thenApply(s -> s.stream().map(keyDecoder)
        .collect(new MultisetCollector<>()));
  }

  @Override
  public CompletableFuture<Multiset<V1>> values() {
    return backingMap.values().thenApply(s ->
        s.stream().map(valueDecoder).collect(new MultisetCollector<>()));
  }

  @Override
  public CompletableFuture<Collection<Map.Entry<K1, V1>>> entries() {
    return backingMap.entries().thenApply(s -> s.stream()
        .map(e -> Maps.immutableEntry(keyDecoder.apply(e.getKey()),
            valueDecoder.apply(e.getValue())))
        .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Map<K1, Collection<V1>>> asMap() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public String name() {
    return backingMap.name();
  }

  @Override
  public CompletableFuture<Void> addListener(MultimapEventListener<K1, V1> listener, Executor executor) {
    synchronized (listeners) {
      InternalBackingMultimapEventListener backingMapListener =
          listeners.computeIfAbsent(listener, k -> new InternalBackingMultimapEventListener(listener));
      return backingMap.addListener(backingMapListener, executor);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(MultimapEventListener<K1, V1> listener) {
    synchronized (listeners) {
      InternalBackingMultimapEventListener backingMapListener = listeners.remove(listener);
      if (backingMapListener != null) {
        return backingMap.removeListener(backingMapListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    backingMap.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    backingMap.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return backingMap.statusChangeListeners();
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingMap.close();
  }

  @Override
  public ConsistentMultimap<K1, V1> sync(Duration operationTimeout) {
    return new BlockingConsistentMultimap<>(this, operationTimeout.toMillis());
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

  private class InternalBackingMultimapEventListener implements MultimapEventListener<K2, V2> {

    private final MultimapEventListener<K1, V1> listener;

    InternalBackingMultimapEventListener(MultimapEventListener<K1, V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(MultimapEvent<K2, V2> event) {
      listener.event(new MultimapEvent(event.name(),
          keyDecoder.apply(event.key()),
          valueDecoder.apply(event.newValue()),
          valueDecoder.apply(event.oldValue())));
    }
  }
}
