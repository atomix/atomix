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

import com.google.common.collect.Maps;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.map.AtomicNavigableMapType;
import io.atomix.core.transaction.TransactionId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class for tree map services.
 */
public abstract class AbstractAtomicNavigableMapService<K extends Comparable<K>> extends AbstractAtomicMapService<K> implements AtomicTreeMapService<K> {
  private final Serializer serializer;

  public AbstractAtomicNavigableMapService(PrimitiveType primitiveType) {
    super(primitiveType);
    serializer = Serializer.using(Namespace.builder()
        .register(AtomicNavigableMapType.instance().namespace())
        .register(SessionId.class)
        .register(TransactionId.class)
        .register(TransactionScope.class)
        .register(MapEntryValue.class)
        .register(MapEntryValue.Type.class)
        .register(new HashMap().keySet().getClass())
        .register(DefaultIterator.class)
        .register(AscendingIterator.class)
        .register(DescendingIterator.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  protected NavigableMap<K, MapEntryValue> createMap() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  protected NavigableMap<K, MapEntryValue> entries() {
    return (NavigableMap<K, MapEntryValue>) super.entries();
  }

  @Override
  public K firstKey() {
    return isEmpty() ? null : entries().firstKey();
  }

  @Override
  public K lastKey() {
    return isEmpty() ? null : entries().lastKey();
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> higherEntry(K key) {
    return isEmpty() ? null : toVersionedEntry(entries().higherEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> firstEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().firstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> lastEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().lastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> pollFirstEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().pollFirstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> pollLastEntry() {
    return isEmpty() ? null : toVersionedEntry(entries().pollLastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> lowerEntry(K key) {
    return toVersionedEntry(entries().lowerEntry(key));
  }

  @Override
  public K lowerKey(K key) {
    return entries().lowerKey(key);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> floorEntry(K key) {
    return toVersionedEntry(entries().floorEntry(key));
  }

  @Override
  public K floorKey(K key) {
    return entries().floorKey(key);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> ceilingEntry(K key) {
    return toVersionedEntry(entries().ceilingEntry(key));
  }

  @Override
  public K ceilingKey(K key) {
    return entries().ceilingKey(key);
  }

  @Override
  public K higherKey(K key) {
    return entries().higherKey(key);
  }

  @Override
  public K pollFirstKey() {
    Map.Entry<K, MapEntryValue> entry = entries().pollFirstEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public K pollLastKey() {
    Map.Entry<K, MapEntryValue> entry = entries().pollLastEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public K subMapFirstKey(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(NavigableMap::firstKey, fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapLastKey(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(NavigableMap::lastKey, fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapCeilingEntry(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.ceilingEntry(key)), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapFloorEntry(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.floorEntry(key)), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapHigherEntry(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.higherEntry(key)), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapLowerEntry(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.lowerEntry(key)), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapFirstEntry(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.firstEntry()), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapLastEntry(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.lastEntry()), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapPollFirstEntry(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.pollFirstEntry()), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, Versioned<byte[]>> subMapPollLastEntry(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> toVersionedEntry(map.pollLastEntry()), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapLowerKey(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> map.lowerKey(key), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapFloorKey(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> map.floorKey(key), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapCeilingKey(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> map.ceilingKey(key), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapHigherKey(K key, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> map.higherKey(key), fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapPollFirstKey(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> {
      Map.Entry<K, MapEntryValue> entry = map.pollFirstEntry();
      return entry != null ? entry.getKey() : null;
    }, fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public K subMapPollLastKey(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return subMapApply(map -> {
      Map.Entry<K, MapEntryValue> entry = map.pollLastEntry();
      return entry != null ? entry.getKey() : null;
    }, fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public int subMapSize(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    if (fromKey != null && toKey != null) {
      return entries().subMap(fromKey, fromInclusive, toKey, toInclusive).size();
    } else if (fromKey != null) {
      return entries().tailMap(fromKey, fromInclusive).size();
    } else if (toKey != null) {
      return entries().headMap(toKey, toInclusive).size();
    } else {
      return entries().size();
    }
  }

  @Override
  public IteratorBatch<K> subMapIterateKeys(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return iterate(sessionId -> new AscendingIterator(sessionId, fromKey, fromInclusive, toKey, toInclusive), (k, v) -> k);
  }

  @Override
  public IteratorBatch<Map.Entry<K, Versioned<byte[]>>> subMapIterateEntries(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return iterate(sessionId -> new AscendingIterator(sessionId, fromKey, fromInclusive, toKey, toInclusive), Maps::immutableEntry);
  }

  @Override
  public IteratorBatch<Versioned<byte[]>> subMapIterateValues(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return iterate(sessionId -> new AscendingIterator(sessionId, fromKey, fromInclusive, toKey, toInclusive), (k, v) -> v);
  }

  @Override
  public IteratorBatch<K> subMapIterateDescendingKeys(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return iterate(sessionId -> new DescendingIterator(sessionId, fromKey, fromInclusive, toKey, toInclusive), (k, v) -> k);
  }

  @Override
  public IteratorBatch<Map.Entry<K, Versioned<byte[]>>> subMapIterateDescendingEntries(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return iterate(sessionId -> new DescendingIterator(sessionId, fromKey, fromInclusive, toKey, toInclusive), Maps::immutableEntry);
  }

  @Override
  public void subMapClear(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    subMapAccept(NavigableMap::clear, fromKey, fromInclusive, toKey, toInclusive);
  }

  private void subMapAccept(Consumer<NavigableMap<K, MapEntryValue>> function, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    try {
      if (fromKey != null && toKey != null) {
        function.accept(entries().subMap(fromKey, fromInclusive, toKey, toInclusive));
      } else if (fromKey != null) {
        function.accept(entries().tailMap(fromKey, fromInclusive));
      } else if (toKey != null) {
        function.accept(entries().headMap(toKey, toInclusive));
      } else {
        function.accept(entries());
      }
    } catch (NoSuchElementException e) {
    }
  }

  private <T> T subMapApply(Function<NavigableMap<K, MapEntryValue>, T> function, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    try {
      if (fromKey != null && toKey != null) {
        return function.apply(entries().subMap(fromKey, fromInclusive, toKey, toInclusive));
      } else if (fromKey != null) {
        return function.apply(entries().tailMap(fromKey, fromInclusive));
      } else if (toKey != null) {
        return function.apply(entries().headMap(toKey, toInclusive));
      } else {
        return function.apply(entries());
      }
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  private Map.Entry<K, Versioned<byte[]>> toVersionedEntry(
      Map.Entry<K, MapEntryValue> entry) {
    return entry == null || valueIsNull(entry.getValue())
        ? null : Maps.immutableEntry(entry.getKey(), toVersioned(entry.getValue()));
  }

  protected class AscendingIterator extends IteratorContext {
    private final K fromKey;
    private final boolean fromInclusive;
    private final K toKey;
    private final boolean toInclusive;

    AscendingIterator(long sessionId, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(sessionId);
      this.fromKey = fromKey;
      this.fromInclusive = fromInclusive;
      this.toKey = toKey;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<Map.Entry<K, MapEntryValue>> create() {
      return subMapApply(m -> m.entrySet().iterator(), fromKey, fromInclusive, toKey, toInclusive);
    }
  }

  protected class DescendingIterator extends IteratorContext {
    private final K fromKey;
    private final boolean fromInclusive;
    private final K toKey;
    private final boolean toInclusive;

    DescendingIterator(long sessionId, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
      super(sessionId);
      this.fromKey = fromKey;
      this.fromInclusive = fromInclusive;
      this.toKey = toKey;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<Map.Entry<K, MapEntryValue>> create() {
      return subMapApply(m -> m.descendingMap().entrySet().iterator(), fromKey, fromInclusive, toKey, toInclusive);
    }
  }
}
