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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.atomix.core.multimap.ConsistentMultimapType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.misc.Match;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * State Machine for {@link ConsistentSetMultimapProxy} resource.
 */
public class DefaultConsistentSetMultimapService extends AbstractPrimitiveService<ConsistentSetMultimapClient> implements ConsistentSetMultimapService {

  private final Serializer serializer = Serializer.using(KryoNamespace.builder()
      .register((KryoNamespace) ConsistentMultimapType.instance().namespace())
      .register(SessionId.class)
      .register(ByteArrayComparator.class)
      .register(new HashMap().keySet().getClass())
      .register(TreeSet.class)
      .register(new com.esotericsoftware.kryo.Serializer<NonTransactionalValues>() {
        @Override
        public void write(Kryo kryo, Output output, NonTransactionalValues object) {
          kryo.writeClassAndObject(output, object.valueSet);
        }

        @Override
        @SuppressWarnings("unchecked")
        public NonTransactionalValues read(Kryo kryo, Input input, Class<NonTransactionalValues> type) {
          NonTransactionalValues commit = new NonTransactionalValues();
          commit.valueSet.addAll((Collection<byte[]>) kryo.readClassAndObject(input));
          return commit;
        }
      }, NonTransactionalValues.class)
      .build());

  private AtomicLong globalVersion = new AtomicLong(1);
  private Set<SessionId> listeners = new LinkedHashSet<>();
  private Map<String, MapEntryValues> backingMap = Maps.newHashMap();

  public DefaultConsistentSetMultimapService() {
    super(ConsistentMultimapType.instance(), ConsistentSetMultimapClient.class);
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(globalVersion.get());
    writer.writeObject(listeners);
    writer.writeObject(backingMap);
  }

  @Override
  public void restore(BackupInput reader) {
    globalVersion = new AtomicLong(reader.readLong());
    listeners = reader.readObject();
    backingMap = reader.readObject();
  }

  @Override
  public void onExpire(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  public void onClose(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  public int size() {
    return backingMap.values()
        .stream()
        .mapToInt(valueCollection -> valueCollection.values().size())
        .sum();
  }

  @Override
  public boolean isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public boolean containsKey(String key) {
    return backingMap.containsKey(key);
  }

  @Override
  public boolean containsValue(byte[] value) {
    if (backingMap.values().isEmpty()) {
      return false;
    }
    Match<byte[]> match = Match.ifValue(value);
    return backingMap
        .values()
        .stream()
        .anyMatch(valueList ->
            valueList
                .values()
                .stream()
                .anyMatch(byteValue ->
                    match.matches(byteValue)));
  }

  @Override
  public boolean containsEntry(String key, byte[] value) {
    MapEntryValues entryValue =
        backingMap.get(key);
    if (entryValue == null) {
      return false;
    } else {
      Match valueMatch = Match.ifValue(value);
      return entryValue
          .values()
          .stream()
          .anyMatch(byteValue -> valueMatch.matches(byteValue));
    }
  }

  @Override
  public void clear() {
    backingMap.clear();
  }

  @Override
  public Set<String> keySet() {
    return ImmutableSet.copyOf(backingMap.keySet());
  }

  @Override
  public Multiset<String> keys() {
    Multiset keys = HashMultiset.create();
    backingMap.forEach((key, mapEntryValues) -> {
      keys.add(key, mapEntryValues.values().size());
    });
    return keys;
  }

  @Override
  public Multiset<byte[]> values() {
    return backingMap
        .values()
        .stream()
        .collect(new HashMultisetValueCollector());
  }

  @Override
  public Collection<Map.Entry<String, byte[]>> entries() {
    return backingMap
        .entrySet()
        .stream()
        .collect(new EntrySetCollector());
  }

  @Override
  public Versioned<Collection<? extends byte[]>> get(String key) {
    return toVersioned(backingMap.get(key));
  }

  @Override
  public boolean remove(String key, byte[] value) {
    MapEntryValues entry = backingMap.get(key);
    if (entry == null) {
      return false;
    }

    if (entry.remove(key, value)) {
      if (entry.values().isEmpty()) {
        backingMap.remove(key);
      }
      onChange(key, value, null);
      return true;
    }
    return false;
  }

  @Override
  public Versioned<Collection<? extends byte[]>> removeAll(String key) {
    MapEntryValues entry = backingMap.get(key);
    if (entry == null) {
      return new Versioned<>(Collections.emptyList(), 0);
    }

    Versioned<Collection<? extends byte[]>> removedValues = entry.removeAll(key);
    backingMap.remove(key);
    removedValues.value().forEach(value -> onChange(key, value, null));
    return removedValues;
  }

  @Override
  public boolean removeAll(String key, Collection<? extends byte[]> values) {
    MapEntryValues entry = backingMap.get(key);
    if (entry == null) {
      return false;
    }

    Versioned<Collection<? extends byte[]>> removedValues = entry.removeAll(key, values);
    if (removedValues != null) {
      if (entry.values().isEmpty()) {
        backingMap.remove(key);
      }
      removedValues.value().forEach(value -> onChange(key, value, null));
      return true;
    }
    return false;
  }

  @Override
  public boolean put(String key, byte[] value) {
    if (backingMap.computeIfAbsent(key, k -> new NonTransactionalValues()).put(key, value)) {
      onChange(key, value, null);
      return true;
    }
    return false;
  }

  @Override
  public boolean putAll(String key, Collection<? extends byte[]> values) {
    if (values.isEmpty()) {
      return false;
    }

    Collection<? extends byte[]> addedValues = backingMap.computeIfAbsent(key, k -> new NonTransactionalValues()).putAll(key, values);
    if (addedValues != null) {
      addedValues.forEach(value -> onChange(key, value, null));
      return true;
    }
    return false;
  }

  @Override
  public Versioned<Collection<? extends byte[]>> replaceValues(String key, Collection<byte[]> values) {
    MapEntryValues entry = backingMap.computeIfAbsent(key, k -> new NonTransactionalValues());

    Collection<? extends byte[]> oldValues = entry.values();
    Versioned<Collection<? extends byte[]>> removedValues = entry.replace(key, values);
    if (entry.values().isEmpty()) {
      backingMap.remove(key);
    }

    for (byte[] value : values) {
      if (!oldValues.contains(value)) {
        onChange(key, null, value);
      }
    }

    for (byte[] value : oldValues) {
      if (!values.contains(value)) {
        onChange(key, value, null);
      }
    }
    return removedValues;
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  /**
   * Sends a change event to listeners.
   *
   * @param key      the changed key
   * @param oldValue the old value
   * @param newValue the new value
   */
  private void onChange(String key, byte[] oldValue, byte[] newValue) {
    listeners.forEach(id -> acceptOn(id, client -> client.onChange(key, oldValue, newValue)));
  }

  private interface MapEntryValues {

    /**
     * Returns the list of raw {@code byte[]'s}.
     *
     * @return list of raw values
     */
    Collection<? extends byte[]> values();

    /**
     * Returns the version of the value.
     *
     * @return version
     */
    long version();

    boolean put(String key, byte[] value);

    Collection<? extends byte[]> putAll(String key, Collection<? extends byte[]> values);

    Versioned<Collection<? extends byte[]>> replace(String key, Collection<? extends byte[]> values);

    boolean remove(String key, byte[] value);

    Versioned<Collection<? extends byte[]>> removeAll(String key);

    Versioned<Collection<? extends byte[]>> removeAll(String key, Collection<? extends byte[]> values);
  }

  private class NonTransactionalValues implements MapEntryValues {
    private long version;
    private final TreeSet<byte[]> valueSet = Sets.newTreeSet(new ByteArrayComparator());

    public NonTransactionalValues() {
      //Set the version to current it will only be updated once this is
      // populated
      this.version = globalVersion.get();
    }

    @Override
    public Collection<? extends byte[]> values() {
      return ImmutableSet.copyOf(valueSet);
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public boolean put(String key, byte[] value) {
      if (valueSet.add(value)) {
        version = getCurrentIndex();
        return true;
      }
      return false;
    }

    @Override
    public Collection<? extends byte[]> putAll(String key, Collection<? extends byte[]> values) {
      Set<byte[]> addedValues = Sets.newHashSet();
      for (byte[] value : values) {
        if (valueSet.add(value)) {
          addedValues.add(value);
        }
      }

      if (!addedValues.isEmpty()) {
        version = getCurrentIndex();
        return addedValues;
      }
      return null;
    }

    @Override
    public Versioned<Collection<? extends byte[]>> replace(String key, Collection<? extends byte[]> values) {
      Versioned<Collection<? extends byte[]>> removedValues = new Versioned<>(Sets.newHashSet(valueSet), version);
      valueSet.clear();
      valueSet.addAll(values);
      version = getCurrentIndex();
      return removedValues;
    }

    @Override
    public boolean remove(String key, byte[] value) {
      if (valueSet.remove(value)) {
        version = getCurrentIndex();
        return true;
      }
      return false;
    }

    @Override
    public Versioned<Collection<? extends byte[]>> removeAll(String key) {
      Set<byte[]> removedValues = Sets.newHashSet(valueSet);
      valueSet.clear();
      return new Versioned<>(removedValues, version = getCurrentIndex());
    }

    @Override
    public Versioned<Collection<? extends byte[]>> removeAll(String key, Collection<? extends byte[]> values) {
      Set<byte[]> removedValues = Sets.newHashSet();
      for (byte[] value : values) {
        if (valueSet.remove(value)) {
          removedValues.add(value);
        }
      }

      if (!removedValues.isEmpty()) {
        return new Versioned<>(removedValues, version = getCurrentIndex());
      }
      return null;
    }
  }

  /**
   * A collector that creates MapEntryValues and creates a multiset of all
   * values in the map an equal number of times to the number of sets in
   * which they participate.
   */
  private static class HashMultisetValueCollector implements
      Collector<MapEntryValues,
          HashMultiset<byte[]>,
          HashMultiset<byte[]>> {

    @Override
    public Supplier<HashMultiset<byte[]>> supplier() {
      return HashMultiset::create;
    }

    @Override
    public BiConsumer<HashMultiset<byte[]>, MapEntryValues> accumulator() {
      return (multiset, mapEntryValues) ->
          multiset.addAll(mapEntryValues.values());
    }

    @Override
    public BinaryOperator<HashMultiset<byte[]>> combiner() {
      return (setOne, setTwo) -> {
        setOne.addAll(setTwo);
        return setOne;
      };
    }

    @Override
    public Function<HashMultiset<byte[]>,
        HashMultiset<byte[]>> finisher() {
      return Function.identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }

  /**
   * A collector that creates Entries of {@code <String, MapEntryValue>} and
   * creates a set of entries all key value pairs in the map.
   */
  private static class EntrySetCollector implements
      Collector<Map.Entry<String, MapEntryValues>,
          Set<Map.Entry<String, byte[]>>,
          Set<Map.Entry<String, byte[]>>> {
    private Set<Map.Entry<String, byte[]>> set = null;

    @Override
    public Supplier<Set<Map.Entry<String, byte[]>>> supplier() {
      return () -> {
        if (set == null) {
          set = Sets.newHashSet();
        }
        return set;
      };
    }

    @Override
    public BiConsumer<Set<Map.Entry<String, byte[]>>,
        Map.Entry<String, MapEntryValues>> accumulator() {
      return (set, entry) -> {
        entry
            .getValue()
            .values()
            .forEach(byteValue ->
                set.add(Maps.immutableEntry(entry.getKey(),
                    byteValue)));
      };
    }

    @Override
    public BinaryOperator<Set<Map.Entry<String, byte[]>>> combiner() {
      return (setOne, setTwo) -> {
        setOne.addAll(setTwo);
        return setOne;
      };
    }

    @Override
    public Function<Set<Map.Entry<String, byte[]>>,
        Set<Map.Entry<String, byte[]>>> finisher() {
      return (unused) -> set;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return EnumSet.of(Characteristics.UNORDERED);
    }
  }

  /**
   * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
   *
   * @param value map entry value
   * @return versioned instance or an empty list versioned -1 if argument is
   * null
   */
  private Versioned<Collection<? extends byte[]>> toVersioned(
      MapEntryValues value) {
    return value == null
        ? new Versioned<>(Collections.emptyList(), -1)
        : new Versioned<>(value.values(), value.version());
  }

  private static class ByteArrayComparator implements Comparator<byte[]>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(byte[] o1, byte[] o2) {
      if (Arrays.equals(o1, o2)) {
        return 0;
      } else {
        for (int i = 0; i < o1.length && i < o2.length; i++) {
          if (o1[i] < o2[i]) {
            return -1;
          } else if (o1[i] > o2[i]) {
            return 1;
          }
        }
        return o1.length > o2.length ? 1 : -1;
      }
    }
  }
}