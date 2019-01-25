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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Sets;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.multimap.AtomicMultimapType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.misc.Match;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Base class for atomic multimap primitives.
 */
public abstract class AbstractAtomicMultimapService extends AbstractPrimitiveService<AtomicMultimapClient> implements AtomicMultimapService {

  private static final int MAX_ITERATOR_BATCH_SIZE = 1024 * 32;

  private final Serializer serializer = Serializer.using(Namespace.builder()
      .register(AtomicMultimapType.instance().namespace())
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
  private Map<String, MapEntryValues> backingMap = Maps.newConcurrentMap();
  protected Map<Long, IteratorContext> entryIterators = Maps.newHashMap();

  protected AbstractAtomicMultimapService(PrimitiveType primitiveType) {
    super(primitiveType, AtomicMultimapClient.class);
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
    entryIterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
  }

  @Override
  public void onClose(Session session) {
    listeners.remove(session.sessionId());
    entryIterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
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
  public boolean containsKeys(Collection<String> keys) {
    for (String key : keys) {
      if (!containsKey(key)) {
        return false;
      }
    }
    return true;
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
  public int keyCount() {
    return backingMap.keySet().size();
  }

  @Override
  public int entryCount() {
    return backingMap.entrySet().stream()
        .mapToInt(entry -> entry.getValue().values().size())
        .sum();
  }

  @Override
  public Versioned<Collection<byte[]>> get(String key) {
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
  public Versioned<Collection<byte[]>> removeAll(String key) {
    MapEntryValues entry = backingMap.get(key);
    if (entry == null) {
      return new Versioned<>(Collections.emptyList(), 0);
    }

    Versioned<Collection<byte[]>> removedValues = entry.removeAll(key);
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

    Versioned<Collection<byte[]>> removedValues = entry.removeAll(key, values);
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
      onChange(key, null, value);
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
  @SuppressWarnings("squid:S2175") // ByteArrayComparator passed into {@link Sets#newTreeSet(Comparator)}
  public Versioned<Collection<byte[]>> replaceValues(String key, Collection<byte[]> values) {
    MapEntryValues entry = backingMap.computeIfAbsent(key, k -> new NonTransactionalValues());

    Set<byte[]> oldValues = Sets.newTreeSet(new ByteArrayComparator());
    oldValues.addAll(entry.values());

    Set<byte[]> newValues = Sets.newTreeSet(new ByteArrayComparator());
    newValues.addAll(values);

    Versioned<Collection<byte[]>> removedValues = entry.replace(key, values);
    if (entry.values().isEmpty()) {
      backingMap.remove(key);
    }

    for (byte[] value : oldValues) {
      if (!newValues.contains(value)) {
        onChange(key, value, null);
      }
    }

    for (byte[] value : newValues) {
      if (!oldValues.contains(value)) {
        onChange(key, null, value);
      }
    }
    return removedValues;
  }

  @Override
  public IteratorBatch<String> iterateKeySet() {
    IteratorBatch<Map.Entry<String, byte[]>> batch = iterateEntries();
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet()), batch.complete());
  }

  @Override
  public IteratorBatch<String> nextKeySet(long iteratorId, int position) {
    IteratorBatch<Map.Entry<String, byte[]>> batch = nextEntries(iteratorId, position);
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet()), batch.complete());
  }

  @Override
  public void closeKeySet(long iteratorId) {
    closeEntries(iteratorId);
  }

  @Override
  public IteratorBatch<String> iterateKeys() {
    IteratorBatch<Map.Entry<String, byte[]>> batch = iterateEntries();
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()), batch.complete());
  }

  @Override
  public IteratorBatch<String> nextKeys(long iteratorId, int position) {
    IteratorBatch<Map.Entry<String, byte[]>> batch = nextEntries(iteratorId, position);
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()), batch.complete());
  }

  @Override
  public void closeKeys(long iteratorId) {
    closeEntries(iteratorId);
  }

  @Override
  public IteratorBatch<byte[]> iterateValues() {
    IteratorBatch<Map.Entry<String, byte[]>> batch = iterateEntries();
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet()), batch.complete());
  }

  @Override
  public IteratorBatch<byte[]> nextValues(long iteratorId, int position) {
    IteratorBatch<Map.Entry<String, byte[]>> batch = nextEntries(iteratorId, position);
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries().stream()
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet()), batch.complete());
  }

  @Override
  public void closeValues(long iteratorId) {
    closeEntries(iteratorId);
  }

  @Override
  public IteratorBatch<Multiset.Entry<byte[]>> iterateValuesSet() {
    IteratorContext iterator = new IteratorContext(getCurrentSession().sessionId().id());
    if (!iterator.iterator.hasNext()) {
      return null;
    }

    long iteratorId = getCurrentIndex();
    entryIterators.put(iteratorId, iterator);
    IteratorBatch<Multiset.Entry<byte[]>> batch = nextValuesSet(iteratorId, 0);
    if (batch.complete()) {
      entryIterators.remove(iteratorId);
    }
    return batch;
  }

  @Override
  public IteratorBatch<Multiset.Entry<byte[]>> nextValuesSet(long iteratorId, int position) {
    IteratorContext context = entryIterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<Multiset.Entry<byte[]>> entries = new ArrayList<>();
    int size = 0;
    while (context.iterator.hasNext()) {
      context.position++;
      if (context.position > position) {
        Map.Entry<String, MapEntryValues> entry = context.iterator.next();
        if (!entry.getValue().values().isEmpty()) {
          byte[] value = entry.getValue().values().iterator().next();
          int count = entry.getValue().values().size();
          size += value.length + 4;
          entries.add(Multisets.immutableEntry(value, count));
        }

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (entries.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(iteratorId, context.position, entries, !context.iterator.hasNext());
  }

  @Override
  public void closeValuesSet(long iteratorId) {
    closeEntries(iteratorId);
  }

  @Override
  public IteratorBatch<Map.Entry<String, byte[]>> iterateEntries() {
    IteratorContext iterator = new IteratorContext(getCurrentSession().sessionId().id());
    if (!iterator.iterator.hasNext()) {
      return null;
    }

    long iteratorId = getCurrentIndex();
    entryIterators.put(iteratorId, iterator);
    IteratorBatch<Map.Entry<String, byte[]>> batch = nextEntries(iteratorId, 0);
    if (batch.complete()) {
      entryIterators.remove(iteratorId);
    }
    return batch;
  }

  @Override
  public IteratorBatch<Map.Entry<String, byte[]>> nextEntries(long iteratorId, int position) {
    IteratorContext context = entryIterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<Map.Entry<String, byte[]>> entries = new ArrayList<>();
    int size = 0;
    while (context.iterator.hasNext()) {
      context.position++;
      if (context.position > position) {
        Map.Entry<String, MapEntryValues> entry = context.iterator.next();
        String key = entry.getKey();
        int keySize = key.length();
        for (byte[] value : entry.getValue().values()) {
          entries.add(Maps.immutableEntry(key, value));
          size += keySize;
          size += value.length;
        }

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (entries.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(iteratorId, context.position, entries, !context.iterator.hasNext());
  }

  @Override
  public void closeEntries(long iteratorId) {
    entryIterators.remove(iteratorId);
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
    listeners.forEach(id -> getSession(id).accept(client -> client.onChange(key, oldValue, newValue)));
  }

  private interface MapEntryValues {

    /**
     * Returns the list of raw {@code byte[]'s}.
     *
     * @return list of raw values
     */
    Collection<byte[]> values();

    /**
     * Returns the version of the value.
     *
     * @return version
     */
    long version();

    boolean put(String key, byte[] value);

    Collection<? extends byte[]> putAll(String key, Collection<? extends byte[]> values);

    Versioned<Collection<byte[]>> replace(String key, Collection<? extends byte[]> values);

    boolean remove(String key, byte[] value);

    Versioned<Collection<byte[]>> removeAll(String key);

    Versioned<Collection<byte[]>> removeAll(String key, Collection<? extends byte[]> values);
  }

  private class NonTransactionalValues implements MapEntryValues {
    private long version;
    private final TreeSet<byte[]> valueSet = Sets.newTreeSet(new ByteArrayComparator());

    NonTransactionalValues() {
      //Set the version to current it will only be updated once this is
      // populated
      this.version = globalVersion.get();
    }

    @Override
    public Collection<byte[]> values() {
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
    public Versioned<Collection<byte[]>> replace(String key, Collection<? extends byte[]> values) {
      Versioned<Collection<byte[]>> removedValues = new Versioned<>(Sets.newHashSet(valueSet), version);
      valueSet.clear();
      valueSet.addAll(values);
      version = getCurrentIndex();
      return removedValues;
    }

    @Override
    @SuppressWarnings("squid:S2175") // ByteArrayComparator passed into {@link Sets#newTreeSet(Comparator)}
    public boolean remove(String key, byte[] value) {
      if (valueSet.remove(value)) {
        version = getCurrentIndex();
        return true;
      }
      return false;
    }

    @Override
    public Versioned<Collection<byte[]>> removeAll(String key) {
      Set<byte[]> removedValues = Sets.newHashSet(valueSet);
      valueSet.clear();
      return new Versioned<>(removedValues, version = getCurrentIndex());
    }

    @Override
    @SuppressWarnings("squid:S2175") // ByteArrayComparator passed into {@link Sets#newTreeSet(Comparator)}
    public Versioned<Collection<byte[]>> removeAll(String key, Collection<? extends byte[]> values) {
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
   * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
   *
   * @param value map entry value
   * @return versioned instance or an empty list versioned -1 if argument is
   * null
   */
  private Versioned<Collection<byte[]>> toVersioned(
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

  private class IteratorContext {
    private final long sessionId;
    private int position = 0;
    private transient Iterator<Map.Entry<String, MapEntryValues>> iterator = backingMap.entrySet().iterator();

    IteratorContext(long sessionId) {
      this.sessionId = sessionId;
    }
  }
}
