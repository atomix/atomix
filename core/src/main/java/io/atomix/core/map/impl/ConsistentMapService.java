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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.impl.ConsistentMapOperations.ContainsKey;
import io.atomix.core.map.impl.ConsistentMapOperations.ContainsValue;
import io.atomix.core.map.impl.ConsistentMapOperations.Get;
import io.atomix.core.map.impl.ConsistentMapOperations.GetAllPresent;
import io.atomix.core.map.impl.ConsistentMapOperations.GetOrDefault;
import io.atomix.core.map.impl.ConsistentMapOperations.Put;
import io.atomix.core.map.impl.ConsistentMapOperations.Remove;
import io.atomix.core.map.impl.ConsistentMapOperations.RemoveValue;
import io.atomix.core.map.impl.ConsistentMapOperations.RemoveVersion;
import io.atomix.core.map.impl.ConsistentMapOperations.Replace;
import io.atomix.core.map.impl.ConsistentMapOperations.ReplaceValue;
import io.atomix.core.map.impl.ConsistentMapOperations.ReplaceVersion;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionBegin;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionCommit;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionPrepare;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionPrepareAndCommit;
import io.atomix.core.map.impl.ConsistentMapOperations.TransactionRollback;
import io.atomix.core.map.impl.MapUpdate.Type;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.atomix.core.map.impl.ConsistentMapEvents.CHANGE;
import static io.atomix.core.map.impl.ConsistentMapOperations.ADD_LISTENER;
import static io.atomix.core.map.impl.ConsistentMapOperations.BEGIN;
import static io.atomix.core.map.impl.ConsistentMapOperations.CLEAR;
import static io.atomix.core.map.impl.ConsistentMapOperations.COMMIT;
import static io.atomix.core.map.impl.ConsistentMapOperations.CONTAINS_KEY;
import static io.atomix.core.map.impl.ConsistentMapOperations.CONTAINS_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.ENTRY_SET;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET_ALL_PRESENT;
import static io.atomix.core.map.impl.ConsistentMapOperations.GET_OR_DEFAULT;
import static io.atomix.core.map.impl.ConsistentMapOperations.IS_EMPTY;
import static io.atomix.core.map.impl.ConsistentMapOperations.KEY_SET;
import static io.atomix.core.map.impl.ConsistentMapOperations.PREPARE;
import static io.atomix.core.map.impl.ConsistentMapOperations.PREPARE_AND_COMMIT;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT_AND_GET;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT_IF_ABSENT;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_LISTENER;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REMOVE_VERSION;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE_VALUE;
import static io.atomix.core.map.impl.ConsistentMapOperations.REPLACE_VERSION;
import static io.atomix.core.map.impl.ConsistentMapOperations.ROLLBACK;
import static io.atomix.core.map.impl.ConsistentMapOperations.SIZE;
import static io.atomix.core.map.impl.ConsistentMapOperations.VALUES;

/**
 * State Machine for {@link ConsistentMapProxy} resource.
 */
public class ConsistentMapService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(ConsistentMapOperations.NAMESPACE)
      .register(ConsistentMapEvents.NAMESPACE)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
      .register(TransactionScope.class)
      .register(TransactionLog.class)
      .register(TransactionId.class)
      .register(MapEntryValue.class)
      .register(MapEntryValue.Type.class)
      .register(new HashMap().keySet().getClass())
      .build());

  protected Map<Long, Session> listeners = new LinkedHashMap<>();
  private Map<String, MapEntryValue> map;
  protected Set<String> preparedKeys = Sets.newHashSet();
  protected Map<TransactionId, TransactionScope> activeTransactions = Maps.newHashMap();
  protected long currentVersion;

  public ConsistentMapService() {
    map = createMap();
  }

  protected Map<String, MapEntryValue> createMap() {
    return Maps.newHashMap();
  }

  protected Map<String, MapEntryValue> entries() {
    return map;
  }

  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BufferOutput<?> writer) {
    writer.writeObject(Sets.newHashSet(listeners.keySet()), serializer()::encode);
    writer.writeObject(preparedKeys, serializer()::encode);
    writer.writeObject(entries(), serializer()::encode);
    writer.writeObject(activeTransactions, serializer()::encode);
    writer.writeLong(currentVersion);
  }

  @Override
  public void restore(BufferInput<?> reader) {
    listeners = new LinkedHashMap<>();
    for (Long sessionId : reader.<Set<Long>>readObject(serializer()::decode)) {
      listeners.put(sessionId, getSessions().getSession(sessionId));
    }
    preparedKeys = reader.readObject(serializer()::decode);
    map = reader.readObject(serializer()::decode);
    activeTransactions = reader.readObject(serializer()::decode);
    currentVersion = reader.readLong();
    map.forEach((key, value) -> {
      if (value.ttl() > 0) {
        value.timer = getScheduler().schedule(Duration.ofMillis(value.ttl() - (getWallClock().getTime().unixTimestamp() - value.created())), () -> {
          entries().remove(key, value);
          publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, toVersioned(value)));
        });
      }
    });
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    // Listeners
    executor.register(ADD_LISTENER, (Commit<Void> c) -> listen(c.session()));
    executor.register(REMOVE_LISTENER, (Commit<Void> c) -> unlisten(c.session()));
    // Queries
    executor.register(CONTAINS_KEY, serializer()::decode, this::containsKey, serializer()::encode);
    executor.register(CONTAINS_VALUE, serializer()::decode, this::containsValue, serializer()::encode);
    executor.register(ENTRY_SET, (Commit<Void> c) -> entrySet(), serializer()::encode);
    executor.register(GET, serializer()::decode, this::get, serializer()::encode);
    executor.register(GET_ALL_PRESENT, serializer()::decode, this::getAllPresent, serializer()::encode);
    executor.register(GET_OR_DEFAULT, serializer()::decode, this::getOrDefault, serializer()::encode);
    executor.register(IS_EMPTY, (Commit<Void> c) -> isEmpty(), serializer()::encode);
    executor.register(KEY_SET, (Commit<Void> c) -> keySet(), serializer()::encode);
    executor.register(SIZE, (Commit<Void> c) -> size(), serializer()::encode);
    executor.register(VALUES, (Commit<Void> c) -> values(), serializer()::encode);
    // Commands
    executor.register(PUT, serializer()::decode, this::put, serializer()::encode);
    executor.register(PUT_IF_ABSENT, serializer()::decode, this::putIfAbsent, serializer()::encode);
    executor.register(PUT_AND_GET, serializer()::decode, this::putAndGet, serializer()::encode);
    executor.register(REMOVE, serializer()::decode, this::remove, serializer()::encode);
    executor.register(REMOVE_VALUE, serializer()::decode, this::removeValue, serializer()::encode);
    executor.register(REMOVE_VERSION, serializer()::decode, this::removeVersion, serializer()::encode);
    executor.register(REPLACE, serializer()::decode, this::replace, serializer()::encode);
    executor.register(REPLACE_VALUE, serializer()::decode, this::replaceValue, serializer()::encode);
    executor.register(REPLACE_VERSION, serializer()::decode, this::replaceVersion, serializer()::encode);
    executor.register(CLEAR, (Commit<Void> c) -> clear(), serializer()::encode);
    executor.register(BEGIN, serializer()::decode, this::begin, serializer()::encode);
    executor.register(PREPARE, serializer()::decode, this::prepare, serializer()::encode);
    executor.register(PREPARE_AND_COMMIT, serializer()::decode, this::prepareAndCommit, serializer()::encode);
    executor.register(COMMIT, serializer()::decode, this::commit, serializer()::encode);
    executor.register(ROLLBACK, serializer()::decode, this::rollback, serializer()::encode);
  }

  /**
   * Handles a contains key commit.
   *
   * @param commit containsKey commit
   * @return {@code true} if map contains key
   */
  protected boolean containsKey(Commit<? extends ContainsKey> commit) {
    MapEntryValue value = entries().get(commit.value().key());
    return value != null && value.type() != MapEntryValue.Type.TOMBSTONE;
  }

  /**
   * Handles a contains value commit.
   *
   * @param commit containsValue commit
   * @return {@code true} if map contains value
   */
  protected boolean containsValue(Commit<? extends ContainsValue> commit) {
    return entries().values().stream()
        .filter(value -> value.type() != MapEntryValue.Type.TOMBSTONE)
        .anyMatch(value -> Arrays.equals(value.value, commit.value().value()));
  }

  /**
   * Handles a get commit.
   *
   * @param commit get commit
   * @return value mapped to key
   */
  protected Versioned<byte[]> get(Commit<? extends Get> commit) {
    return toVersioned(entries().get(commit.value().key()));
  }

  /**
   * Handles a get all present commit.
   *
   * @param commit get all present commit
   * @return keys present in map
   */
  protected Map<String, Versioned<byte[]>> getAllPresent(Commit<? extends GetAllPresent> commit) {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE
            && commit.value().keys().contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, o -> toVersioned(o.getValue())));
  }

  /**
   * Handles a get or default commit.
   *
   * @param commit get or default commit
   * @return value mapped to key
   */
  protected Versioned<byte[]> getOrDefault(Commit<? extends GetOrDefault> commit) {
    MapEntryValue value = entries().get(commit.value().key());
    if (value == null) {
      return new Versioned<>(commit.value().defaultValue(), 0);
    } else if (value.type() == MapEntryValue.Type.TOMBSTONE) {
      return new Versioned<>(commit.value().defaultValue(), value.version);
    } else {
      return new Versioned<>(value.value(), value.version);
    }
  }

  /**
   * Handles a size commit.
   *
   * @return number of entries in map
   */
  protected int size() {
    return (int) entries().values().stream()
        .filter(value -> value.type() != MapEntryValue.Type.TOMBSTONE)
        .count();
  }

  /**
   * Handles an is empty commit.
   *
   * @return {@code true} if map is empty
   */
  protected boolean isEmpty() {
    return entries().values().stream()
        .noneMatch(value -> value.type() != MapEntryValue.Type.TOMBSTONE);
  }

  /**
   * Handles a keySet commit.
   *
   * @return set of keys in map
   */
  protected Set<String> keySet() {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Handles a values commit.
   *
   * @return collection of values in map
   */
  protected Collection<Versioned<byte[]>> values() {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
        .map(entry -> toVersioned(entry.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Handles a entry set commit.
   *
   * @return set of map entries
   */
  protected Set<Map.Entry<String, Versioned<byte[]>>> entrySet() {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
        .map(e -> Maps.immutableEntry(e.getKey(), toVersioned(e.getValue())))
        .collect(Collectors.toSet());
  }

  /**
   * Returns a boolean indicating whether the given MapEntryValues are equal.
   *
   * @param oldValue the first value to compare
   * @param newValue the second value to compare
   * @return indicates whether the two values are equal
   */
  protected boolean valuesEqual(MapEntryValue oldValue, MapEntryValue newValue) {
    return (oldValue == null && newValue == null)
        || (oldValue != null && newValue != null && valuesEqual(oldValue.value(), newValue.value()));
  }

  /**
   * Returns a boolean indicating whether the given entry values are equal.
   *
   * @param oldValue the first value to compare
   * @param newValue the second value to compare
   * @return indicates whether the two values are equal
   */
  protected boolean valuesEqual(byte[] oldValue, byte[] newValue) {
    return (oldValue == null && newValue == null)
        || (oldValue != null && newValue != null && Arrays.equals(oldValue, newValue));
  }

  /**
   * Returns a boolean indicating whether the given MapEntryValue is null or a tombstone.
   *
   * @param value the value to check
   * @return indicates whether the given value is null or is a tombstone
   */
  protected boolean valueIsNull(MapEntryValue value) {
    return value == null || value.type() == MapEntryValue.Type.TOMBSTONE;
  }

  /**
   * Updates the given value.
   *
   * @param key the key to update
   * @param value the value to update
   */
  protected void putValue(String key, MapEntryValue value) {
    MapEntryValue oldValue = entries().put(key, value);
    cancelTtl(oldValue);
    scheduleTtl(key, value);
  }

  /**
   * Schedules the TTL for the given value.
   *
   * @param value the value for which to schedule the TTL
   */
  protected void scheduleTtl(String key, MapEntryValue value) {
    if (value.ttl() > 0) {
      value.timer = getScheduler().schedule(Duration.ofMillis(value.ttl()), () -> {
        entries().remove(key, value);
        publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, toVersioned(value)));
      });
    }
  }

  /**
   * Cancels the TTL for the given value.
   *
   * @param value the value for which to cancel the TTL
   */
  protected void cancelTtl(MapEntryValue value) {
    if (value != null && value.timer != null) {
      value.timer.cancel();
    }
  }

  /**
   * Handles a put commit.
   *
   * @param commit put commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> put(Commit<? extends Put> commit) {
    String key = commit.value().key();
    MapEntryValue oldValue = entries().get(key);
    MapEntryValue newValue = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        commit.index(),
        commit.value().value(),
        commit.wallClockTime().unixTimestamp(),
        commit.value().ttl());

    // If the value is null or a tombstone, this is an insert.
    // Otherwise, only update the value if it has changed to reduce the number of events.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            commit.index(),
            key,
            toVersioned(oldValue));
      }
      putValue(commit.value().key(), newValue);
      Versioned<byte[]> result = toVersioned(oldValue);
      publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, toVersioned(newValue), result));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
    } else if (!valuesEqual(oldValue, newValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            commit.index(),
            key,
            toVersioned(oldValue));
      }
      putValue(commit.value().key(), newValue);
      Versioned<byte[]> result = toVersioned(oldValue);
      publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, toVersioned(newValue), result));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
    }
    // If the value hasn't changed, return a NOOP result.
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, commit.index(), key, toVersioned(oldValue));
  }

  /**
   * Handles a putIfAbsent commit.
   *
   * @param commit putIfAbsent commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> putIfAbsent(Commit<? extends Put> commit) {
    String key = commit.value().key();
    MapEntryValue oldValue = entries().get(key);

    // If the value is null, this is an INSERT.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            commit.index(),
            key,
            toVersioned(oldValue));
      }
      MapEntryValue newValue = new MapEntryValue(
          MapEntryValue.Type.VALUE,
          commit.index(),
          commit.value().value(),
          commit.wallClockTime().unixTimestamp(),
          commit.value().ttl());
      putValue(commit.value().key(), newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, result, null));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, null);
    }
    return new MapEntryUpdateResult<>(
        MapEntryUpdateResult.Status.PRECONDITION_FAILED,
        commit.index(),
        key,
        toVersioned(oldValue));
  }

  /**
   * Handles a putAndGet commit.
   *
   * @param commit putAndGet commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> putAndGet(Commit<? extends Put> commit) {
    String key = commit.value().key();
    MapEntryValue oldValue = entries().get(key);
    MapEntryValue newValue = new MapEntryValue(MapEntryValue.Type.VALUE, commit.index(), commit.value().value(), commit.wallClockTime().unixTimestamp(), commit.value().ttl());

    // If the value is null or a tombstone, this is an insert.
    // Otherwise, only update the value if it has changed to reduce the number of events.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            commit.index(),
            key,
            toVersioned(oldValue));
      }
      putValue(commit.value().key(), newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, result, null));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
    } else if (!valuesEqual(oldValue, newValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            commit.index(),
            key,
            toVersioned(oldValue));
      }
      putValue(commit.value().key(), newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, result, toVersioned(oldValue)));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
    }
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, commit.index(), key, toVersioned(oldValue));
  }

  /**
   * Handles a remove commit.
   *
   * @param index     the commit index
   * @param key       the key to remove
   * @param predicate predicate to determine whether to remove the entry
   * @return map entry update result
   */
  private MapEntryUpdateResult<String, byte[]> removeIf(long index, String key, Predicate<MapEntryValue> predicate) {
    MapEntryValue value = entries().get(key);

    // If the value does not exist or doesn't match the predicate, return a PRECONDITION_FAILED error.
    if (valueIsNull(value) || !predicate.test(value)) {
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.PRECONDITION_FAILED, index, key, null);
    }

    // If the key has been locked by a transaction, return a WRITE_LOCK error.
    if (preparedKeys.contains(key)) {
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.WRITE_LOCK, index, key, null);
    }

    // If no transactions are active, remove the key. Otherwise, replace it with a tombstone.
    if (activeTransactions.isEmpty()) {
      entries().remove(key);
    } else {
      entries().put(key, new MapEntryValue(MapEntryValue.Type.TOMBSTONE, index, null, 0, 0));
    }

    // Cancel the timer if one is scheduled.
    cancelTtl(value);

    Versioned<byte[]> result = toVersioned(value);
    publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, result));
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
  }

  /**
   * Handles a remove commit.
   *
   * @param commit remove commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> remove(Commit<? extends Remove> commit) {
    return removeIf(commit.index(), commit.value().key(), v -> true);
  }

  /**
   * Handles a removeValue commit.
   *
   * @param commit removeValue commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> removeValue(Commit<? extends RemoveValue> commit) {
    return removeIf(commit.index(), commit.value().key(), v ->
        valuesEqual(v, new MapEntryValue(MapEntryValue.Type.VALUE, commit.index(), commit.value().value(), commit.wallClockTime().unixTimestamp(), 0)));
  }

  /**
   * Handles a removeVersion commit.
   *
   * @param commit removeVersion commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> removeVersion(Commit<? extends RemoveVersion> commit) {
    return removeIf(commit.index(), commit.value().key(), v -> v.version() == commit.value().version());
  }

  /**
   * Handles a replace commit.
   *
   * @param index     the commit index
   * @param key       the key to replace
   * @param newValue  the value with which to replace the key
   * @param predicate a predicate to determine whether to replace the key
   * @return map entry update result
   */
  private MapEntryUpdateResult<String, byte[]> replaceIf(
      long index, String key, MapEntryValue newValue, Predicate<MapEntryValue> predicate) {
    MapEntryValue oldValue = entries().get(key);

    // If the key is not set or the current value doesn't match the predicate, return a PRECONDITION_FAILED error.
    if (valueIsNull(oldValue) || !predicate.test(oldValue)) {
      return new MapEntryUpdateResult<>(
          MapEntryUpdateResult.Status.PRECONDITION_FAILED,
          index,
          key,
          toVersioned(oldValue));
    }

    // If the key has been locked by a transaction, return a WRITE_LOCK error.
    if (preparedKeys.contains(key)) {
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.WRITE_LOCK, index, key, null);
    }

    putValue(key, newValue);
    Versioned<byte[]> result = toVersioned(oldValue);
    publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, toVersioned(newValue), result));
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
  }

  /**
   * Handles a replace commit.
   *
   * @param commit replace commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> replace(Commit<? extends Replace> commit) {
    MapEntryValue value = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        commit.index(),
        commit.value().value(),
        commit.wallClockTime().unixTimestamp(),
        0);
    return replaceIf(commit.index(), commit.value().key(), value, v -> true);
  }

  /**
   * Handles a replaceValue commit.
   *
   * @param commit replaceValue commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> replaceValue(Commit<? extends ReplaceValue> commit) {
    MapEntryValue value = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        commit.index(),
        commit.value().newValue(),
        commit.wallClockTime().unixTimestamp(),
        0);
    return replaceIf(commit.index(), commit.value().key(), value,
        v -> valuesEqual(v.value(), commit.value().oldValue()));
  }

  /**
   * Handles a replaceVersion commit.
   *
   * @param commit replaceVersion commit
   * @return map entry update result
   */
  protected MapEntryUpdateResult<String, byte[]> replaceVersion(Commit<? extends ReplaceVersion> commit) {
    MapEntryValue value = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        commit.index(),
        commit.value().newValue(),
        commit.wallClockTime().unixTimestamp(),
        0);
    return replaceIf(commit.index(), commit.value().key(), value,
        v -> v.version() == commit.value().oldVersion());
  }

  /**
   * Handles a clear commit.
   *
   * @return clear result
   */
  protected MapEntryUpdateResult.Status clear() {
    Iterator<Map.Entry<String, MapEntryValue>> iterator = entries().entrySet().iterator();
    Map<String, MapEntryValue> entriesToAdd = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, MapEntryValue> entry = iterator.next();
      String key = entry.getKey();
      MapEntryValue value = entry.getValue();
      if (!valueIsNull(value)) {
        Versioned<byte[]> removedValue = new Versioned<>(value.value(), value.version());
        publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, removedValue));
        cancelTtl(value);
        if (activeTransactions.isEmpty()) {
          iterator.remove();
        } else {
          entriesToAdd.put(key, new MapEntryValue(MapEntryValue.Type.TOMBSTONE, value.version, null, 0, 0));
        }
      }
    }
    entries().putAll(entriesToAdd);
    return MapEntryUpdateResult.Status.OK;
  }

  /**
   * Handles a listen commit.
   *
   * @param session listen session
   */
  protected void listen(Session session) {
    listeners.put(session.sessionId().id(), session);
  }

  /**
   * Handles an unlisten commit.
   *
   * @param session unlisten session
   */
  protected void unlisten(Session session) {
    listeners.remove(session.sessionId().id());
  }

  /**
   * Handles a begin commit.
   *
   * @param commit transaction begin commit
   * @return transaction state version
   */
  protected long begin(Commit<? extends TransactionBegin> commit) {
    long version = commit.index();
    activeTransactions.put(commit.value().transactionId(), new TransactionScope(version));
    return version;
  }

  /**
   * Handles an prepare and commit commit.
   *
   * @param commit transaction prepare and commit commit
   * @return prepare result
   */
  protected PrepareResult prepareAndCommit(Commit<? extends TransactionPrepareAndCommit> commit) {
    TransactionId transactionId = commit.value().transactionLog().transactionId();
    PrepareResult prepareResult = prepare(commit);
    TransactionScope transactionScope = activeTransactions.remove(transactionId);
    if (prepareResult == PrepareResult.OK) {
      this.currentVersion = commit.index();
      transactionScope = transactionScope.prepared(commit);
      commitTransaction(transactionScope);
    }
    discardTombstones();
    return prepareResult;
  }

  /**
   * Handles an prepare commit.
   *
   * @param commit transaction prepare commit
   * @return prepare result
   */
  protected PrepareResult prepare(Commit<? extends TransactionPrepare> commit) {
    try {
      TransactionLog<MapUpdate<String, byte[]>> transactionLog = commit.value().transactionLog();

      // Iterate through records in the transaction log and perform isolation checks.
      for (MapUpdate<String, byte[]> record : transactionLog.records()) {
        String key = record.key();

        // If the record is a VERSION_MATCH then check that the record's version matches the current
        // version of the state machine.
        if (record.type() == MapUpdate.Type.VERSION_MATCH && key == null) {
          if (record.version() > currentVersion) {
            return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
          } else {
            continue;
          }
        }

        // If the prepared keys already contains the key contained within the record, that indicates a
        // conflict with a concurrent transaction.
        if (preparedKeys.contains(key)) {
          return PrepareResult.CONCURRENT_TRANSACTION;
        }

        // Read the existing value from the map.
        MapEntryValue existingValue = entries().get(key);

        // Note: if the existing value is null, that means the key has not changed during the transaction,
        // otherwise a tombstone would have been retained.
        if (existingValue == null) {
          // If the value is null, ensure the version is equal to the transaction version.
          if (record.type() != Type.PUT_IF_ABSENT && record.version() != transactionLog.version()) {
            return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
          }
        } else {
          // If the value is non-null, compare the current version with the record version.
          if (existingValue.version() > record.version()) {
            return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
          }
        }
      }

      // No violations detected. Mark modified keys locked for transactions.
      transactionLog.records().forEach(record -> {
        if (record.type() != MapUpdate.Type.VERSION_MATCH) {
          preparedKeys.add(record.key());
        }
      });

      // Update the transaction scope. If the transaction scope is not set on this node, that indicates the
      // coordinator is communicating with another node. Transactions assume that the client is communicating
      // with a single leader in order to limit the overhead of retaining tombstones.
      TransactionScope transactionScope = activeTransactions.get(transactionLog.transactionId());
      if (transactionScope == null) {
        activeTransactions.put(
            transactionLog.transactionId(),
            new TransactionScope(transactionLog.version(), commit.value().transactionLog()));
        return PrepareResult.PARTIAL_FAILURE;
      } else {
        activeTransactions.put(
            transactionLog.transactionId(),
            transactionScope.prepared(commit));
        return PrepareResult.OK;
      }
    } catch (Exception e) {
      getLogger().warn("Failure applying {}", commit, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Handles an commit commit (ha!).
   *
   * @param commit transaction commit commit
   * @return commit result
   */
  protected CommitResult commit(Commit<? extends TransactionCommit> commit) {
    TransactionId transactionId = commit.value().transactionId();
    TransactionScope transactionScope = activeTransactions.remove(transactionId);
    if (transactionScope == null) {
      return CommitResult.UNKNOWN_TRANSACTION_ID;
    }

    try {
      this.currentVersion = commit.index();
      return commitTransaction(transactionScope);
    } catch (Exception e) {
      getLogger().warn("Failure applying {}", commit, e);
      throw Throwables.propagate(e);
    } finally {
      discardTombstones();
    }
  }

  /**
   * Applies committed operations to the state machine.
   */
  private CommitResult commitTransaction(TransactionScope transactionScope) {
    TransactionLog<MapUpdate<String, byte[]>> transactionLog = transactionScope.transactionLog();
    boolean retainTombstones = !activeTransactions.isEmpty();

    List<MapEvent<String, byte[]>> eventsToPublish = Lists.newArrayList();
    for (MapUpdate<String, byte[]> record : transactionLog.records()) {
      if (record.type() == MapUpdate.Type.VERSION_MATCH) {
        continue;
      }

      String key = record.key();
      checkState(preparedKeys.remove(key), "key is not prepared");

      if (record.type() == MapUpdate.Type.LOCK) {
        continue;
      }

      MapEntryValue previousValue = entries().remove(key);

      // Cancel the previous timer if set.
      cancelTtl(previousValue);

      MapEntryValue newValue = null;

      // If the record is not a delete, create a transactional commit.
      if (record.type() != MapUpdate.Type.REMOVE_IF_VERSION_MATCH) {
        newValue = new MapEntryValue(MapEntryValue.Type.VALUE, currentVersion, record.value(), 0, 0);
      } else if (retainTombstones) {
        // For deletes, if tombstones need to be retained then create and store a tombstone commit.
        newValue = new MapEntryValue(MapEntryValue.Type.TOMBSTONE, currentVersion, null, 0, 0);
      }

      MapEvent<String, byte[]> event;
      if (newValue != null) {
        entries().put(key, newValue);
        if (!valueIsNull(newValue)) {
          if (!valueIsNull(previousValue)) {
            event = new MapEvent<>(
                MapEvent.Type.UPDATE,
                "",
                key,
                toVersioned(newValue),
                toVersioned(previousValue));
          } else {
            event = new MapEvent<>(
                MapEvent.Type.INSERT,
                "",
                key,
                toVersioned(newValue),
                null);
          }
        } else {
          event = new MapEvent<>(
              MapEvent.Type.REMOVE,
              "",
              key,
              null,
              toVersioned(previousValue));
        }
      } else {
        event = new MapEvent<>(
            MapEvent.Type.REMOVE,
            "",
            key,
            null,
            toVersioned(previousValue));
      }
      eventsToPublish.add(event);
    }
    publish(eventsToPublish);
    return CommitResult.OK;
  }

  /**
   * Handles an rollback commit (ha!).
   *
   * @param commit transaction rollback commit
   * @return rollback result
   */
  protected RollbackResult rollback(Commit<? extends TransactionRollback> commit) {
    TransactionId transactionId = commit.value().transactionId();
    TransactionScope transactionScope = activeTransactions.remove(transactionId);
    if (transactionScope == null) {
      return RollbackResult.UNKNOWN_TRANSACTION_ID;
    } else if (!transactionScope.isPrepared()) {
      discardTombstones();
      return RollbackResult.OK;
    } else {
      try {
        transactionScope.transactionLog().records()
            .forEach(record -> {
              if (record.type() != MapUpdate.Type.VERSION_MATCH) {
                preparedKeys.remove(record.key());
              }
            });
        return RollbackResult.OK;
      } finally {
        discardTombstones();
      }
    }

  }

  /**
   * Discards tombstones no longer needed by active transactions.
   */
  private void discardTombstones() {
    if (activeTransactions.isEmpty()) {
      Iterator<Map.Entry<String, MapEntryValue>> iterator = entries().entrySet().iterator();
      while (iterator.hasNext()) {
        MapEntryValue value = iterator.next().getValue();
        if (value.type() == MapEntryValue.Type.TOMBSTONE) {
          iterator.remove();
        }
      }
    } else {
      long lowWaterMark = activeTransactions.values().stream()
          .mapToLong(TransactionScope::version)
          .min().getAsLong();
      Iterator<Map.Entry<String, MapEntryValue>> iterator = entries().entrySet().iterator();
      while (iterator.hasNext()) {
        MapEntryValue value = iterator.next().getValue();
        if (value.type() == MapEntryValue.Type.TOMBSTONE && value.version < lowWaterMark) {
          iterator.remove();
        }
      }
    }
  }

  /**
   * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
   *
   * @param value map entry value
   * @return versioned instance
   */
  protected Versioned<byte[]> toVersioned(MapEntryValue value) {
    return value != null && value.type() != MapEntryValue.Type.TOMBSTONE
        ? new Versioned<>(value.value(), value.version()) : null;
  }

  /**
   * Publishes an event to listeners.
   *
   * @param event event to publish
   */
  private void publish(MapEvent<String, byte[]> event) {
    publish(Lists.newArrayList(event));
  }

  /**
   * Publishes events to listeners.
   *
   * @param events list of map event to publish
   */
  private void publish(List<MapEvent<String, byte[]>> events) {
    listeners.values().forEach(session -> {
      session.publish(CHANGE, serializer()::encode, events);
    });
  }

  @Override
  public void onExpire(Session session) {
    closeListener(session.sessionId().id());
  }

  @Override
  public void onClose(Session session) {
    closeListener(session.sessionId().id());
  }

  private void closeListener(Long sessionId) {
    listeners.remove(sessionId);
  }

  /**
   * Interface implemented by map values.
   */
  protected static class MapEntryValue {
    final Type type;
    final long version;
    final byte[] value;
    final long created;
    final long ttl;
    transient Scheduled timer;

    MapEntryValue(Type type, long version, byte[] value, long created, long ttl) {
      this.type = type;
      this.version = version;
      this.value = value;
      this.created = created;
      this.ttl = ttl;
    }

    /**
     * Returns the value type.
     *
     * @return the value type
     */
    Type type() {
      return type;
    }

    /**
     * Returns the version of the value.
     *
     * @return version
     */
    long version() {
      return version;
    }

    /**
     * Returns the raw {@code byte[]}.
     *
     * @return raw value
     */
    byte[] value() {
      return value;
    }

    /**
     * Returns the time at which the value was created.
     *
     * @return time at which the value was created
     */
    long created() {
      return created;
    }

    /**
     * Returns the value time to live.
     *
     * @return time to live
     */
    long ttl() {
      return ttl;
    }

    /**
     * Value type.
     */
    enum Type {
      VALUE,
      TOMBSTONE,
    }
  }

  /**
   * Map transaction scope.
   */
  protected static final class TransactionScope {
    private final long version;
    private final TransactionLog<MapUpdate<String, byte[]>> transactionLog;

    private TransactionScope(long version) {
      this(version, null);
    }

    private TransactionScope(long version, TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
      this.version = version;
      this.transactionLog = transactionLog;
    }

    /**
     * Returns the transaction version.
     *
     * @return the transaction version
     */
    long version() {
      return version;
    }

    /**
     * Returns whether this is a prepared transaction scope.
     *
     * @return whether this is a prepared transaction scope
     */
    boolean isPrepared() {
      return transactionLog != null;
    }

    /**
     * Returns the transaction commit log.
     *
     * @return the transaction commit log
     */
    TransactionLog<MapUpdate<String, byte[]>> transactionLog() {
      checkState(isPrepared());
      return transactionLog;
    }

    /**
     * Returns a new transaction scope with a prepare commit.
     *
     * @param commit the prepare commit
     * @return new transaction scope updated with the prepare commit
     */
    TransactionScope prepared(Commit<? extends TransactionPrepare> commit) {
      return new TransactionScope(version, commit.value().transactionLog());
    }
  }
}