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
import com.google.common.collect.Sets;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * State Machine for {@link AtomicMapProxy} resource.
 */
public abstract class AbstractAtomicMapService<K> extends AbstractPrimitiveService<AtomicMapClient> implements AtomicMapService<K> {

  private static final int MAX_ITERATOR_BATCH_SIZE = 1024 * 32;

  private final Serializer serializer;
  protected Set<SessionId> listeners = Sets.newLinkedHashSet();
  private Map<K, MapEntryValue> map;
  protected Set<K> preparedKeys = Sets.newHashSet();
  protected Map<TransactionId, TransactionScope<K>> activeTransactions = Maps.newHashMap();
  protected Map<Long, IteratorContext> entryIterators = Maps.newHashMap();
  protected long currentVersion;

  public AbstractAtomicMapService(PrimitiveType primitiveType) {
    super(primitiveType, AtomicMapClient.class);
    serializer = Serializer.using(Namespace.builder()
        .register(primitiveType.namespace())
        .register(SessionId.class)
        .register(TransactionId.class)
        .register(TransactionScope.class)
        .register(MapEntryValue.class)
        .register(MapEntryValue.Type.class)
        .register(new HashMap().keySet().getClass())
        .register(DefaultIterator.class)
        .build());
    map = createMap();
  }

  protected Map<K, MapEntryValue> createMap() {
    return Maps.newConcurrentMap();
  }

  protected Map<K, MapEntryValue> entries() {
    return map;
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeObject(listeners);
    writer.writeObject(preparedKeys);
    writer.writeObject(Maps.newHashMap(entries()));
    writer.writeObject(activeTransactions);
    writer.writeLong(currentVersion);
    writer.writeObject(entryIterators);
  }

  @Override
  public void restore(BackupInput reader) {
    listeners = reader.readObject();
    preparedKeys = reader.readObject();
    Map<K, MapEntryValue> map = reader.readObject();
    this.map = createMap();
    this.map.putAll(map);
    activeTransactions = reader.readObject();
    currentVersion = reader.readLong();
    entryIterators = reader.readObject();

    map.forEach((key, value) -> {
      if (value.ttl() > 0) {
        value.timer = getScheduler().schedule(Duration.ofMillis(value.ttl() - (getWallClock().getTime().unixTimestamp() - value.created())), () -> {
          entries().remove(key, value);
          publish(new AtomicMapEvent<>(AtomicMapEvent.Type.REMOVE, key, null, toVersioned(value)));
        });
      }
    });
  }

  @Override
  public boolean containsKey(K key) {
    MapEntryValue value = entries().get(key);
    return value != null && value.type() != MapEntryValue.Type.TOMBSTONE;
  }

  @Override
  public boolean containsKeys(Collection<? extends K> keys) {
    for (K key : keys) {
      if (!containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean containsValue(byte[] value) {
    return entries().values().stream()
        .filter(v -> v.type() != MapEntryValue.Type.TOMBSTONE)
        .anyMatch(v -> Arrays.equals(v.value, value));
  }

  @Override
  public Versioned<byte[]> get(K key) {
    return toVersioned(entries().get(key));
  }

  @Override
  public Map<K, Versioned<byte[]>> getAllPresent(Set<K> keys) {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE
            && keys.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, o -> toVersioned(o.getValue())));
  }

  @Override
  public Versioned<byte[]> getOrDefault(K key, byte[] defaultValue) {
    MapEntryValue value = entries().get(key);
    if (value == null) {
      return new Versioned<>(defaultValue, 0);
    } else if (value.type() == MapEntryValue.Type.TOMBSTONE) {
      return new Versioned<>(defaultValue, value.version);
    } else {
      return new Versioned<>(value.value(), value.version);
    }
  }

  @Override
  public int size() {
    return (int) entries().values().stream()
        .filter(value -> value.type() != MapEntryValue.Type.TOMBSTONE)
        .count();
  }

  @Override
  public boolean isEmpty() {
    return entries().values().stream()
        .noneMatch(value -> value.type() != MapEntryValue.Type.TOMBSTONE);
  }

  @Override
  public Set<K> keySet() {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<Versioned<byte[]>> values() {
    return entries().entrySet().stream()
        .filter(entry -> entry.getValue().type() != MapEntryValue.Type.TOMBSTONE)
        .map(entry -> toVersioned(entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public Set<Map.Entry<K, Versioned<byte[]>>> entrySet() {
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
   * @param key   the key to update
   * @param value the value to update
   */
  protected void putValue(K key, MapEntryValue value) {
    MapEntryValue oldValue = entries().put(key, value);
    cancelTtl(oldValue);
    scheduleTtl(key, value);
  }

  /**
   * Schedules the TTL for the given value.
   *
   * @param value the value for which to schedule the TTL
   */
  protected void scheduleTtl(K key, MapEntryValue value) {
    if (value.ttl() > 0) {
      value.timer = getScheduler().schedule(Duration.ofMillis(value.ttl()), () -> {
        entries().remove(key, value);
        publish(new AtomicMapEvent<>(AtomicMapEvent.Type.REMOVE, key, null, toVersioned(value)));
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

  @Override
  public MapEntryUpdateResult<K, byte[]> put(K key, byte[] value, long ttl) {
    MapEntryValue oldValue = entries().get(key);
    MapEntryValue newValue = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        getCurrentIndex(),
        value,
        getWallClock().getTime().unixTimestamp(),
        ttl);

    // If the value is null or a tombstone, this is an insert.
    // Otherwise, only update the value if it has changed to reduce the number of events.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            getCurrentIndex(),
            key,
            toVersioned(oldValue));
      }
      putValue(key, newValue);
      Versioned<byte[]> result = toVersioned(oldValue);
      publish(new AtomicMapEvent<>(AtomicMapEvent.Type.INSERT, key, toVersioned(newValue), result));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, getCurrentIndex(), key, result);
    } else if (!valuesEqual(oldValue, newValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            getCurrentIndex(),
            key,
            toVersioned(oldValue));
      }
      putValue(key, newValue);
      Versioned<byte[]> result = toVersioned(oldValue);
      publish(new AtomicMapEvent<>(AtomicMapEvent.Type.UPDATE, key, toVersioned(newValue), result));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, getCurrentIndex(), key, result);
    }
    // If the value hasn't changed, return a NOOP result.
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, getCurrentIndex(), key, toVersioned(oldValue));
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> putIfAbsent(K key, byte[] value, long ttl) {
    MapEntryValue oldValue = entries().get(key);

    // If the value is null, this is an INSERT.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            getCurrentIndex(),
            key,
            toVersioned(oldValue));
      }
      MapEntryValue newValue = new MapEntryValue(
          MapEntryValue.Type.VALUE,
          getCurrentIndex(),
          value,
          getWallClock().getTime().unixTimestamp(),
          ttl);
      putValue(key, newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new AtomicMapEvent<>(AtomicMapEvent.Type.INSERT, key, result, null));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, getCurrentIndex(), key, null);
    }
    return new MapEntryUpdateResult<>(
        MapEntryUpdateResult.Status.PRECONDITION_FAILED,
        getCurrentIndex(),
        key,
        toVersioned(oldValue));
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> putAndGet(K key, byte[] value, long ttl) {
    MapEntryValue oldValue = entries().get(key);
    MapEntryValue newValue = new MapEntryValue(MapEntryValue.Type.VALUE, getCurrentIndex(), value, getWallClock().getTime().unixTimestamp(), ttl);

    // If the value is null or a tombstone, this is an insert.
    // Otherwise, only update the value if it has changed to reduce the number of events.
    if (valueIsNull(oldValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            getCurrentIndex(),
            key,
            toVersioned(oldValue));
      }
      putValue(key, newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new AtomicMapEvent<>(AtomicMapEvent.Type.INSERT, key, result, null));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, getCurrentIndex(), key, result);
    } else if (!valuesEqual(oldValue, newValue)) {
      // If the key has been locked by a transaction, return a WRITE_LOCK error.
      if (preparedKeys.contains(key)) {
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.WRITE_LOCK,
            getCurrentIndex(),
            key,
            toVersioned(oldValue));
      }
      putValue(key, newValue);
      Versioned<byte[]> result = toVersioned(newValue);
      publish(new AtomicMapEvent<>(AtomicMapEvent.Type.UPDATE, key, result, toVersioned(oldValue)));
      return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, getCurrentIndex(), key, result);
    }
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, getCurrentIndex(), key, toVersioned(oldValue));
  }

  /**
   * Handles a remove commit.
   *
   * @param index     the commit index
   * @param key       the key to remove
   * @param predicate predicate to determine whether to remove the entry
   * @return map entry update result
   */
  private MapEntryUpdateResult<K, byte[]> removeIf(long index, K key, Predicate<MapEntryValue> predicate) {
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
    publish(new AtomicMapEvent<>(AtomicMapEvent.Type.REMOVE, key, null, result));
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> remove(K key) {
    return removeIf(getCurrentIndex(), key, v -> true);
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> remove(K key, byte[] value) {
    return removeIf(getCurrentIndex(), key, v ->
        valuesEqual(v, new MapEntryValue(MapEntryValue.Type.VALUE, getCurrentIndex(), value, getWallClock().getTime().unixTimestamp(), 0)));
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> remove(K key, long version) {
    return removeIf(getCurrentIndex(), key, v -> v.version() == version);
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
  private MapEntryUpdateResult<K, byte[]> replaceIf(
      long index, K key, MapEntryValue newValue, Predicate<MapEntryValue> predicate) {
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
    publish(new AtomicMapEvent<>(AtomicMapEvent.Type.UPDATE, key, toVersioned(newValue), result));
    return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> replace(K key, byte[] value) {
    MapEntryValue entryValue = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        getCurrentIndex(),
        value,
        getWallClock().getTime().unixTimestamp(),
        0);
    return replaceIf(getCurrentIndex(), key, entryValue, v -> true);
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> replace(K key, byte[] oldValue, byte[] newValue) {
    MapEntryValue entryValue = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        getCurrentIndex(),
        newValue,
        getWallClock().getTime().unixTimestamp(),
        0);
    return replaceIf(getCurrentIndex(), key, entryValue,
        v -> valuesEqual(v.value(), oldValue));
  }

  @Override
  public MapEntryUpdateResult<K, byte[]> replace(K key, long oldVersion, byte[] newValue) {
    MapEntryValue value = new MapEntryValue(
        MapEntryValue.Type.VALUE,
        getCurrentIndex(),
        newValue,
        getWallClock().getTime().unixTimestamp(),
        0);
    return replaceIf(getCurrentIndex(), key, value,
        v -> v.version() == oldVersion);
  }

  @Override
  public void clear() {
    Iterator<Map.Entry<K, MapEntryValue>> iterator = entries().entrySet().iterator();
    Map<K, MapEntryValue> entriesToAdd = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<K, MapEntryValue> entry = iterator.next();
      K key = entry.getKey();
      MapEntryValue value = entry.getValue();
      if (!valueIsNull(value)) {
        Versioned<byte[]> removedValue = new Versioned<>(value.value(), value.version());
        publish(new AtomicMapEvent<>(AtomicMapEvent.Type.REMOVE, key, null, removedValue));
        cancelTtl(value);
        if (activeTransactions.isEmpty()) {
          iterator.remove();
        } else {
          entriesToAdd.put(key, new MapEntryValue(MapEntryValue.Type.TOMBSTONE, value.version, null, 0, 0));
        }
      }
    }
    entries().putAll(entriesToAdd);
  }

  @Override
  public IteratorBatch<K> iterateKeys() {
    return iterate(DefaultIterator::new, (k, v) -> k);
  }

  @Override
  public IteratorBatch<K> nextKeys(long iteratorId, int position) {
    return next(iteratorId, position, (k, v) -> k);
  }

  @Override
  public void closeKeys(long iteratorId) {
    close(iteratorId);
  }

  @Override
  public IteratorBatch<Versioned<byte[]>> iterateValues() {
    return iterate(DefaultIterator::new, (k, v) -> v);
  }

  @Override
  public IteratorBatch<Versioned<byte[]>> nextValues(long iteratorId, int position) {
    return next(iteratorId, position, (k, v) -> v);
  }

  @Override
  public void closeValues(long iteratorId) {
    close(iteratorId);
  }

  @Override
  public IteratorBatch<Map.Entry<K, Versioned<byte[]>>> iterateEntries() {
    return iterate(DefaultIterator::new, Maps::immutableEntry);
  }

  @Override
  public IteratorBatch<Map.Entry<K, Versioned<byte[]>>> nextEntries(long iteratorId, int position) {
    return next(iteratorId, position, Maps::immutableEntry);
  }

  @Override
  public void closeEntries(long iteratorId) {
    close(iteratorId);
  }

  protected <T> IteratorBatch<T> iterate(
      Function<Long, IteratorContext> contextFactory,
      BiFunction<K, Versioned<byte[]>, T> function) {
    IteratorContext iterator = contextFactory.apply(getCurrentSession().sessionId().id());
    if (!iterator.iterator().hasNext()) {
      return null;
    }

    long iteratorId = getCurrentIndex();
    entryIterators.put(iteratorId, iterator);
    IteratorBatch<T> batch = next(iteratorId, 0, function);
    if (batch.complete()) {
      entryIterators.remove(iteratorId);
    }
    return batch;
  }

  protected <T> IteratorBatch<T> next(long iteratorId, int position, BiFunction<K, Versioned<byte[]>, T> function) {
    IteratorContext context = entryIterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<T> entries = new ArrayList<>();
    int size = 0;
    while (context.iterator().hasNext()) {
      context.incrementPosition();
      if (context.position() > position) {
        Map.Entry<K, MapEntryValue> entry = context.iterator().next();
        entries.add(function.apply(entry.getKey(), toVersioned(entry.getValue())));
        size += entry.getValue().value().length;

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (entries.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(iteratorId, context.position, entries, !context.iterator().hasNext());
  }

  protected void close(long iteratorId) {
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

  @Override
  public long begin(TransactionId transactionId) {
    long version = getCurrentIndex();
    activeTransactions.put(transactionId, new TransactionScope<>(version));
    return version;
  }

  @Override
  public PrepareResult prepareAndCommit(TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
    TransactionId transactionId = transactionLog.transactionId();
    PrepareResult prepareResult = prepare(transactionLog);
    TransactionScope<K> transactionScope = activeTransactions.remove(transactionId);
    if (prepareResult == PrepareResult.OK) {
      this.currentVersion = getCurrentIndex();
      transactionScope = transactionScope.prepared(transactionLog);
      commitTransaction(transactionScope);
    }
    discardTombstones();
    return prepareResult;
  }

  @Override
  public PrepareResult prepare(TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
    try {
      // Iterate through records in the transaction log and perform isolation checks.
      for (MapUpdate<K, byte[]> record : transactionLog.records()) {
        K key = record.key();

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
          if (record.type() != MapUpdate.Type.PUT_IF_ABSENT && record.version() != transactionLog.version()) {
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
      TransactionScope<K> transactionScope = activeTransactions.get(transactionLog.transactionId());
      if (transactionScope == null) {
        activeTransactions.put(
            transactionLog.transactionId(),
            new TransactionScope<>(transactionLog.version(), transactionLog));
        return PrepareResult.PARTIAL_FAILURE;
      } else {
        activeTransactions.put(
            transactionLog.transactionId(),
            transactionScope.prepared(transactionLog));
        return PrepareResult.OK;
      }
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public CommitResult commit(TransactionId transactionId) {
    TransactionScope<K> transactionScope = activeTransactions.remove(transactionId);
    if (transactionScope == null) {
      return CommitResult.UNKNOWN_TRANSACTION_ID;
    }

    try {
      this.currentVersion = getCurrentIndex();
      return commitTransaction(transactionScope);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    } finally {
      discardTombstones();
    }
  }

  /**
   * Applies committed operations to the state machine.
   */
  private CommitResult commitTransaction(TransactionScope<K> transactionScope) {
    TransactionLog<MapUpdate<K, byte[]>> transactionLog = transactionScope.transactionLog();
    boolean retainTombstones = !activeTransactions.isEmpty();

    List<AtomicMapEvent<K, byte[]>> eventsToPublish = Lists.newArrayList();
    for (MapUpdate<K, byte[]> record : transactionLog.records()) {
      if (record.type() == MapUpdate.Type.VERSION_MATCH) {
        continue;
      }

      K key = record.key();
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

      AtomicMapEvent<K, byte[]> event;
      if (newValue != null) {
        entries().put(key, newValue);
        if (!valueIsNull(newValue)) {
          if (!valueIsNull(previousValue)) {
            event = new AtomicMapEvent<>(
                AtomicMapEvent.Type.UPDATE,
                key,
                toVersioned(newValue),
                toVersioned(previousValue));
          } else {
            event = new AtomicMapEvent<>(
                AtomicMapEvent.Type.INSERT,
                key,
                toVersioned(newValue),
                null);
          }
        } else {
          event = new AtomicMapEvent<>(
              AtomicMapEvent.Type.REMOVE,
              key,
              null,
              toVersioned(previousValue));
        }
      } else {
        event = new AtomicMapEvent<>(
            AtomicMapEvent.Type.REMOVE,
            key,
            null,
            toVersioned(previousValue));
      }
      eventsToPublish.add(event);
    }
    publish(eventsToPublish);
    return CommitResult.OK;
  }

  @Override
  public RollbackResult rollback(TransactionId transactionId) {
    TransactionScope<K> transactionScope = activeTransactions.remove(transactionId);
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
      Iterator<Map.Entry<K, MapEntryValue>> iterator = entries().entrySet().iterator();
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
      Iterator<Map.Entry<K, MapEntryValue>> iterator = entries().entrySet().iterator();
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
  private void publish(AtomicMapEvent<K, byte[]> event) {
    publish(Lists.newArrayList(event));
  }

  /**
   * Publishes events to listeners.
   *
   * @param events list of map event to publish
   */
  private void publish(List<AtomicMapEvent<K, byte[]>> events) {
    listeners.forEach(listener -> events.forEach(event -> getSession(listener).accept(client -> client.change(event))));
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
    public Type type() {
      return type;
    }

    /**
     * Returns the version of the value.
     *
     * @return version
     */
    public long version() {
      return version;
    }

    /**
     * Returns the raw {@code byte[]}.
     *
     * @return raw value
     */
    public byte[] value() {
      return value;
    }

    /**
     * Returns the time at which the value was created.
     *
     * @return time at which the value was created
     */
    public long created() {
      return created;
    }

    /**
     * Returns the value time to live.
     *
     * @return time to live
     */
    public long ttl() {
      return ttl;
    }

    /**
     * Value type.
     */
    public enum Type {
      VALUE,
      TOMBSTONE,
    }
  }

  /**
   * Map transaction scope.
   */
  protected static final class TransactionScope<K> {
    private final long version;
    private final TransactionLog<MapUpdate<K, byte[]>> transactionLog;

    private TransactionScope(long version) {
      this(version, null);
    }

    private TransactionScope(long version, TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
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
    TransactionLog<MapUpdate<K, byte[]>> transactionLog() {
      checkState(isPrepared());
      return transactionLog;
    }

    /**
     * Returns a new transaction scope with a prepare commit.
     *
     * @param transactionLog the transaction log
     * @return new transaction scope updated with the prepare commit
     */
    TransactionScope<K> prepared(TransactionLog<MapUpdate<K, byte[]>> transactionLog) {
      return new TransactionScope(version, transactionLog);
    }
  }

  protected abstract class IteratorContext {
    private final long sessionId;
    private int position = 0;
    private transient Iterator<Map.Entry<K, MapEntryValue>> iterator;

    public IteratorContext(long sessionId) {
      this.sessionId = sessionId;
    }

    protected abstract Iterator<Map.Entry<K, MapEntryValue>> create();

    public long sessionId() {
      return sessionId;
    }

    public int position() {
      return position;
    }

    public void incrementPosition() {
      position++;
    }

    public Iterator<Map.Entry<K, MapEntryValue>> iterator() {
      if (iterator == null) {
        iterator = create();
      }
      return iterator;
    }
  }

  protected class DefaultIterator extends IteratorContext {
    public DefaultIterator(long sessionId) {
      super(sessionId);
    }

    @Override
    protected Iterator<Map.Entry<K, MapEntryValue>> create() {
      return entries().entrySet().iterator();
    }
  }
}
