/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.map.impl;

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;
import io.atomix.utils.time.Versioned;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Consistent map service.
 */
public interface AtomicMapService<K> {

  /**
   * Returns the number of entries in the map.
   *
   * @return map size.
   */
  @Query
  int size();

  /**
   * Returns true if the map is empty.
   *
   * @return true if map has no entries, false otherwise
   */
  @Query
  boolean isEmpty();

  /**
   * Returns true if this map contains a mapping for the specified key.
   *
   * @param key key
   * @return true if map contains key, false otherwise
   */
  @Query
  boolean containsKey(K key);

  /**
   * Returns true if this map contains mappings for all the specified keys.
   *
   * @param keys keys
   * @return true if map contains key, false otherwise
   */
  @Query
  boolean containsKeys(Collection<? extends K> keys);

  /**
   * Returns true if this map contains the specified value.
   *
   * @param value value
   * @return true if map contains value, false otherwise.
   */
  @Query
  boolean containsValue(byte[] value);

  /**
   * Returns the value (and version) to which the specified key is mapped, or null if this map contains no mapping for
   * the key.
   *
   * @param key the key whose associated value (and version) is to be returned
   * @return the value (and version) to which the specified key is mapped, or null if this map contains no mapping for
   *     the key
   */
  @Query
  Versioned<byte[]> get(K key);

  /**
   * Returns a map of the values associated with the {@code keys} in this map. The returned map will only contain
   * entries which already exist in the map.
   * <p>
   * Note that duplicate elements in {@code keys}, as determined by {@link Object#equals}, will be ignored.
   *
   * @param keys the keys whose associated values are to be returned
   * @return the unmodifiable mapping of keys to values for the specified keys found in the map
   */
  @Query
  Map<K, Versioned<byte[]>> getAllPresent(Set<K> keys);

  /**
   * Returns the value (and version) to which the specified key is mapped, or the provided default value if this map
   * contains no mapping for the key.
   * <p>
   * Note: a non-null {@link Versioned} value will be returned even if the {@code defaultValue} is {@code null}.
   *
   * @param key the key whose associated value (and version) is to be returned
   * @param defaultValue the default value to return if the key is not set
   * @return the value (and version) to which the specified key is mapped, or null if this map contains no mapping for
   *     the key
   */
  @Query
  Versioned<byte[]> getOrDefault(K key, byte[] defaultValue);

  /**
   * Associates the specified value with the specified key in this map (optional operation). If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value (and version) associated with key, or null if there was no mapping for key.
   */
  @Command
  default MapEntryUpdateResult<K, byte[]> put(K key, byte[] value) {
    return put(key, value, 0);
  }

  /**
   * Associates the specified value with the specified key in this map (optional operation). If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl the time to live after which to remove the value
   * @return the previous value (and version) associated with key, or null if there was no mapping for key.
   */
  @Command("putWithTtl")
  MapEntryUpdateResult<K, byte[]> put(K key, byte[] value, long ttl);

  /**
   * Associates the specified value with the specified key in this map (optional operation). If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return new value.
   */
  @Command
  default MapEntryUpdateResult<K, byte[]> putAndGet(K key, byte[] value) {
    return putAndGet(key, value, 0);
  }

  /**
   * Associates the specified value with the specified key in this map (optional operation). If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl the time to live after which to remove the value
   * @return new value.
   */
  @Command("putAndGetWithTtl")
  MapEntryUpdateResult<K, byte[]> putAndGet(K key, byte[] value, long ttl);

  /**
   * Removes the mapping for a key from this map if it is present (optional operation).
   *
   * @param key key whose value is to be removed from the map
   * @return the value (and version) to which this map previously associated the key, or null if the map contained no
   *     mapping for the key.
   */
  @Command
  MapEntryUpdateResult<K, byte[]> remove(K key);

  /**
   * Removes all of the mappings from this map (optional operation). The map will be empty after this call returns.
   */
  @Command
  void clear();

  /**
   * Returns a Set view of the keys contained in this map. This method differs from the behavior of
   * java.util.Map.keySet() in that what is returned is a unmodifiable snapshot view of the keys in the ConsistentMap.
   * Attempts to modify the returned set, whether direct or via its iterator, result in an
   * UnsupportedOperationException.
   *
   * @return a set of the keys contained in this map
   */
  @Query
  Set<K> keySet();

  /**
   * Returns the collection of values (and associated versions) contained in this map. This method differs from the
   * behavior of java.util.Map.values() in that what is returned is a unmodifiable snapshot view of the values in the
   * ConsistentMap. Attempts to modify the returned collection, whether direct or via its iterator, result in an
   * UnsupportedOperationException.
   *
   * @return a collection of the values (and associated versions) contained in this map
   */
  @Query
  Collection<Versioned<byte[]>> values();

  /**
   * Returns the set of entries contained in this map. This method differs from the behavior of java.util.Map.entrySet()
   * in that what is returned is a unmodifiable snapshot view of the entries in the ConsistentMap. Attempts to modify
   * the returned set, whether direct or via its iterator, result in an UnsupportedOperationException.
   *
   * @return set of entries contained in this map.
   */
  @Query
  Set<Map.Entry<K, Versioned<byte[]>>> entrySet();

  /**
   * If the specified key is not already associated with a value associates it with the given value and returns null,
   * else returns the current value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key or null if key does not already mapped to a value.
   */
  @Command
  default MapEntryUpdateResult<K, byte[]> putIfAbsent(K key, byte[] value) {
    return putIfAbsent(key, value, 0);
  }

  /**
   * If the specified key is not already associated with a value associates it with the given value and returns null,
   * else returns the current value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl the time to live after which to remove the value
   * @return the previous value associated with the specified key or null if key does not already mapped to a value.
   */
  @Command("putIfAbsentWithTtl")
  MapEntryUpdateResult<K, byte[]> putIfAbsent(K key, byte[] value, long ttl);

  /**
   * Removes the entry for the specified key only if it is currently mapped to the specified value.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return true if the value was removed
   */
  @Command("removeValue")
  MapEntryUpdateResult<K, byte[]> remove(K key, byte[] value);

  /**
   * Removes the entry for the specified key only if its current version in the map is equal to the specified version.
   *
   * @param key key with which the specified version is associated
   * @param version version expected to be associated with the specified key
   * @return true if the value was removed
   */
  @Command("removeVersion")
  MapEntryUpdateResult<K, byte[]> remove(K key, long version);

  /**
   * Replaces the entry for the specified key only if there is any value which associated with specified key.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return the previous value associated with the specified key or null
   */
  @Command("replace")
  MapEntryUpdateResult<K, byte[]> replace(K key, byte[] value);

  /**
   * Replaces the entry for the specified key only if currently mapped to the specified value.
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return true if the value was replaced
   */
  @Command("replaceValue")
  MapEntryUpdateResult<K, byte[]> replace(K key, byte[] oldValue, byte[] newValue);

  /**
   * Replaces the entry for the specified key only if it is currently mapped to the specified version.
   *
   * @param key key key with which the specified value is associated
   * @param oldVersion version expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return true if the value was replaced
   */
  @Command("replaceVersion")
  MapEntryUpdateResult<K, byte[]> replace(K key, long oldVersion, byte[] newValue);

  /**
   * Returns a key iterator.
   *
   * @return the key iterator ID
   */
  @Command
  IteratorBatch<K> iterateKeys();

  /**
   * Returns the next batch of entries for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position the iterator position
   * @return the next batch of keys for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<K> nextKeys(long iteratorId, int position);

  /**
   * Closes a key iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void closeKeys(long iteratorId);

  /**
   * Returns a values iterator.
   *
   * @return the values iterator ID
   */
  @Command
  IteratorBatch<Versioned<byte[]>> iterateValues();

  /**
   * Returns the next batch of values for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position the iterator position
   * @return the next batch of values for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<Versioned<byte[]>> nextValues(long iteratorId, int position);

  /**
   * Closes a value iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void closeValues(long iteratorId);

  /**
   * Returns an entry iterator.
   *
   * @return the entry iterator ID
   */
  @Command
  IteratorBatch<Map.Entry<K, Versioned<byte[]>>> iterateEntries();

  /**
   * Returns the next batch of entries for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position the iterator position
   * @return the next batch of entries for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<Map.Entry<K, Versioned<byte[]>>> nextEntries(long iteratorId, int position);

  /**
   * Closes an entry iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void closeEntries(long iteratorId);

  /**
   * Adds a listener to the service.
   */
  @Command
  void listen();

  /**
   * Removes a listener from the service.
   */
  @Command
  void unlisten();

  /**
   * Begins a transaction.
   *
   * @param transactionId the transaction identifier
   * @return the starting version number
   */
  @Command
  long begin(TransactionId transactionId);

  /**
   * Prepares and commits a transaction.
   *
   * @param transactionLog the transaction log
   * @return the prepare result
   */
  @Command
  PrepareResult prepareAndCommit(TransactionLog<MapUpdate<K, byte[]>> transactionLog);

  /**
   * Prepares a transaction.
   *
   * @param transactionLog the transaction log
   * @return the prepare result
   */
  @Command
  PrepareResult prepare(TransactionLog<MapUpdate<K, byte[]>> transactionLog);

  /**
   * Commits a transaction.
   *
   * @param transactionId the transaction identifier
   * @return the commit result
   */
  @Command
  CommitResult commit(TransactionId transactionId);

  /**
   * Rolls back a transaction.
   *
   * @param transactionId the transaction identifier
   * @return the rollback result
   */
  @Command
  RollbackResult rollback(TransactionId transactionId);

}
