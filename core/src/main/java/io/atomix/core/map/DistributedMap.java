// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.set.DistributedSet;
import io.atomix.primitive.SyncPrimitive;

/**
 * Distributed map.
 */
public interface DistributedMap<K, V> extends SyncPrimitive, Map<K, V> {

  /**
   * Acquires a lock on the given key.
   *
   * @param key the key for which to acquire the lock
   */
  void lock(K key);

  /**
   * Attempts to acquire a lock on the given key.
   *
   * @param key the key for which to acquire the lock
   * @return a boolean indicating whether the lock was successful
   */
  boolean tryLock(K key);

  /**
   * Attempts to acquire a lock on the given key.
   *
   * @param key the key for which to acquire the lock
   * @param timeout the lock timeout
   * @param unit the lock unit
   * @return a boolean indicating whether the lock was successful
   */
  default boolean tryLock(K key, long timeout, TimeUnit unit) {
    return tryLock(key, Duration.ofMillis(unit.toMillis(timeout)));
  }

  /**
   * Attempts to acquire a lock on the given key.
   *
   * @param key the key for which to acquire the lock
   * @param timeout the lock timeout
   * @return a boolean indicating whether the lock was successful
   */
  boolean tryLock(K key, Duration timeout);

  /**
   * Returns a boolean indicating whether a lock is currently held on the given key.
   *
   * @param key the key for which to determine whether a lock exists
   * @return indicates whether a lock exists on the given key
   */
  boolean isLocked(K key);

  /**
   * Releases a lock on the given key.
   *
   * @param key the key for which to release the lock
   */
  void unlock(K key);

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   */
  default void addListener(MapEventListener<K, V> listener) {
    addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   * @param executor executor to use for handling incoming map events
   */
  void addListener(MapEventListener<K, V> listener, Executor executor);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive map change notifications.
   *
   * @param listener listener to unregister
   */
  void removeListener(MapEventListener<K, V> listener);

  @Override
  DistributedSet<K> keySet();

  @Override
  DistributedSet<Entry<K, V>> entrySet();

  @Override
  DistributedCollection<V> values();

  @Override
  AsyncDistributedMap<K, V> async();
}
