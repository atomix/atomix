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

package io.atomix.core.multimap;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.set.DistributedSet;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * This provides a synchronous version of the functionality provided by
 * {@link AsyncAtomicMultimap}.  Instead of returning futures this map
 * blocks until the future completes then returns the result.
 */
public interface AtomicMultimap<K, V> extends SyncPrimitive {

  /**
   * Returns the number of key-value pairs in this multimap.
   *
   * @return the number of key-value pairs
   */
  int size();

  /**
   * Returns if this multimap contains no key-value pairs.
   *
   * @return true if no key-value pairs exist, false otherwise
   */
  boolean isEmpty();

  /**
   * Returns true if there is at lease one key-value pair with a key equal to
   * key.
   *
   * @param key the key to query
   * @return true if the map contains a
   * key-value pair with key false otherwise
   */
  boolean containsKey(K key);

  /**
   * Returns true if this map contains at lease one key-value pair with a
   * value equal to value.
   *
   * @param value the value to query
   * @return true if there is a key-value pair with the specified value,
   * false otherwise.
   */
  boolean containsValue(V value);

  /**
   * Returns true if this map contains at least one key-value pair with key
   * and value specified.
   *
   * @param key   the key to query
   * @param value the value to query
   * @return true if there is a key-value pair with the specified key and
   * value, false otherwise.
   */
  boolean containsEntry(K key, V value);

  /**
   * If the key-value pair does not already exist adds either the key value
   * pair or the value to the set of values associated with the key and
   * returns true, if the key-value pair already exists then behavior is
   * implementation specific with some implementations allowing duplicates
   * and others ignoring put requests for existing entries.
   *
   * @param key   the key to add
   * @param value the value to add
   * @return true if the map has changed because of this call,
   * false otherwise
   */
  boolean put(K key, V value);

  /**
   * Removes the key-value pair with the specified values if it exists. In
   * implementations that allow duplicates which matching entry will be
   * removed is undefined.
   *
   * @param key   the key of the pair to be removed
   * @param value the value of the pair to be removed
   * @return true if the map changed because of this call, false otherwise.
   */
  boolean remove(K key, V value);

  /**
   * Removes the key-value pairs with the specified key and values if they
   * exist. In implementations that allow duplicates each instance of a key
   * will remove one matching entry, which one is not defined. Equivalent to
   * repeated calls to {@code remove()} for each key value pair but more
   * efficient.
   *
   * @param key    the key of the pair to be removed
   * @param values the set of values to be removed
   * @return true if the map changes because of this call, false otherwise.
   */
  boolean removeAll(K key, Collection<? extends V> values);

  /**
   * Removes all values associated with the specified key as well as the key
   * itself.
   *
   * @param key the key whose key-value pairs will be removed
   * @return the set of values that were removed, which may be empty, if the
   * values did not exist the version will be less than one.
   */
  Versioned<Collection<V>> removeAll(K key);

  /**
   * Adds the set of key-value pairs of the specified key with each of the
   * values in the iterable if each key-value pair does not already exist,
   * if the pair does exist the behavior is implementation specific.
   * (Same as repeated puts but with efficiency gains.)
   *
   * @param key    the key to use for all pairs to be added
   * @param values the set of values to be added in pairs with the key
   * @return true if any change in the map results from this call,
   * false otherwise
   */
  boolean putAll(K key, Collection<? extends V> values);

  /**
   * Stores all the values in values associated with the key specified,
   * removes all preexisting values and returns a collection of the removed
   * values which may be empty if the entry did not exist.
   *
   * @param key    the key for all entries to be added
   * @param values the values to be associated with the key
   * @return the collection of removed values, which may be empty
   */
  Versioned<Collection<V>> replaceValues(K key, Collection<V> values);

  /**
   * Removes all key-value pairs, after which it will be empty.
   */
  void clear();

  /**
   * Returns a collection of values associated with the specified key, if the
   * key is not in the map it will return an empty collection.
   *
   * @param key the key whose associated values will be returned
   * @return the collection of the values
   * associated with the specified key, the collection may be empty
   */
  Versioned<Collection<V>> get(K key);

  /**
   * Returns a set of the keys contained in this multimap with one or more
   * associated values.
   *
   * @return the collection of all keys with one or more associated values,
   * this may be empty
   */
  DistributedSet<K> keySet();

  /**
   * Returns a multiset of the keys present in this multimap with one or more
   * associated values each. Keys will appear once for each key-value pair
   * in which they participate.
   *
   * @return a multiset of the keys, this may be empty
   */
  DistributedMultiset<K> keys();

  /**
   * Returns a collection of values in the set with duplicates permitted, the
   * size of this collection will equal the size of the map at the time of
   * creation.
   *
   * @return a collection of values, this may be empty
   */
  DistributedMultiset<V> values();

  /**
   * Returns a collection of each key-value pair in this map.
   *
   * @return a collection of all entries in the map, this may be empty
   */
  DistributedCollection<Map.Entry<K, V>> entries();

  /**
   * Returns the multimap as a distributed map.
   *
   * @return the multimap as a distributed map
   */
  DistributedMap<K, Versioned<Collection<V>>> asMap();

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   */
  default void addListener(AtomicMultimapEventListener<K, V> listener) {
    addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   * @param executor executor to use for handling incoming map events
   */
  void addListener(AtomicMultimapEventListener<K, V> listener, Executor executor);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive map change notifications.
   *
   * @param listener listener to unregister
   */
  void removeListener(AtomicMultimapEventListener<K, V> listener);

  @Override
  AsyncAtomicMultimap<K, V> async();
}
