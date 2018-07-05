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

import io.atomix.core.map.impl.AtomicMapService;
import io.atomix.primitive.operation.Query;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Consistent tree map service.
 */
public interface AtomicTreeMapService<K extends Comparable<K>> extends AtomicMapService<K> {

  /**
   * Returns the lowest key in the map.
   *
   * @return the key or null if none exist
   */
  @Query
  K firstKey();

  /**
   * Returns the highest key in the map.
   *
   * @return the key or null if none exist
   */
  @Query
  K lastKey();

  /**
   * Returns the entry associated with the least key greater than or equal to the key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> ceilingEntry(K key);

  /**
   * Returns the entry associated with the greatest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> floorEntry(K key);

  /**
   * Returns the entry associated with the lest key greater than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> higherEntry(K key);

  /**
   * Returns the entry associated with the largest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> lowerEntry(K key);

  /**
   * Returns the entry associated with the lowest key in the map.
   *
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> firstEntry();

  /**
   * Returns the entry associated with the highest key in the map.
   *
   * @return the entry or null
   */
  @Query
  Map.Entry<K, Versioned<byte[]>> lastEntry();

  /**
   * Returns the entry associated with the greatest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  K lowerKey(K key);

  /**
   * Returns the entry associated with the highest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  K floorKey(K key);

  /**
   * Returns the lowest key greater than or equal to key.
   *
   * @param key the key
   * @return the key or null
   */
  @Query
  K ceilingKey(K key);

  /**
   * Returns the lowest key greater than key.
   *
   * @param key the key
   * @return the key or null
   */
  @Query
  K higherKey(K key);

  /**
   * Returns a navigable map containing the entries from the original map
   * which are larger than (or if specified equal to) {@code lowerKey} AND
   * less than (or if specified equal to) {@code upperKey}.
   *
   * @param fromKey       the start key
   * @param fromInclusive whether the start key is inclusive
   * @param toKey         the end key
   * @param toInclusive   whether the end key is inclusive
   * @return a navigable map containing entries in the specified range (this
   * may be empty)
   */
  @Query
  NavigableMap<K, byte[]> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

}
