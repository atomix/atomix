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

import io.atomix.primitive.operation.Query;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Consistent tree map service.
 */
public interface ConsistentTreeMapService extends ConsistentMapService {

  /**
   * Returns the lowest key in the map.
   *
   * @return the key or null if none exist
   */
  @Query
  String firstKey();

  /**
   * Returns the highest key in the map.
   *
   * @return the key or null if none exist
   */
  @Query
  String lastKey();

  /**
   * Returns the entry associated with the least key greater than or equal to the key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> ceilingEntry(String key);

  /**
   * Returns the entry associated with the greatest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> floorEntry(String key);

  /**
   * Returns the entry associated with the lest key greater than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> higherEntry(String key);

  /**
   * Returns the entry associated with the largest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> lowerEntry(String key);

  /**
   * Returns the entry associated with the lowest key in the map.
   *
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> firstEntry();

  /**
   * Returns the entry associated with the highest key in the map.
   *
   * @return the entry or null
   */
  @Query
  Map.Entry<String, Versioned<byte[]>> lastEntry();

  /**
   * Returns the entry associated with the greatest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  String lowerKey(String key);

  /**
   * Returns the entry associated with the highest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  @Query
  String floorKey(String key);

  /**
   * Returns the lowest key greater than or equal to key.
   *
   * @param key the key
   * @return the key or null
   */
  @Query
  String ceilingKey(String key);

  /**
   * Returns the lowest key greater than key.
   *
   * @param key the key
   * @return the key or null
   */
  @Query
  String higherKey(String key);

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
  NavigableMap<String, byte[]> subMap(String fromKey, boolean fromInclusive, String toKey, boolean toInclusive);

}
