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

package io.atomix.core.map;

/**
 * Atomic sorted map.
 */
public interface AtomicSortedMap<K extends Comparable<K>, V> extends AtomicMap<K, V> {

  /**
   * Returns the lowest key in the map.
   *
   * @return the key or null if none exist
   */
  K firstKey();

  /**
   * Returns the highest key in the map.
   *
   * @return the key or null if none exist
   */
  K lastKey();

  @Override
  AsyncAtomicSortedMap<K, V> async();
}
