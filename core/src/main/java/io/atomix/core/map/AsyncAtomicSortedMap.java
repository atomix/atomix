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
package io.atomix.core.map;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous atomic sorted map.
 */
public interface AsyncAtomicSortedMap<K extends Comparable<K>, V> extends AsyncAtomicMap<K, V> {

  /**
   * Returns the first (lowest) key currently in this map.
   *
   * @return the first (lowest) key currently in this map
   * @throws NoSuchElementException if this map is empty
   */
  CompletableFuture<K> firstKey();

  /**
   * Returns the last (highest) key currently in this map.
   *
   * @return the last (highest) key currently in this map
   * @throws NoSuchElementException if this map is empty
   */
  CompletableFuture<K> lastKey();

  @Override
  default AtomicSortedMap<K, V> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicSortedMap<K, V> sync(Duration operationTimeout);
}
