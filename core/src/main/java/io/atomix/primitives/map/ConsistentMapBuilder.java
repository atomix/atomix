/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.primitives.map;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for {@link ConsistentMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class ConsistentMapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentMapBuilder<K, V>, ConsistentMap<K, V>, AsyncConsistentMap<K, V>> {

  private boolean nullValues = false;

  public ConsistentMapBuilder() {
    super(DistributedPrimitive.Type.CONSISTENT_MAP);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public ConsistentMapBuilder<K, V> withNullValues() {
    nullValues = true;
    return this;
  }

  /**
   * Returns whether null values are supported by the map.
   *
   * @return {@code true} if null values are supported; {@code false} otherwise
   */
  public boolean nullValues() {
    return nullValues;
  }

  @Override
  public ConsistentMap<K, V> build() {
    return buildAsync().asConsistentMap();
  }
}
