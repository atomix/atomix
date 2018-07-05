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
package io.atomix.core.map;

import io.atomix.core.cache.CachedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Builder for {@link DistributedTreeMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class DistributedTreeMapBuilder<K extends Comparable<K>, V>
    extends CachedPrimitiveBuilder<DistributedTreeMapBuilder<K, V>, DistributedTreeMapConfig, DistributedTreeMap<K, V>> {

  public DistributedTreeMapBuilder(String name, DistributedTreeMapConfig config, PrimitiveManagementService managementService) {
    super(DistributedTreeMapType.instance(), name, config, managementService);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public DistributedTreeMapBuilder<K, V> withNullValues() {
    config.setNullValues();
    return this;
  }

  /**
   * Sets whether null values are allowed.
   *
   * @param nullValues whether null values are allowed
   * @return this builder
   */
  public DistributedTreeMapBuilder<K, V> withNullValues(boolean nullValues) {
    config.setNullValues(nullValues);
    return this;
  }
}
