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

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.map.NavigableMapCompatibleBuilder;
import io.atomix.primitive.protocol.map.NavigableMapProtocol;

/**
 * Builder for {@link DistributedNavigableMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class DistributedNavigableMapBuilder<K extends Comparable<K>, V>
    extends MapBuilder<DistributedNavigableMapBuilder<K, V>, DistributedNavigableMapConfig, DistributedNavigableMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<DistributedNavigableMapBuilder<K, V>>, NavigableMapCompatibleBuilder<DistributedNavigableMapBuilder<K, V>> {

  public DistributedNavigableMapBuilder(String name, DistributedNavigableMapConfig config, PrimitiveManagementService managementService) {
    super(DistributedNavigableMapType.instance(), name, config, managementService);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public DistributedNavigableMapBuilder<K, V> withNullValues() {
    config.setNullValues();
    return this;
  }

  /**
   * Sets whether null values are allowed.
   *
   * @param nullValues whether null values are allowed
   * @return this builder
   */
  public DistributedNavigableMapBuilder<K, V> withNullValues(boolean nullValues) {
    config.setNullValues(nullValues);
    return this;
  }

  @Override
  public DistributedNavigableMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedNavigableMapBuilder<K, V> withProtocol(NavigableMapProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
