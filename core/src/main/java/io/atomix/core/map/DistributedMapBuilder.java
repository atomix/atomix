// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.map.MapCompatibleBuilder;
import io.atomix.primitive.protocol.map.MapProtocol;

/**
 * Builder for {@link DistributedMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class DistributedMapBuilder<K, V>
    extends MapBuilder<DistributedMapBuilder<K, V>, DistributedMapConfig, DistributedMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<DistributedMapBuilder<K, V>>, MapCompatibleBuilder<DistributedMapBuilder<K, V>> {

  protected DistributedMapBuilder(String name, DistributedMapConfig config, PrimitiveManagementService managementService) {
    super(DistributedMapType.instance(), name, config, managementService);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public DistributedMapBuilder<K, V> withNullValues() {
    config.setNullValues();
    return this;
  }

  /**
   * Sets whether null values are allowed.
   *
   * @param nullValues whether null values are allowed
   * @return this builder
   */
  public DistributedMapBuilder<K, V> withNullValues(boolean nullValues) {
    config.setNullValues(nullValues);
    return this;
  }

  @Override
  public DistributedMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedMapBuilder<K, V> withProtocol(MapProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
