// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.map.SortedMapCompatibleBuilder;
import io.atomix.primitive.protocol.map.SortedMapProtocol;

/**
 * Builder for {@link DistributedSortedMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class DistributedSortedMapBuilder<K extends Comparable<K>, V>
    extends MapBuilder<DistributedSortedMapBuilder<K, V>, DistributedSortedMapConfig, DistributedSortedMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<DistributedSortedMapBuilder<K, V>>, SortedMapCompatibleBuilder<DistributedSortedMapBuilder<K, V>> {

  public DistributedSortedMapBuilder(String name, DistributedSortedMapConfig config, PrimitiveManagementService managementService) {
    super(DistributedSortedMapType.instance(), name, config, managementService);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public DistributedSortedMapBuilder<K, V> withNullValues() {
    config.setNullValues();
    return this;
  }

  /**
   * Sets whether null values are allowed.
   *
   * @param nullValues whether null values are allowed
   * @return this builder
   */
  public DistributedSortedMapBuilder<K, V> withNullValues(boolean nullValues) {
    config.setNullValues(nullValues);
    return this;
  }

  @Override
  public DistributedSortedMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedSortedMapBuilder<K, V> withProtocol(SortedMapProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
