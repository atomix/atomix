// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for {@link AtomicMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class AtomicMapBuilder<K, V>
    extends MapBuilder<AtomicMapBuilder<K, V>, AtomicMapConfig, AtomicMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<AtomicMapBuilder<K, V>> {

  protected AtomicMapBuilder(String name, AtomicMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicMapType.instance(), name, config, managementService);
  }

  /**
   * Enables null values in the map.
   *
   * @return this builder
   */
  public AtomicMapBuilder<K, V> withNullValues() {
    config.setNullValues();
    return this;
  }

  /**
   * Sets whether null values are allowed.
   *
   * @param nullValues whether null values are allowed
   * @return this builder
   */
  public AtomicMapBuilder<K, V> withNullValues(boolean nullValues) {
    config.setNullValues(nullValues);
    return this;
  }

  @Override
  public AtomicMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
