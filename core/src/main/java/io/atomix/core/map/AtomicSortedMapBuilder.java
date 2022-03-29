// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.map;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for {@link AtomicSortedMap}.
 */
public abstract class AtomicSortedMapBuilder<K extends Comparable<K>, V>
    extends MapBuilder<AtomicSortedMapBuilder<K, V>, AtomicSortedMapConfig, AtomicSortedMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<AtomicSortedMapBuilder<K, V>> {

  protected AtomicSortedMapBuilder(String name, AtomicSortedMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicSortedMapType.instance(), name, config, managementService);
  }

  @Override
  public AtomicSortedMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
