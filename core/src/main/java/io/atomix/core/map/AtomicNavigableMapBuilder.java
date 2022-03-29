// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.map;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for {@link AtomicNavigableMap}.
 */
public abstract class AtomicNavigableMapBuilder<K extends Comparable<K>, V>
    extends MapBuilder<AtomicNavigableMapBuilder<K, V>, AtomicNavigableMapConfig, AtomicNavigableMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<AtomicNavigableMapBuilder<K, V>> {

  protected AtomicNavigableMapBuilder(String name, AtomicNavigableMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicNavigableMapType.instance(), name, config, managementService);
  }

  @Override
  public AtomicNavigableMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
