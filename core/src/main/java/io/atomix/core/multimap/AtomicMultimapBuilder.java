// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * A builder class for {@code AsyncConsistentMultimap}.
 */
public abstract class AtomicMultimapBuilder<K, V>
    extends MultimapBuilder<AtomicMultimapBuilder<K, V>, AtomicMultimapConfig, AtomicMultimap<K, V>, K, V>
    implements ProxyCompatibleBuilder<AtomicMultimapBuilder<K, V>> {

  protected AtomicMultimapBuilder(String name, AtomicMultimapConfig config, PrimitiveManagementService managementService) {
    super(AtomicMultimapType.instance(), name, config, managementService);
  }

  @Override
  public AtomicMultimapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
