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
public abstract class DistributedMultimapBuilder<K, V>
    extends MultimapBuilder<DistributedMultimapBuilder<K, V>, DistributedMultimapConfig, DistributedMultimap<K, V>, K, V>
    implements ProxyCompatibleBuilder<DistributedMultimapBuilder<K, V>> {

  protected DistributedMultimapBuilder(String name, DistributedMultimapConfig config, PrimitiveManagementService managementService) {
    super(DistributedMultimapType.instance(), name, config, managementService);
  }

  @Override
  public DistributedMultimapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
