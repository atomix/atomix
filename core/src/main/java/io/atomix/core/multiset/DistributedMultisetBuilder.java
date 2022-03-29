// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multiset;

import io.atomix.core.collection.DistributedCollectionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for distributed multiset.
 *
 * @param <E> multiset element type
 */
public abstract class DistributedMultisetBuilder<E>
    extends DistributedCollectionBuilder<DistributedMultisetBuilder<E>, DistributedMultisetConfig, DistributedMultiset<E>, E>
    implements ProxyCompatibleBuilder<DistributedMultisetBuilder<E>> {

  protected DistributedMultisetBuilder(String name, DistributedMultisetConfig config, PrimitiveManagementService managementService) {
    super(DistributedMultisetType.instance(), name, config, managementService);
  }

  @Override
  public DistributedMultisetBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
