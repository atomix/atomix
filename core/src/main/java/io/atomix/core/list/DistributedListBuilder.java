// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list;

import io.atomix.core.collection.DistributedCollectionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for distributed queue.
 *
 * @param <E> queue element type
 */
public abstract class DistributedListBuilder<E>
    extends DistributedCollectionBuilder<DistributedListBuilder<E>, DistributedListConfig, DistributedList<E>, E>
    implements ProxyCompatibleBuilder<DistributedListBuilder<E>> {

  protected DistributedListBuilder(String name, DistributedListConfig config, PrimitiveManagementService managementService) {
    super(DistributedListType.instance(), name, config, managementService);
  }

  @Override
  public DistributedListBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
