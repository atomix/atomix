// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue;

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
public abstract class DistributedQueueBuilder<E>
    extends DistributedCollectionBuilder<DistributedQueueBuilder<E>, DistributedQueueConfig, DistributedQueue<E>, E>
    implements ProxyCompatibleBuilder<DistributedQueueBuilder<E>> {

  protected DistributedQueueBuilder(String name, DistributedQueueConfig config, PrimitiveManagementService managementService) {
    super(DistributedQueueType.instance(), name, config, managementService);
  }

  @Override
  public DistributedQueueBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
