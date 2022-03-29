// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Work queue builder.
 */
public abstract class WorkQueueBuilder<E>
    extends PrimitiveBuilder<WorkQueueBuilder<E>, WorkQueueConfig, WorkQueue<E>>
    implements ProxyCompatibleBuilder<WorkQueueBuilder<E>> {

  protected WorkQueueBuilder(String name, WorkQueueConfig config, PrimitiveManagementService managementService) {
    super(WorkQueueType.instance(), name, config, managementService);
  }

  @Override
  public WorkQueueBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
