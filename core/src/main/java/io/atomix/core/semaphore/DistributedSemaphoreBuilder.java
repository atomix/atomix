// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Distributed semaphore builder.
 */
public abstract class DistributedSemaphoreBuilder
    extends PrimitiveBuilder<DistributedSemaphoreBuilder, DistributedSemaphoreConfig, DistributedSemaphore>
    implements ProxyCompatibleBuilder<DistributedSemaphoreBuilder> {

  protected DistributedSemaphoreBuilder(String name, DistributedSemaphoreConfig config, PrimitiveManagementService managementService) {
    super(DistributedSemaphoreType.instance(), name, config, managementService);
  }

  /**
   * Sets the semaphore's initial capacity.
   *
   * @param permits the initial number of permits
   * @return the semaphore builder
   */
  public DistributedSemaphoreBuilder withInitialCapacity(int permits) {
    config.setInitialCapacity(permits);
    return this;
  }

  @Override
  public DistributedSemaphoreBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
