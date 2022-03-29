// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed cyclic barrier builder.
 */
public abstract class DistributedCyclicBarrierBuilder
    extends PrimitiveBuilder<DistributedCyclicBarrierBuilder, DistributedCyclicBarrierConfig, DistributedCyclicBarrier>
    implements ProxyCompatibleBuilder<DistributedCyclicBarrierBuilder> {

  protected Runnable barrierAction = () -> {
  };

  protected DistributedCyclicBarrierBuilder(String name, DistributedCyclicBarrierConfig config, PrimitiveManagementService managementService) {
    super(DistributedCyclicBarrierType.instance(), name, config, managementService);
  }

  @Override
  public DistributedCyclicBarrierBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  /**
   * Sets the action to run when the barrier is tripped.
   *
   * @param barrierAction the action to run when the barrier is tripped
   * @return the cyclic barrier builder
   */
  public DistributedCyclicBarrierBuilder withBarrierAction(Runnable barrierAction) {
    this.barrierAction = checkNotNull(barrierAction);
    return this;
  }
}
