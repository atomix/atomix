/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
