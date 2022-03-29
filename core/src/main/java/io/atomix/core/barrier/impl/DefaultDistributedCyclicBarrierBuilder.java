// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier.impl;

import io.atomix.core.barrier.AsyncDistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrierBuilder;
import io.atomix.core.barrier.DistributedCyclicBarrierConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed cyclic barrier builder implementation.
 */
public class DefaultDistributedCyclicBarrierBuilder extends DistributedCyclicBarrierBuilder {
  public DefaultDistributedCyclicBarrierBuilder(String name, DistributedCyclicBarrierConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedCyclicBarrier> buildAsync() {
    return newProxy(DistributedCyclicBarrierService.class, new ServiceConfig())
        .thenCompose(proxy -> new DistributedCyclicBarrierProxy(proxy, managementService.getPrimitiveRegistry(), barrierAction).connect())
        .thenApply(AsyncDistributedCyclicBarrier::sync);
  }
}
