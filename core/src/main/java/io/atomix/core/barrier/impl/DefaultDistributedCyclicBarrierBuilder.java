/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
