/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreBuilder;
import io.atomix.core.semaphore.DistributedSemaphoreConfig;
import io.atomix.core.semaphore.DistributedSemaphoreServiceConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;

import java.util.concurrent.CompletableFuture;


public class DistributedSemaphoreProxyBuilder extends DistributedSemaphoreBuilder {
  public DistributedSemaphoreProxyBuilder(String name, DistributedSemaphoreConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @SuppressWarnings("unchecked")
  @Override
  public CompletableFuture<DistributedSemaphore> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
            name(),
            primitiveType(),
            new DistributedSemaphoreServiceConfig().setInitialCapacity(config.initialCapacity()),
            managementService.getPartitionService());

    return new DistributedSemaphoreProxy(
            proxy,
            managementService.getPrimitiveRegistry(),
            managementService.getExecutorService())
            .connect()
            .thenApply(AsyncDistributedSemaphore::sync);
  }
}
