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
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed semaphore builder.
 */
public class DefaultDistributedSemaphoreBuilder extends DistributedSemaphoreBuilder {
  public DefaultDistributedSemaphoreBuilder(String name, DistributedSemaphoreConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @SuppressWarnings("unchecked")
  @Override
  public CompletableFuture<DistributedSemaphore> buildAsync() {
    return newProxy(AtomicSemaphoreService.class, new AtomicSemaphoreServiceConfig().setInitialCapacity(config.initialCapacity()))
        .thenCompose(proxy -> new AtomicSemaphoreProxy(
            proxy,
            managementService.getPrimitiveRegistry(),
            managementService.getExecutorService())
            .connect())
        .thenApply(DelegatingAsyncDistributedSemaphore::new)
        .thenApply(AsyncDistributedSemaphore::sync);
  }
}
