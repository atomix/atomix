// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
