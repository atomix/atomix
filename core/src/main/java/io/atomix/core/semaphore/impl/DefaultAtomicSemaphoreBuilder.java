// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphoreBuilder;
import io.atomix.core.semaphore.AtomicSemaphoreConfig;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

public class DefaultAtomicSemaphoreBuilder extends AtomicSemaphoreBuilder {
  public DefaultAtomicSemaphoreBuilder(String name, AtomicSemaphoreConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @SuppressWarnings("unchecked")
  @Override
  public CompletableFuture<AtomicSemaphore> buildAsync() {
    return newProxy(AtomicSemaphoreService.class, new AtomicSemaphoreServiceConfig().setInitialCapacity(config.initialCapacity()))
        .thenCompose(proxy -> new AtomicSemaphoreProxy(
            proxy,
            managementService.getPrimitiveRegistry(),
            managementService.getExecutorService())
            .connect())
        .thenApply(AsyncAtomicSemaphore::sync);
  }
}
