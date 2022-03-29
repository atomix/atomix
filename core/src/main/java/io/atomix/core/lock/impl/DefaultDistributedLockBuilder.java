// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.DistributedLockBuilder;
import io.atomix.core.lock.DistributedLockConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed lock builder implementation.
 */
public class DefaultDistributedLockBuilder extends DistributedLockBuilder {
  public DefaultDistributedLockBuilder(String name, DistributedLockConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedLock> buildAsync() {
    return newProxy(AtomicLockService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicLockProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(lock -> new DelegatingAsyncDistributedLock(lock).sync());
  }
}
