// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator.impl;

import io.atomix.core.counter.impl.AtomicCounterProxy;
import io.atomix.core.counter.impl.AtomicCounterService;
import io.atomix.core.idgenerator.AsyncAtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGeneratorBuilder;
import io.atomix.core.idgenerator.AtomicIdGeneratorConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of AtomicIdGeneratorBuilder.
 */
public class DelegatingAtomicIdGeneratorBuilder extends AtomicIdGeneratorBuilder {
  public DelegatingAtomicIdGeneratorBuilder(String name, AtomicIdGeneratorConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<AtomicIdGenerator> buildAsync() {
    return newProxy(AtomicCounterService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicCounterProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(DelegatingAtomicIdGenerator::new)
        .thenApply(AsyncAtomicIdGenerator::sync);
  }
}
