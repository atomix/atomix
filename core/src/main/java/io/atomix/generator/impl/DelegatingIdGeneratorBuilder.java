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
package io.atomix.generator.impl;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.counter.impl.AtomicCounterProxy;
import io.atomix.generator.AsyncAtomicIdGenerator;
import io.atomix.generator.AtomicIdGenerator;
import io.atomix.generator.AtomicIdGeneratorBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of AtomicIdGeneratorBuilder.
 */
public class DelegatingIdGeneratorBuilder extends AtomicIdGeneratorBuilder {
  private final PrimitiveManagementService managementService;

  public DelegatingIdGeneratorBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  private AsyncAtomicIdGenerator newIdGenerator(PrimitiveProxy proxy) {
    return new DelegatingIdGenerator(new AtomicCounterProxy(proxy.open().join()));
  }

  @Override
  public AtomicIdGenerator build() {
    return buildAsync().asAtomicIdGenerator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncAtomicIdGenerator buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newIdGenerator(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
