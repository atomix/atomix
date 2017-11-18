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
package io.atomix.counter.impl;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.counter.AsyncAtomicCounter;
import io.atomix.counter.AtomicCounterBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomic counter proxy builder.
 */
public class AtomicCounterProxyBuilder extends AtomicCounterBuilder {
  private final PrimitiveManagementService managementService;

  public AtomicCounterProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncAtomicCounter newCounter(PrimitiveProxy proxy) {
    return new AtomicCounterProxy(proxy.open().join());
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncAtomicCounter buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newCounter(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
