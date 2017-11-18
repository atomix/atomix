/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.value.impl;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.value.AsyncAtomicValue;
import io.atomix.value.AtomicValueBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of AtomicValueBuilder.
 *
 * @param <V> value type
 */
public class AtomicValueProxyBuilder<V> extends AtomicValueBuilder<V> {
  private final PrimitiveManagementService managementService;

  public AtomicValueProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncAtomicValue<V> newValue(PrimitiveProxy proxy) {
    AtomicValueProxy value = new AtomicValueProxy(proxy.open().join());
    return new TranscodingAsyncAtomicValue<>(
        value,
        serializer()::encode,
        serializer()::decode);
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncAtomicValue<V> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newValue(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
