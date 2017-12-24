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
package io.atomix.core.value.impl;

import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;

import java.util.concurrent.CompletableFuture;

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

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicValue<V>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .newProxy(name(), primitiveType(), protocol)
        .connect()
        .thenApply(proxy -> {
          AtomicValueProxy value = new AtomicValueProxy(proxy);
          return new TranscodingAsyncAtomicValue<V, byte[]>(
              value,
              serializer()::encode,
              serializer()::decode)
              .sync();
        });
  }
}
