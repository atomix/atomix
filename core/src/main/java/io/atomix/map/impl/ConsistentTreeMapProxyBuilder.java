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
package io.atomix.map.impl;

import io.atomix.map.AsyncConsistentTreeMap;
import io.atomix.map.ConsistentTreeMapBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default {@link AsyncConsistentTreeMap} builder.
 *
 * @param <V> type for map value
 */
public class ConsistentTreeMapProxyBuilder<V> extends ConsistentTreeMapBuilder<V> {
  private final PrimitiveManagementService managementService;

  public ConsistentTreeMapProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncConsistentTreeMap<V> newTreeMap(PrimitiveProxy proxy) {
    ConsistentTreeMapProxy rawMap = new ConsistentTreeMapProxy(proxy.open().join());

    Serializer serializer = serializer();
    return new TranscodingAsyncConsistentTreeMap<>(
        rawMap,
        value -> value == null ? null : serializer.encode(value),
        bytes -> serializer.decode(bytes));
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncConsistentTreeMap<V> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newTreeMap(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
