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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMapBuilder;
import io.atomix.core.map.ConsistentTreeMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncConsistentTreeMap} builder.
 *
 * @param <V> type for map value
 */
public class ConsistentTreeMapProxyBuilder<V> extends ConsistentTreeMapBuilder<V> {
  public ConsistentTreeMapProxyBuilder(String name, ConsistentTreeMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ConsistentTreeMap<V>> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
        name(),
        primitiveType(),
        managementService.getPartitionService());
    return new ConsistentTreeMapProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(map -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncConsistentTreeMap<V, byte[]>(
              (AsyncConsistentTreeMap) map,
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
