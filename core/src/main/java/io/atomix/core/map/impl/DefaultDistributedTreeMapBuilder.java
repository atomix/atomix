/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.map.impl;

import io.atomix.core.map.DistributedTreeMap;
import io.atomix.core.map.DistributedTreeMapBuilder;
import io.atomix.core.map.DistributedTreeMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed tree map builder.
 */
public class DefaultDistributedTreeMapBuilder<K extends Comparable<K>, V> extends DistributedTreeMapBuilder<K, V> {
  public DefaultDistributedTreeMapBuilder(String name, DistributedTreeMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedTreeMap<K, V>> buildAsync() {
    ProxyClient proxy = protocol().newProxy(
        name(),
        primitiveType(),
        AtomicTreeMapService.class,
        new ServiceConfig(),
        managementService.getPartitionService());
    return new AtomicTreeMapProxy<K>(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(map -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicTreeMap<K, V, byte[]>(
              map,
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));
        }).thenApply(atomicTreeMap -> new DelegatingAsyncDistributedTreeMap<>(atomicTreeMap).sync());
  }
}
