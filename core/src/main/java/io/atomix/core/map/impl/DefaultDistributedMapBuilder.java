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

import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.DistributedMapConfig;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed map builder.
 */
public class DefaultDistributedMapBuilder<K, V> extends DistributedMapBuilder<K, V> {
  public DefaultDistributedMapBuilder(String name, DistributedMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedMap<K, V>> buildAsync() {
    return AtomicMapType.<K, V>instance().newBuilder(name, new AtomicMapConfig()
        .setName(config.getName())
        .setProtocolConfig(config.getProtocolConfig())
        .setNamespaceConfig(config.getNamespaceConfig())
        .setCacheConfig(config.getCacheConfig())
        .setReadOnly(config.isReadOnly()), managementService)
        .buildAsync()
        .thenApply(atomicMap -> new DelegatingAsyncDistributedMap<>(atomicMap.async()).sync());
  }
}
