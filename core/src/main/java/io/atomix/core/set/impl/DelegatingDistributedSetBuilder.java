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
package io.atomix.core.set.impl;

import io.atomix.core.map.ConsistentMapConfig;
import io.atomix.core.map.impl.ConsistentMapProxyBuilder;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DelegatingDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  public DelegatingDistributedSetBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    ConsistentMapConfig mapConfig = new ConsistentMapConfig();
    return new ConsistentMapProxyBuilder<E, Boolean>(name(), mapConfig, managementService)
        .buildAsync()
        .thenApply(map -> new DelegatingAsyncDistributedSet<>(map.async()).sync());
  }
}
