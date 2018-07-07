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

import io.atomix.core.set.DistributedTreeSet;
import io.atomix.core.set.DistributedTreeSetBuilder;
import io.atomix.core.set.DistributedTreeSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed tree set builder.
 *
 * @param <E> type for set elements
 */
public class DefaultDistributedTreeSetBuilder<E extends Comparable<E>> extends DistributedTreeSetBuilder<E> {
  public DefaultDistributedTreeSetBuilder(String name, DistributedTreeSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedTreeSet<E>> buildAsync() {
    ProxyClient proxy = protocol().newProxy(
        name(),
        primitiveType(),
        DistributedTreeSetService.class,
        new ServiceConfig(),
        managementService.getPartitionService());
    return new DistributedTreeSetProxy<E>(proxy, managementService.getPrimitiveRegistry()).connect()
        .thenApply(set -> {
          if (config.getCacheConfig().isEnabled()) {
            set = new CachingAsyncDistributedTreeSet<>(set, config.getCacheConfig());
          }

          if (config.isReadOnly()) {
            set = new UnmodifiableAsyncDistributedTreeSet<>(set);
          }
          return set.sync();
        });
  }
}
