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
package io.atomix.core.election.impl;

import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.core.election.LeaderElectorConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class DefaultLeaderElectorBuilder<T> extends LeaderElectorBuilder<T> {
  public DefaultLeaderElectorBuilder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<LeaderElector<T>> buildAsync() {
    return newProxy(LeaderElectorService.class, new ServiceConfig())
        .thenCompose(proxy -> new LeaderElectorProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(proxy -> {
          Serializer serializer = serializer();
          AsyncLeaderElector<T> elector = new TranscodingAsyncLeaderElector<>(
              proxy,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes));

          if (config.getCacheConfig().isEnabled()) {
            elector = new CachingAsyncLeaderElector<T>(elector, config.getCacheConfig());
          }
          return elector.sync();
        });
  }
}
