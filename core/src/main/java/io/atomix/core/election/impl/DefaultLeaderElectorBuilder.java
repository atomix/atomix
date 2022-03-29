// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
