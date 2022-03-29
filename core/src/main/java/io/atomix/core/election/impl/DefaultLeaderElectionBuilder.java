// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election.impl;

import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeaderElectionConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class DefaultLeaderElectionBuilder<T> extends LeaderElectionBuilder<T> {
  public DefaultLeaderElectionBuilder(String name, LeaderElectionConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<LeaderElection<T>> buildAsync() {
    return newProxy(LeaderElectionService.class, new ServiceConfig())
        .thenCompose(proxy -> new LeaderElectionProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(election -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncLeaderElection<T, byte[]>(
              election,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
