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

import io.atomix.core.set.AsyncDistributedTreeSet;
import io.atomix.core.set.DistributedTreeSet;
import io.atomix.core.set.DistributedTreeSetBuilder;
import io.atomix.core.set.DistributedTreeSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.set.TreeSetProtocolProvider;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;

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
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof TreeSetProtocolProvider) {
        return managementService.getPrimitiveCache().getPrimitive(name, () ->
            CompletableFuture.completedFuture(((TreeSetProtocolProvider) protocol).<E>newTreeSetProtocol(name, managementService))
                .thenApply(set -> new GossipDistributedTreeSet<>(name, protocol, set)))
            .thenApply(AsyncDistributedTreeSet::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Sets are not supported by the provided gossip protocol"));
      }
    } else {
      return newProxy(DistributedTreeSetService.class, new ServiceConfig())
          .thenCompose(proxy -> new DistributedTreeSetProxy<E>((ProxyClient) proxy, managementService.getPrimitiveRegistry()).connect())
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
}
