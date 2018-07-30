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

import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.DistributedSortedSet;
import io.atomix.core.set.DistributedSortedSetBuilder;
import io.atomix.core.set.DistributedSortedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.set.SortedSetProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed sorted set builder.
 *
 * @param <E> type for set elements
 */
public class DefaultDistributedSortedSetBuilder<E extends Comparable<E>> extends DistributedSortedSetBuilder<E> {
  public DefaultDistributedSortedSetBuilder(String name, DistributedSortedSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSortedSet<E>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof SortedSetProtocol) {
        return managementService.getPrimitiveCache().getPrimitive(name, () ->
            CompletableFuture.completedFuture(((SortedSetProtocol) protocol).<E>newSortedSetDelegate(name, serializer(), managementService))
                .thenApply(set -> new GossipDistributedSortedSet<>(name, protocol, set)))
            .thenApply(AsyncDistributedSortedSet::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Sets are not supported by the provided gossip protocol"));
      }
    } else {
      return newProxy(DistributedTreeSetService.class, new ServiceConfig())
          .thenCompose(proxy -> new DistributedNavigableSetProxy<E>((ProxyClient) proxy, managementService.getPrimitiveRegistry()).connect())
          .thenApply(set -> {
            if (config.getCacheConfig().isEnabled()) {
              set = new CachingAsyncDistributedNavigableSet<>(set, config.getCacheConfig());
            }

            if (config.isReadOnly()) {
              set = new UnmodifiableAsyncDistributedNavigableSet<>(set);
            }
            return set.sync();
          });
    }
  }
}
