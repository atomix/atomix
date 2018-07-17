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

import io.atomix.core.map.AsyncDistributedSortedMap;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.core.map.DistributedSortedMapBuilder;
import io.atomix.core.map.DistributedSortedMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.SortedMapProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed sorted map builder.
 */
public class DefaultDistributedSortedMapBuilder<K extends Comparable<K>, V> extends DistributedSortedMapBuilder<K, V> {
  public DefaultDistributedSortedMapBuilder(String name, DistributedSortedMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSortedMap<K, V>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof SortedMapProtocol) {
        return managementService.getPrimitiveCache().getPrimitive(name, () ->
            CompletableFuture.completedFuture(((SortedMapProtocol) protocol).<K, V>newSortedMapDelegate(name, serializer(), managementService))
                .thenApply(set -> new GossipDistributedSortedMap<>(name, protocol, set)))
            .thenApply(AsyncDistributedSortedMap::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Sets are not supported by the provided gossip protocol"));
      }
    } else {
      return newProxy(AtomicTreeMapService.class, new ServiceConfig())
          .thenCompose(proxy -> new AtomicNavigableMapProxy<K>((ProxyClient) proxy, managementService.getPrimitiveRegistry()).connect())
          .thenApply(map -> {
            Serializer serializer = serializer();
            return new TranscodingAsyncAtomicNavigableMap<K, V, byte[]>(
                map,
                value -> serializer.encode(value),
                bytes -> serializer.decode(bytes));
          }).thenApply(atomicMap -> new DelegatingAsyncDistributedSortedMap<>(atomicMap).sync());
    }
  }
}
