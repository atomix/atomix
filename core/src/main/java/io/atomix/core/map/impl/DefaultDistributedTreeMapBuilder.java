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

import io.atomix.core.map.AsyncDistributedTreeMap;
import io.atomix.core.map.DistributedTreeMap;
import io.atomix.core.map.DistributedTreeMapBuilder;
import io.atomix.core.map.DistributedTreeMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.TreeMapProtocolProvider;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
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
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof TreeMapProtocolProvider) {
        return managementService.getPrimitiveCache().getPrimitive(name, () ->
            CompletableFuture.completedFuture(((TreeMapProtocolProvider) protocol).<K, V>newTreeMapProtocol(name, managementService))
                .thenApply(set -> new AsyncDistributedJavaTreeMap<>(name, protocol, set)))
            .thenApply(AsyncDistributedTreeMap::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Sets are not supported by the provided gossip protocol"));
      }
    } else {
      return newProxy(AtomicTreeMapService.class, new ServiceConfig())
          .thenCompose(proxy -> new AtomicTreeMapProxy<K>((ProxyClient) proxy, managementService.getPrimitiveRegistry()).connect())
          .thenApply(map -> {
            Serializer serializer = protocol.serializer();
            return new TranscodingAsyncAtomicTreeMap<K, V, byte[]>(
                map,
                value -> serializer.encode(value),
                bytes -> serializer.decode(bytes));
          }).thenApply(atomicTreeMap -> new DelegatingAsyncDistributedTreeMap<>(atomicTreeMap).sync());
    }
  }
}
