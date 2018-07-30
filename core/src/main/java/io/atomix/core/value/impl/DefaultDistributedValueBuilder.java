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
package io.atomix.core.value.impl;

import io.atomix.core.value.AsyncDistributedValue;
import io.atomix.core.value.DistributedValue;
import io.atomix.core.value.DistributedValueBuilder;
import io.atomix.core.value.DistributedValueConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.value.ValueProtocol;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of DistributedValueBuilder.
 *
 * @param <V> value type
 */
public class DefaultDistributedValueBuilder<V> extends DistributedValueBuilder<V> {
  public DefaultDistributedValueBuilder(String name, DistributedValueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedValue<V>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof ValueProtocol) {
        CompletableFuture<AsyncDistributedValue<V>> future = managementService.getPrimitiveCache().getPrimitive(name, () ->
            CompletableFuture.completedFuture(((ValueProtocol) protocol).<V>newValueDelegate(name, serializer(), managementService))
                .thenApply(map -> new GossipDistributedValue<>(name, protocol, map)));
        return future.thenApply(AsyncDistributedValue::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Values are not supported by the provided gossip protocol"));
      }
    } else {
      return newProxy(AtomicValueService.class, new ServiceConfig())
          .thenCompose(proxy -> new AtomicValueProxy(proxy, managementService.getPrimitiveRegistry()).connect())
          .thenApply(elector -> {
            Serializer serializer = serializer();
            return new TranscodingAsyncAtomicValue<V, byte[]>(
                elector,
                key -> serializer.encode(key),
                bytes -> serializer.decode(bytes));
          })
          .thenApply(value -> new DelegatingAsyncDistributedValue<>(value).sync());
    }
  }
}
