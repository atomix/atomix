// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
