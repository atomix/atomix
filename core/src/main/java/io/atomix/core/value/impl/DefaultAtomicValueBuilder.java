// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of AtomicValueBuilder.
 *
 * @param <V> value type
 */
public class DefaultAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
  public DefaultAtomicValueBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicValue<V>> buildAsync() {
    return newProxy(AtomicValueService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicValueProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(elector -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicValue<V, byte[]>(
              elector,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
