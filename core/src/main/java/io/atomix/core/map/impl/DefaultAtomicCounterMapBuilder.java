// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicCounterMapBuilder;
import io.atomix.core.map.AtomicCounterMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@code AtomicCounterMapBuilder}.
 */
public class DefaultAtomicCounterMapBuilder<K> extends AtomicCounterMapBuilder<K> {
  public DefaultAtomicCounterMapBuilder(String name, AtomicCounterMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicCounterMap<K>> buildAsync() {
    return newProxy(AtomicCounterMapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicCounterMapProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(map -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicCounterMap<K, String>(
              map,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)))
              .sync();
        });
  }
}
