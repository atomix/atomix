// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.map.AtomicNavigableMapBuilder;
import io.atomix.core.map.AtomicNavigableMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link io.atomix.core.map.AsyncAtomicNavigableMap} builder.
 *
 * @param <V> type for map value
 */
public class DefaultAtomicNavigableMapBuilder<K extends Comparable<K>, V> extends AtomicNavigableMapBuilder<K, V> {
  public DefaultAtomicNavigableMapBuilder(String name, AtomicNavigableMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicNavigableMap<K, V>> buildAsync() {
    return newProxy(AtomicTreeMapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicNavigableMapProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(map -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicNavigableMap<K, V, byte[]>(
              (AsyncAtomicNavigableMap) map,
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
