// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicSortedMap;
import io.atomix.core.map.AtomicSortedMap;
import io.atomix.core.map.AtomicSortedMapBuilder;
import io.atomix.core.map.AtomicSortedMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncAtomicSortedMap} builder.
 *
 * @param <V> type for map value
 */
public class DefaultAtomicSortedMapBuilder<K extends Comparable<K>, V> extends AtomicSortedMapBuilder<K, V> {
  public DefaultAtomicSortedMapBuilder(String name, AtomicSortedMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicSortedMap<K, V>> buildAsync() {
    return newProxy(AtomicTreeMapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicNavigableMapProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(map -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicSortedMap<K, V, byte[]>(
              (AsyncAtomicSortedMap) map,
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
