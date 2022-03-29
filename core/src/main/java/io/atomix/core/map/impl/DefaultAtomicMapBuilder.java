// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncAtomicMap} builder.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public class DefaultAtomicMapBuilder<K, V> extends AtomicMapBuilder<K, V> {
  public DefaultAtomicMapBuilder(String name, AtomicMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicMap<K, V>> buildAsync() {
    return newProxy(AtomicMapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicMapProxy((ProxyClient) proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawMap -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicMap<K, V, String, byte[]>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));
        }).thenApply(map -> {
          if (!config.isNullValues()) {
            return new NotNullAsyncAtomicMap<>(map);
          }
          return map;
        }).thenCompose(map -> {
          if (config.getCacheConfig().isEnabled()) {
            if (config.getCacheConfig().getSize() == -1) {
              return new CachedAsyncAtomicMap<>(map).create();
            } else {
              return CompletableFuture.completedFuture(new CachingAsyncAtomicMap<>(map, config.getCacheConfig()));
            }
          }
          return CompletableFuture.completedFuture(map);
        })
        .thenApply(map -> {
          if (config.isReadOnly()) {
            return new UnmodifiableAsyncAtomicMap<>(map);
          }
          return map;
        })
        .thenApply(AsyncAtomicMap::sync);
  }
}
