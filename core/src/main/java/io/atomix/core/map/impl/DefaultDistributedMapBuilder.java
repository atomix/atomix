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

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.DistributedMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed map builder.
 */
public class DefaultDistributedMapBuilder<K, V> extends DistributedMapBuilder<K, V> {
  public DefaultDistributedMapBuilder(String name, DistributedMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedMap<K, V>> buildAsync() {
    ProxyClient proxy = protocol().newProxy(
        name(),
        primitiveType(),
        AtomicMapService.class,
        new ServiceConfig(),
        managementService.getPartitionService());
    return new AtomicMapProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(rawMap -> {
          Serializer serializer = serializer();
          AsyncAtomicMap<K, V> map = new TranscodingAsyncAtomicMap<K, V, String, byte[]>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));

          if (!config.isNullValues()) {
            map = new NotNullAsyncAtomicMap<>(map);
          }

          if (config.getCacheConfig().isEnabled()) {
            map = new CachingAsyncAtomicMap<>(map, config.getCacheConfig());
          }

          if (config.isReadOnly()) {
            map = new UnmodifiableAsyncAtomicMap<>(map);
          }
          return map;
        }).thenApply(atomicMap -> new DelegatingAsyncDistributedMap<>(atomicMap).sync());
  }
}
