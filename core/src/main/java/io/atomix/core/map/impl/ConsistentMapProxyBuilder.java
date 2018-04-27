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
package io.atomix.core.map.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.map.ConsistentMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link AsyncConsistentMap} builder.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public class ConsistentMapProxyBuilder<K, V> extends ConsistentMapBuilder<K, V> {
  public ConsistentMapProxyBuilder(String name, ConsistentMapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ConsistentMap<K, V>> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
        name(),
        primitiveType(),
        managementService.getPartitionService());
    return new ConsistentMapProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(rawMap -> {
          Serializer serializer = serializer();
          AsyncConsistentMap<K, V> map = new TranscodingAsyncConsistentMap<K, V, String, byte[]>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));

          if (!config.isNullValues()) {
            map = new NotNullAsyncConsistentMap<>(map);
          }

          if (config.isCacheEnabled()) {
            map = new CachingAsyncConsistentMap<>(map, config.getCacheSize());
          }

          if (config.isReadOnly()) {
            map = new UnmodifiableAsyncConsistentMap<>(map);
          }
          return map.sync();
        });
  }
}