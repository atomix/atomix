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
