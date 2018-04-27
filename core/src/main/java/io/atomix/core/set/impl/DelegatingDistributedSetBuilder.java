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
package io.atomix.core.set.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.impl.CachingAsyncConsistentMap;
import io.atomix.core.map.impl.ConsistentMapProxy;
import io.atomix.core.map.impl.TranscodingAsyncConsistentMap;
import io.atomix.core.map.impl.UnmodifiableAsyncConsistentMap;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DelegatingDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  public DelegatingDistributedSetBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
        name(),
        primitiveType(),
        managementService.getPartitionService());
    return new ConsistentMapProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(rawMap -> {
          Serializer serializer = serializer();
          AsyncConsistentMap<E, Boolean> map = new TranscodingAsyncConsistentMap<>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));

          if (config.isCacheEnabled()) {
            map = new CachingAsyncConsistentMap<>(map, config.getCacheSize());
          }

          if (config.isReadOnly()) {
            map = new UnmodifiableAsyncConsistentMap<>(map);
          }
          return new DelegatingAsyncDistributedSet<>(map).sync();
        });
  }
}
