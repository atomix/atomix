/*
 * Copyright 2016 Open Networking Foundation
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

package io.atomix.core.multimap.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multimap.DistributedMultimapBuilder;
import io.atomix.core.multimap.DistributedMultimapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link DistributedMultimap} builder.
 */
public class DefaultDistributedMultimapBuilder<K, V> extends DistributedMultimapBuilder<K, V> {
  public DefaultDistributedMultimapBuilder(String name, DistributedMultimapConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedMultimap<K, V>> buildAsync() {
    return newProxy(AtomicMultimapService.class, new ServiceConfig())
        .thenCompose(proxy -> new AtomicMultimapProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawMultimap -> {
          Serializer serializer = serializer();
          AsyncAtomicMultimap<K, V> multimap = new TranscodingAsyncAtomicMultimap<>(
              rawMultimap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));
          if (config.getCacheConfig().isEnabled()) {
            multimap = new CachingAsyncAtomicMultimap<>(multimap, config.getCacheConfig());
          }
          return multimap;
        }).thenApply(atomicMultimap -> new DelegatingAsyncDistributedMultimap<>(atomicMultimap).sync());
  }
}
