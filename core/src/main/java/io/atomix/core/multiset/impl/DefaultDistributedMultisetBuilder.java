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
package io.atomix.core.multiset.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.DistributedMultisetBuilder;
import io.atomix.core.multiset.DistributedMultisetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed multiset builder.
 *
 * @param <E> type for multiset elements
 */
public class DefaultDistributedMultisetBuilder<E> extends DistributedMultisetBuilder<E> {
  public DefaultDistributedMultisetBuilder(String name, DistributedMultisetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedMultiset<E>> buildAsync() {
    return newProxy(DistributedMultisetService.class, new ServiceConfig())
        .thenCompose(proxy -> new DistributedMultisetProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawList -> {
          Serializer serializer = serializer();
          AsyncDistributedMultiset<E> list = new TranscodingAsyncDistributedMultiset<>(
              rawList,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));

          if (config.getCacheConfig().isEnabled()) {
            list = new CachingAsyncDistributedMultiset<>(list, config.getCacheConfig());
          }

          if (config.isReadOnly()) {
            list = new UnmodifiableAsyncDistributedMultiset<>(list);
          }
          return list.sync();
        });
  }
}
