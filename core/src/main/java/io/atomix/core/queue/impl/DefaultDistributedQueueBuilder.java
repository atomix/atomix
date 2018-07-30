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
package io.atomix.core.queue.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.core.queue.DistributedQueueBuilder;
import io.atomix.core.queue.DistributedQueueConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed queue builder.
 *
 * @param <E> type for queue elements
 */
public class DefaultDistributedQueueBuilder<E> extends DistributedQueueBuilder<E> {
  public DefaultDistributedQueueBuilder(String name, DistributedQueueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedQueue<E>> buildAsync() {
    return newProxy(DistributedQueueService.class, new ServiceConfig())
        .thenCompose(proxy -> new DistributedQueueProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(rawQueue -> {
          Serializer serializer = serializer();
          AsyncDistributedQueue<E> queue = new TranscodingAsyncDistributedQueue<>(
              rawQueue,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));

          if (config.getCacheConfig().isEnabled()) {
            queue = new CachingAsyncDistributedQueue<>(queue, config.getCacheConfig());
          }

          if (config.isReadOnly()) {
            queue = new UnmodifiableAsyncDistributedQueue<>(queue);
          }
          return queue.sync();
        });
  }
}
