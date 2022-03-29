// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
