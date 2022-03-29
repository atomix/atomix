// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue.impl;

import io.atomix.core.workqueue.WorkQueue;
import io.atomix.core.workqueue.WorkQueueBuilder;
import io.atomix.core.workqueue.WorkQueueConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default work queue builder implementation.
 */
public class DefaultWorkQueueBuilder<E> extends WorkQueueBuilder<E> {
  public DefaultWorkQueueBuilder(String name, WorkQueueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<WorkQueue<E>> buildAsync() {
    return newProxy(WorkQueueService.class, new ServiceConfig())
        .thenCompose(proxy -> new WorkQueueProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(queue -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncWorkQueue<E, byte[]>(
              queue,
              item -> serializer.encode(item),
              bytes -> serializer.decode(bytes))
              .sync();
        });
  }
}
