/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.queue.impl;

import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueBuilder;
import io.atomix.core.queue.WorkQueueConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default work queue builder implementation.
 */
public class WorkQueueProxyBuilder<E> extends WorkQueueBuilder<E> {
  public WorkQueueProxyBuilder(String name, WorkQueueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<WorkQueue<E>> buildAsync() {
    PrimitiveProxy proxy = protocol().newProxy(
        name(),
        primitiveType(),
        managementService.getPartitionService());
    return new WorkQueueProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
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
