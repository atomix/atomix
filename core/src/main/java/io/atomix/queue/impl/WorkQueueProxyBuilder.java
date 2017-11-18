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
package io.atomix.queue.impl;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.queue.AsyncWorkQueue;
import io.atomix.queue.WorkQueueBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default work queue builder implementation.
 */
public class WorkQueueProxyBuilder<E> extends WorkQueueBuilder<E> {
  private final PrimitiveManagementService managementService;

  public WorkQueueProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncWorkQueue<E> newWorkQueue(PrimitiveProxy proxy) {
    WorkQueueProxy workQueue = new WorkQueueProxy(proxy.open().join());
    return new TranscodingAsyncWorkQueue<>(workQueue, serializer()::encode, serializer()::decode);
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncWorkQueue<E> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return newWorkQueue(managementService.getPartitionService()
        .getPartitionGroup(protocol)
        .getPartition(name())
        .getPrimitiveClient()
        .proxyBuilder(name(), primitiveType(), protocol)
        .build());
  }
}
