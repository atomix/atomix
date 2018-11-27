/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.log.impl;

import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogBuilder;
import io.atomix.core.log.DistributedLogConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.LogProtocol;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed log builder.
 */
public class DefaultDistributedLogBuilder<E> extends DistributedLogBuilder<E> {
  public DefaultDistributedLogBuilder(String name, DistributedLogConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedLog<E>> buildAsync() {
    LogProtocol protocol = (LogProtocol) protocol();
    return protocol.newClient(managementService.getPartitionService())
        .connect()
        .thenApply(client -> new DefaultAsyncDistributedLog<E>(name, client, serializer()).sync());
  }
}
