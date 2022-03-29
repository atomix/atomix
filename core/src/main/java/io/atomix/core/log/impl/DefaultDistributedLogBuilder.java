// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
