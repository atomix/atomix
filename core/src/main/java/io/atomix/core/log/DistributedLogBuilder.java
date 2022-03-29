// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.LogCompatibleBuilder;
import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;

/**
 * Builder for DistributedLog.
 */
public abstract class DistributedLogBuilder<E>
    extends PrimitiveBuilder<DistributedLogBuilder<E>, DistributedLogConfig, DistributedLog<E>>
    implements LogCompatibleBuilder<DistributedLogBuilder<E>> {

  protected DistributedLogBuilder(String name, DistributedLogConfig config, PrimitiveManagementService managementService) {
    super(DistributedLogType.instance(), name, config, managementService);
  }

  @Override
  public DistributedLogBuilder<E> withProtocol(LogProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
