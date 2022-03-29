// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for DistributedLock.
 */
public abstract class DistributedLockBuilder
    extends PrimitiveBuilder<DistributedLockBuilder, DistributedLockConfig, DistributedLock>
    implements ProxyCompatibleBuilder<DistributedLockBuilder> {

  protected DistributedLockBuilder(String name, DistributedLockConfig config, PrimitiveManagementService managementService) {
    super(DistributedLockType.instance(), name, config, managementService);
  }

  @Override
  public DistributedLockBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
