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
 * Builder for AtomicIdGenerator.
 */
public abstract class AtomicLockBuilder
    extends PrimitiveBuilder<AtomicLockBuilder, AtomicLockConfig, AtomicLock>
    implements ProxyCompatibleBuilder<AtomicLockBuilder> {

  protected AtomicLockBuilder(String name, AtomicLockConfig config, PrimitiveManagementService managementService) {
    super(AtomicLockType.instance(), name, config, managementService);
  }

  @Override
  public AtomicLockBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
