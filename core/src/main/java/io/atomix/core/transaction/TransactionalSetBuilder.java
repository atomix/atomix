// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.set.DistributedSetType;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Transactional set builder.
 */
public abstract class TransactionalSetBuilder<E>
    extends PrimitiveBuilder<TransactionalSetBuilder<E>, TransactionalSetConfig, TransactionalSet<E>>
    implements ProxyCompatibleBuilder<TransactionalSetBuilder<E>> {

  protected TransactionalSetBuilder(String name, TransactionalSetConfig config, PrimitiveManagementService managementService) {
    super(DistributedSetType.instance(), name, config, managementService);
  }

  @Override
  public TransactionalSetBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
