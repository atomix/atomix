// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.map.AtomicMapType;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Transactional map builder.
 */
public abstract class TransactionalMapBuilder<K, V>
    extends PrimitiveBuilder<TransactionalMapBuilder<K, V>, TransactionalMapConfig, TransactionalMap<K, V>>
    implements ProxyCompatibleBuilder<TransactionalMapBuilder<K, V>> {

  protected TransactionalMapBuilder(String name, TransactionalMapConfig config, PrimitiveManagementService managementService) {
    super(AtomicMapType.instance(), name, config, managementService);
  }

  @Override
  public TransactionalMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
