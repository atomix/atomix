// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for constructing new AtomicValue instances.
 *
 * @param <V> atomic value type
 */
public abstract class AtomicValueBuilder<V>
    extends ValueBuilder<AtomicValueBuilder<V>, AtomicValueConfig, AtomicValue<V>, V>
    implements ProxyCompatibleBuilder<AtomicValueBuilder<V>> {

  protected AtomicValueBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    super(AtomicValueType.instance(), name, config, managementService);
  }

  @Override
  public AtomicValueBuilder<V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
