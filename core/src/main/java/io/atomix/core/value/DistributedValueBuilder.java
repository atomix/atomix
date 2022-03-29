// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.value.ValueCompatibleBuilder;
import io.atomix.primitive.protocol.value.ValueProtocol;

/**
 * Builder for constructing new DistributedValue instances.
 *
 * @param <V> atomic value type
 */
public abstract class DistributedValueBuilder<V>
    extends ValueBuilder<DistributedValueBuilder<V>, DistributedValueConfig, DistributedValue<V>, V>
    implements ProxyCompatibleBuilder<DistributedValueBuilder<V>>, ValueCompatibleBuilder<DistributedValueBuilder<V>> {

  protected DistributedValueBuilder(String name, DistributedValueConfig config, PrimitiveManagementService managementService) {
    super(DistributedValueType.instance(), name, config, managementService);
  }

  @Override
  public DistributedValueBuilder<V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedValueBuilder<V> withProtocol(ValueProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
