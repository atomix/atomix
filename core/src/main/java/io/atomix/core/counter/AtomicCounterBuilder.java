// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for AtomicCounter.
 */
public abstract class AtomicCounterBuilder
    extends PrimitiveBuilder<AtomicCounterBuilder, AtomicCounterConfig, AtomicCounter>
    implements ProxyCompatibleBuilder<AtomicCounterBuilder> {

  protected AtomicCounterBuilder(String name, AtomicCounterConfig config, PrimitiveManagementService managementService) {
    super(AtomicCounterType.instance(), name, config, managementService);
  }

  @Override
  public AtomicCounterBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
