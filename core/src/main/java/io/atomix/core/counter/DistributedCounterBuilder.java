// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.counter.CounterCompatibleBuilder;
import io.atomix.primitive.protocol.counter.CounterProtocol;

/**
 * Builder for DistributedCounter.
 */
public abstract class DistributedCounterBuilder
    extends PrimitiveBuilder<DistributedCounterBuilder, DistributedCounterConfig, DistributedCounter>
    implements ProxyCompatibleBuilder<DistributedCounterBuilder>, CounterCompatibleBuilder<DistributedCounterBuilder> {

  protected DistributedCounterBuilder(String name, DistributedCounterConfig config, PrimitiveManagementService managementService) {
    super(DistributedCounterType.instance(), name, config, managementService);
  }

  @Override
  public DistributedCounterBuilder withProtocol(CounterProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedCounterBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
