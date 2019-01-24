/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
