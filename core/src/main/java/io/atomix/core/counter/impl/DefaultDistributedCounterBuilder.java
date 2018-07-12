/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.counter.impl;

import io.atomix.core.counter.AsyncDistributedCounter;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.counter.DistributedCounterBuilder;
import io.atomix.core.counter.DistributedCounterConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.counter.CounterProtocol;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.config.ConfigurationException;

import java.util.concurrent.CompletableFuture;

/**
 * Default distributed counter builder.
 */
public class DefaultDistributedCounterBuilder extends DistributedCounterBuilder {
  public DefaultDistributedCounterBuilder(String name, DistributedCounterConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedCounter> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    if (protocol instanceof GossipProtocol) {
      if (protocol instanceof CounterProtocol) {
        return managementService.getPrimitiveCache().getPrimitive(name, () -> CompletableFuture.completedFuture(
            new GossipDistributedCounter(name, (GossipProtocol) protocol, ((CounterProtocol) protocol)
                .newCounterDelegate(name, managementService))))
            .thenApply(AsyncDistributedCounter::sync);
      } else {
        return Futures.exceptionalFuture(new UnsupportedOperationException("Counter is not supported by the provided gossip protocol"));
      }
    } else if (protocol instanceof ProxyProtocol) {
      return newProxy(AtomicCounterService.class, new ServiceConfig())
          .thenCompose(proxy -> new AtomicCounterProxy(proxy, managementService.getPrimitiveRegistry()).connect())
          .thenApply(DelegatingDistributedCounter::new)
          .thenApply(AsyncDistributedCounter::sync);
    } else {
      return Futures.exceptionalFuture(new ConfigurationException("Invalid protocol type"));
    }
  }
}
