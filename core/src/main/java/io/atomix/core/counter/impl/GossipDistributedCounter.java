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
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.counter.CounterProtocol;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Gossip distributed counter.
 */
public class GossipDistributedCounter implements AsyncDistributedCounter {
  private final String name;
  private final GossipProtocol protocol;
  private final CounterProtocol counter;

  public GossipDistributedCounter(String name, GossipProtocol protocol, CounterProtocol counter) {
    this.name = name;
    this.protocol = protocol;
    this.counter = counter;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return DistributedCounterType.instance();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return protocol;
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return counter.increment();
  }

  @Override
  public CompletableFuture<Long> get() {
    return counter.get();
  }

  @Override
  public DistributedCounter sync(Duration operationTimeout) {
    return new BlockingDistributedCounter(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Void> close() {
    return counter.close();
  }
}
