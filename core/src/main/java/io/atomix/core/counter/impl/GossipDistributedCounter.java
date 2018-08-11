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
import io.atomix.primitive.protocol.counter.CounterDelegate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Gossip distributed counter.
 */
public class GossipDistributedCounter implements AsyncDistributedCounter {
  private final String name;
  private final GossipProtocol protocol;
  private final CounterDelegate counter;

  public GossipDistributedCounter(String name, GossipProtocol protocol, CounterDelegate counter) {
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
    return CompletableFuture.completedFuture(counter.incrementAndGet());
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return CompletableFuture.completedFuture(counter.decrementAndGet());
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return CompletableFuture.completedFuture(counter.getAndIncrement());
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return CompletableFuture.completedFuture(counter.getAndDecrement());
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return CompletableFuture.completedFuture(counter.getAndAdd(delta));
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return CompletableFuture.completedFuture(counter.addAndGet(delta));
  }

  @Override
  public CompletableFuture<Long> get() {
    return CompletableFuture.completedFuture(counter.get());
  }

  @Override
  public DistributedCounter sync(Duration operationTimeout) {
    return new BlockingDistributedCounter(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Void> close() {
    counter.close();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }
}
