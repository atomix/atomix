/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.lock;

import io.atomix.core.PrimitiveTypes;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Replication;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder for AtomicIdGenerator.
 */
public abstract class DistributedLockBuilder
    extends DistributedPrimitiveBuilder<DistributedLockBuilder, DistributedLock> {

  private Duration lockTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);

  public DistributedLockBuilder(String name) {
    super(PrimitiveTypes.lock(), name);
  }

  /**
   * Sets the lock timeout in milliseconds.
   *
   * @param lockTimeoutMillis the lock timeout in milliseconds
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(long lockTimeoutMillis) {
    return withLockTimeout(Duration.ofMillis(lockTimeoutMillis));
  }

  /**
   * Sets the lock timeout.
   *
   * @param lockTimeout the lock timeout
   * @param timeUnit    the timeout time unit
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(long lockTimeout, TimeUnit timeUnit) {
    return withLockTimeout(Duration.ofMillis(timeUnit.toMillis(lockTimeout)));
  }

  /**
   * Sets the lock timeout.
   *
   * @param lockTimeout the lock timeout
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(Duration lockTimeout) {
    this.lockTimeout = checkNotNull(lockTimeout);
    return this;
  }

  /**
   * Returns the lock timeout.
   *
   * @return the lock timeout
   */
  public Duration lockTimeout() {
    return lockTimeout;
  }

  @Override
  protected Consistency defaultConsistency() {
    return Consistency.LINEARIZABLE;
  }

  @Override
  protected Persistence defaultPersistence() {
    return Persistence.PERSISTENT;
  }

  @Override
  protected Replication defaultReplication() {
    return Replication.SYNCHRONOUS;
  }

  @Override
  public PrimitiveProtocol protocol() {
    PrimitiveProtocol protocol = super.protocol();
    if (protocol != null) {
      return protocol;
    }
    return newRaftProtocol(consistency());
  }

  private PrimitiveProtocol newRaftProtocol(Consistency readConsistency) {
    return RaftProtocol.builder()
        .withMinTimeout(lockTimeout)
        .withMaxTimeout(Duration.ofSeconds(5))
        .withReadConsistency(readConsistency == Consistency.LINEARIZABLE
            ? ReadConsistency.LINEARIZABLE
            : ReadConsistency.SEQUENTIAL)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withRecoveryStrategy(recovery())
        .withMaxRetries(maxRetries())
        .withRetryDelay(retryDelay())
        .build();
  }
}