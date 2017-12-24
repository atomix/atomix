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
package io.atomix.core.value;

import io.atomix.core.PrimitiveTypes;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Replication;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;

/**
 * Builder for constructing new AtomicValue instances.
 *
 * @param <V> atomic value type
 */
public abstract class AtomicValueBuilder<V>
    extends DistributedPrimitiveBuilder<AtomicValueBuilder<V>, AtomicValue<V>> {

  public AtomicValueBuilder(String name) {
    super(PrimitiveTypes.value(), name);
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

    switch (consistency()) {
      case LINEARIZABLE:
        switch (persistence()) {
          case PERSISTENT:
            return newRaftProtocol(Consistency.LINEARIZABLE);
          case EPHEMERAL:
            return newMultiPrimaryProtocol(Consistency.LINEARIZABLE, Replication.SYNCHRONOUS);
        }
      case SEQUENTIAL:
      case EVENTUAL:
        switch (persistence()) {
          case PERSISTENT:
            return newRaftProtocol(Consistency.SEQUENTIAL);
          case EPHEMERAL:
            return newMultiPrimaryProtocol(Consistency.SEQUENTIAL, Replication.SYNCHRONOUS);
          default:
            throw new AssertionError();
        }
      default:
        throw new AssertionError();
    }
  }

  private PrimitiveProtocol newRaftProtocol(Consistency readConsistency) {
    return RaftProtocol.builder()
        .withMinTimeout(Duration.ofSeconds(5))
        .withMaxTimeout(Duration.ofSeconds(30))
        .withReadConsistency(readConsistency == Consistency.LINEARIZABLE
            ? ReadConsistency.LINEARIZABLE_LEASE
            : ReadConsistency.SEQUENTIAL)
        .withCommunicationStrategy(readConsistency == Consistency.SEQUENTIAL
            ? CommunicationStrategy.FOLLOWERS
            : CommunicationStrategy.LEADER)
        .withRecoveryStrategy(recovery())
        .withMaxRetries(maxRetries())
        .withRetryDelay(retryDelay())
        .build();
  }

  private PrimitiveProtocol newMultiPrimaryProtocol(Consistency consistency, Replication replication) {
    return MultiPrimaryProtocol.builder()
        .withConsistency(consistency)
        .withReplication(replication)
        .withRecovery(recovery())
        .withBackups(backups())
        .withMaxRetries(maxRetries())
        .withRetryDelay(retryDelay())
        .build();
  }
}
