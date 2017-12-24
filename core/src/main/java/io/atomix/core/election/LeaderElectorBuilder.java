/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.election;

import io.atomix.cluster.NodeId;
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
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder for constructing new {@link AsyncLeaderElector} instances.
 */
public abstract class LeaderElectorBuilder<T>
    extends DistributedPrimitiveBuilder<LeaderElectorBuilder<T>, LeaderElector<T>> {

  private Duration electionTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);
  private Serializer serializer;

  public LeaderElectorBuilder(String name) {
    super(PrimitiveTypes.leaderElector(), name);
  }

  /**
   * Sets the election timeout in milliseconds.
   *
   * @param electionTimeoutMillis the election timeout in milliseconds
   * @return leader elector builder
   */
  public LeaderElectorBuilder<T> withElectionTimeout(long electionTimeoutMillis) {
    return withElectionTimeout(Duration.ofMillis(electionTimeoutMillis));
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout the election timeout
   * @param timeUnit        the timeout time unit
   * @return leader elector builder
   */
  public LeaderElectorBuilder<T> withElectionTimeout(long electionTimeout, TimeUnit timeUnit) {
    return withElectionTimeout(Duration.ofMillis(timeUnit.toMillis(electionTimeout)));
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout the election timeout
   * @return leader elector builder
   */
  public LeaderElectorBuilder<T> withElectionTimeout(Duration electionTimeout) {
    this.electionTimeout = checkNotNull(electionTimeout);
    return this;
  }

  /**
   * Returns the election timeout.
   *
   * @return the election timeout
   */
  public Duration electionTimeout() {
    return electionTimeout;
  }

  @Override
  public LeaderElectorBuilder<T> withSerializer(Serializer serializer) {
    this.serializer = serializer;
    return this;
  }

  @Override
  public Serializer serializer() {
    if (serializer == null) {
      serializer = Serializer.using(KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .register(NodeId.class)
          .build());
    }
    return serializer;
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
        .withMinTimeout(electionTimeout)
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
