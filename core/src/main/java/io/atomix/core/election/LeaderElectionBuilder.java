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
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder for constructing new {@link AsyncLeaderElection} instances.
 */
public abstract class LeaderElectionBuilder<T>
    extends DistributedPrimitiveBuilder<LeaderElectionBuilder<T>, LeaderElectionConfig, LeaderElection<T>> {

  private Duration electionTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);
  private Serializer serializer;

  public LeaderElectionBuilder(String name, LeaderElectionConfig config) {
    super(PrimitiveTypes.leaderElection(), name, config);
  }

  /**
   * Sets the election timeout in milliseconds.
   *
   * @param electionTimeoutMillis the election timeout in milliseconds
   * @return leader elector builder
   */
  public LeaderElectionBuilder<T> withElectionTimeout(long electionTimeoutMillis) {
    return withElectionTimeout(Duration.ofMillis(electionTimeoutMillis));
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout the election timeout
   * @param timeUnit        the timeout time unit
   * @return leader elector builder
   */
  public LeaderElectionBuilder<T> withElectionTimeout(long electionTimeout, TimeUnit timeUnit) {
    return withElectionTimeout(Duration.ofMillis(timeUnit.toMillis(electionTimeout)));
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout the election timeout
   * @return leader elector builder
   */
  public LeaderElectionBuilder<T> withElectionTimeout(Duration electionTimeout) {
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
  public LeaderElectionBuilder<T> withSerializer(Serializer serializer) {
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
}
