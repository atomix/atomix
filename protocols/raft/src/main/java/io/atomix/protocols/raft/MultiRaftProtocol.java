/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.PartitionedPrimitiveProxy;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Multi-Raft protocol.
 */
public class MultiRaftProtocol extends RaftProtocol {
  public static final Type TYPE = new Type();

  /**
   * The multi-raft protocol type.
   */
  public static class Type implements PrimitiveProtocol.Type {
    private static final String NAME = "multi-raft";

    @Override
    public String name() {
      return NAME;
    }
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @return a new multi-Raft protocol builder
   */
  public static Builder builder() {
    return new Builder(new MultiRaftProtocolConfig());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @param group the partition group
   * @return the multi-Raft protocol builder
   */
  public static Builder builder(String group) {
    return new Builder(new MultiRaftProtocolConfig().setGroup(group));
  }

  private final MultiRaftProtocolConfig config;

  protected MultiRaftProtocol(MultiRaftProtocolConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, RaftPartitionGroup partitionGroup) {
    List<PartitionProxy> partitions = new ArrayList<>();
    for (RaftPartition partition : partitionGroup.getPartitions()) {
      PartitionProxy proxy = partition.getProxyClient().proxyBuilder(primitiveName, primitiveType)
          .withMinTimeout(config.getMinTimeout())
          .withMaxTimeout(config.getMaxTimeout())
          .withReadConsistency(config.getReadConsistency())
          .withCommunicationStrategy(config.getCommunicationStrategy())
          .withRecoveryStrategy(config.getRecoveryStrategy())
          .withMaxRetries(config.getMaxRetries())
          .withRetryDelay(config.getRetryDelay())
          .withExecutor(config.getExecutor())
          .build();
      partitions.add(proxy);
    }
    return new PartitionedPrimitiveProxy(primitiveName, primitiveType, partitions, config.getPartitioner());
  }

  /**
   * Multi-Raft protocol builder.
   */
  public static class Builder extends RaftProtocol.Builder {
    private final MultiRaftProtocolConfig config;

    protected Builder(MultiRaftProtocolConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public Builder withMinTimeout(Duration minTimeout) {
      super.withMinTimeout(minTimeout);
      return this;
    }

    @Override
    public Builder withMaxTimeout(Duration maxTimeout) {
      super.withMaxTimeout(maxTimeout);
      return this;
    }

    @Override
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      super.withReadConsistency(readConsistency);
      return this;
    }

    @Override
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      super.withCommunicationStrategy(communicationStrategy);
      return this;
    }

    @Override
    public Builder withRecoveryStrategy(Recovery recoveryStrategy) {
      super.withRecoveryStrategy(recoveryStrategy);
      return this;
    }

    @Override
    public Builder withMaxRetries(int maxRetries) {
      super.withMaxRetries(maxRetries);
      return this;
    }

    @Override
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      super.withRetryDelayMillis(retryDelayMillis);
      return this;
    }

    @Override
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      super.withRetryDelay(retryDelay, timeUnit);
      return this;
    }

    @Override
    public Builder withRetryDelay(Duration retryDelay) {
      super.withRetryDelay(retryDelay);
      return this;
    }

    @Override
    public Builder withExecutor(Executor executor) {
      super.withExecutor(executor);
      return this;
    }

    @Override
    public MultiRaftProtocol build() {
      return new MultiRaftProtocol(config);
    }
  }
}
