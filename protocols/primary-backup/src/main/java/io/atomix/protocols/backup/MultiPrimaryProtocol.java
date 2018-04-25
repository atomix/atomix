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
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.PartitionedPrimitiveProxy;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol extends PrimaryBackupProtocol {
  public static final Type TYPE = new Type();

  /**
   * The multi-primary protocol type.
   */
  public static class Type implements PrimitiveProtocol.Type {
    private static final String NAME = "multi-primary";

    @Override
    public String name() {
      return NAME;
    }
  }

  /**
   * Returns a new multi-primary protocol builder.
   *
   * @return a new multi-primary protocol builder
   */
  public static Builder builder() {
    return new Builder(new MultiPrimaryProtocolConfig());
  }

  /**
   * Returns a new multi-primary protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new multi-primary protocol builder for the given group
   */
  public static Builder builder(String group) {
    return new Builder(new MultiPrimaryProtocolConfig().setGroup(group));
  }

  private final MultiPrimaryProtocolConfig config;

  protected MultiPrimaryProtocol(MultiPrimaryProtocolConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, PrimaryBackupPartitionGroup partitionGroup) {
    List<PartitionProxy> partitions = new ArrayList<>();
    for (PrimaryBackupPartition partition : partitionGroup.getPartitions()) {
      PartitionProxy proxy = partition.getProxyClient().proxyBuilder(primitiveName, primitiveType)
          .withConsistency(config.getConsistency())
          .withReplication(config.getReplication())
          .withRecovery(config.getRecovery())
          .withNumBackups(config.getNumBackups())
          .withMaxRetries(config.getMaxRetries())
          .withRetryDelay(config.getRetryDelay())
          .withExecutor(config.getExecutor())
          .build();
      partitions.add(proxy);
    }
    return new PartitionedPrimitiveProxy(primitiveName, primitiveType, partitions, config.getPartitioner());
  }

  /**
   * Primary-backup protocol builder.
   */
  public static class Builder extends PrimaryBackupProtocol.Builder {
    private final MultiPrimaryProtocolConfig config;

    protected Builder(MultiPrimaryProtocolConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public Builder withConsistency(Consistency consistency) {
      config.setConsistency(consistency);
      return this;
    }

    @Override
    public Builder withReplication(Replication replication) {
      config.setReplication(replication);
      return this;
    }

    @Override
    public Builder withRecovery(Recovery recovery) {
      config.setRecovery(recovery);
      return this;
    }

    @Override
    public Builder withBackups(int numBackups) {
      config.setNumBackups(numBackups);
      return this;
    }

    @Override
    public Builder withMaxRetries(int maxRetries) {
      config.setMaxRetries(maxRetries);
      return this;
    }

    @Override
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      config.setRetryDelayMillis(retryDelayMillis);
      return this;
    }

    @Override
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    @Override
    public Builder withRetryDelay(Duration retryDelay) {
      config.setRetryDelay(retryDelay);
      return this;
    }

    @Override
    public Builder withExecutor(Executor executor) {
      config.setExecutor(executor);
      return this;
    }

    /**
     * Sets the partitioner.
     *
     * @param partitioner the partitioner
     * @return the protocol builder
     */
    public Builder withPartitioner(Partitioner<String> partitioner) {
      config.setPartitioner(partitioner);
      return this;
    }

    @Override
    public MultiPrimaryProtocol build() {
      return new MultiPrimaryProtocol(config);
    }
  }
}
