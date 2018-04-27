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
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.PartitionedPrimitiveProxy;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol implements PrimitiveProtocol {
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

  protected final MultiPrimaryProtocolConfig config;

  protected MultiPrimaryProtocol(MultiPrimaryProtocolConfig config) {
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  @Override
  public PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, PartitionService partitionService) {
    Collection<PartitionProxy> partitions = partitionService.<PrimaryBackupPartition>getPartitionGroup(this)
        .getPartitions()
        .stream()
        .map(partition -> partition.getProxyClient().proxyBuilder(primitiveName, primitiveType)
            .withConsistency(config.getConsistency())
            .withReplication(config.getReplication())
            .withRecovery(config.getRecovery())
            .withNumBackups(config.getBackups())
            .withMaxRetries(config.getMaxRetries())
            .withRetryDelay(config.getRetryDelay())
            .withExecutor(config.getExecutor())
            .build())
        .collect(Collectors.toList());
    return new PartitionedPrimitiveProxy(primitiveName, primitiveType, partitions, config.getPartitioner());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("group", group())
        .toString();
  }

  /**
   * Multi-primary protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<MultiPrimaryProtocolConfig, MultiPrimaryProtocol> {
    protected Builder(MultiPrimaryProtocolConfig config) {
      super(config);
    }

    /**
     * Sets the protocol partitioner.
     *
     * @param partitioner the protocol partitioner
     * @return the protocol builder
     */
    public Builder withPartitioner(Partitioner<String> partitioner) {
      config.setPartitioner(partitioner);
      return this;
    }

    /**
     * Sets the protocol consistency model.
     *
     * @param consistency the protocol consistency model
     * @return the protocol builder
     */
    public Builder withConsistency(Consistency consistency) {
      config.setConsistency(consistency);
      return this;
    }

    /**
     * Sets the protocol replication strategy.
     *
     * @param replication the protocol replication strategy
     * @return the protocol builder
     */
    public Builder withReplication(Replication replication) {
      config.setReplication(replication);
      return this;
    }

    /**
     * Sets the protocol recovery strategy.
     *
     * @param recovery the protocol recovery strategy
     * @return the protocol builder
     */
    public Builder withRecovery(Recovery recovery) {
      config.setRecovery(recovery);
      return this;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol builder
     */
    public Builder withBackups(int numBackups) {
      config.setBackups(numBackups);
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      config.setMaxRetries(maxRetries);
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      config.setRetryDelayMillis(retryDelayMillis);
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit   the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      config.setRetryDelay(retryDelay);
      return this;
    }

    /**
     * Sets the executor with which to complete proxy futures.
     *
     * @param executor The executor with which to complete proxy futures.
     * @return The proxy builder.
     * @throws NullPointerException if the executor is null
     */
    public Builder withExecutor(Executor executor) {
      config.setExecutor(executor);
      return this;
    }

    @Override
    public MultiPrimaryProtocol build() {
      return new MultiPrimaryProtocol(config);
    }
  }
}
