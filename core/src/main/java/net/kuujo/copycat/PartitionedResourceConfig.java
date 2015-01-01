/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import net.kuujo.copycat.internal.util.Assert;

import java.util.Map;

/**
 * Partitioned resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PartitionedResourceConfig<T extends PartitionedResourceConfig<T>> extends ResourceConfig<T> {
  public static final String RESOURCE_PARTITIONS = "partitions";
  public static final String RESOURCE_REPLICATION_FACTOR = "replication-factor";

  private static final int DEFAULT_RESOURCE_PARTITIONS = 1;
  private static final int DEFAULT_RESOURCE_REPLICATION_FACTOR = -1;

  protected PartitionedResourceConfig() {
    super();
  }

  protected PartitionedResourceConfig(Map<String, Object> config) {
    super(config);
  }

  protected PartitionedResourceConfig(Config config) {
    super(config);
  }

  /**
   * Sets the number of event log partitions.
   *
   * @param partitions The number of event log partitions.
   * @throws java.lang.IllegalArgumentException If the number of partitions is invalid
   */
  public void setPartitions(int partitions) {
    put(RESOURCE_PARTITIONS, Assert.arg(partitions, partitions > 0, "partitions must be positive"));
  }

  /**
   * Returns the number of event log partitions.
   *
   * @return The number of event log partitions.
   */
  public int getPartitions() {
    return get(RESOURCE_PARTITIONS, DEFAULT_RESOURCE_PARTITIONS);
  }

  /**
   * Sets the number of event log partitions, returning the configuration for method chaining.
   *
   * @param partitions The number of event log partitions.
   * @return The event log configuration.
   * @throws java.lang.IllegalArgumentException If the number of partitions is invalid
   */
  @SuppressWarnings("unchecked")
  public T withPartitions(int partitions) {
    setPartitions(partitions);
    return (T) this;
  }

  /**
   * Sets the event log replication factor.
   *
   * @param replicationFactor The event log replication factor.
   * @throws java.lang.IllegalArgumentException If the replication factor is invalid
   */
  public void setReplicationFactor(int replicationFactor) {
    put(RESOURCE_REPLICATION_FACTOR, Assert.arg(replicationFactor, replicationFactor > -2, "replication factor must be -1, 0, or greater than 0"));
  }

  /**
   * Returns the event log replication factor.
   *
   * @return The event log replication factor.
   */
  public int getReplicationFactor() {
    return get(RESOURCE_REPLICATION_FACTOR, DEFAULT_RESOURCE_REPLICATION_FACTOR);
  }

  /**
   * Sets the event log replication factor, returning the configuration for method chaining.
   *
   * @param replicationFactor The event log replication factor.
   * @return The event log configuration.
   * @throws java.lang.IllegalArgumentException If the replication factor is invalid
   */
  @SuppressWarnings("unchecked")
  public T withReplicationFactor(int replicationFactor) {
    setReplicationFactor(replicationFactor);
    return (T) this;
  }

}
