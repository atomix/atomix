/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource;

/**
 * Partitioned resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PartitionedResourceConfig<T extends PartitionedResourceConfig<T>> extends ResourceConfig<T> {
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = -1;

  private int partitions = DEFAULT_PARTITIONS;
  private int replicationFactor = DEFAULT_REPLICATION_FACTOR;

  public PartitionedResourceConfig() {
  }

  public PartitionedResourceConfig(ResourceConfig<?> config) {
    super(config);
  }

  public PartitionedResourceConfig(PartitionedResourceConfig<?> config) {
    super(config);
    partitions = config.partitions;
    replicationFactor = config.getReplicationFactor();
  }

  /**
   * Sets the number of resource partitions.
   *
   * @param partitions The number of resource partitions.
   * @throws java.lang.IllegalArgumentException If the number of partitions is not positive
   */
  public void setPartitions(int partitions) {
    if (partitions <= 0)
      throw new IllegalArgumentException("number of partitions must be positive");
    this.partitions = partitions;
  }

  /**
   * Returns the number of resource partitions.
   *
   * @return The number of resource partitions.
   */
  public int getPartitions() {
    return partitions;
  }

  /**
   * Sets the number of resource partitions, returning the configuration for method chaining.
   *
   * @param partitions The number of resource partitions.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the number of partitions is not positive
   */
  @SuppressWarnings("unchecked")
  public T withPartitions(int partitions) {
    setPartitions(partitions);
    return (T) this;
  }

  /**
   * Sets the resource replication factor.
   *
   * @param replicationFactor The resource replication factor.
   * @throws java.lang.IllegalArgumentException If the replication factor is less than {@code -1}
   */
  public void setReplicationFactor(int replicationFactor) {
    if (replicationFactor < 1 && replicationFactor != -1)
      throw new IllegalArgumentException("replication factor must be positive or -1");
    this.replicationFactor = replicationFactor;
  }

  /**
   * Returns the resource replication factor.
   *
   * @return The resource replication factor. Defaults to {@code -1}
   */
  public int getReplicationFactor() {
    return replicationFactor;
  }

  /**
   * Sets the resource replication factor, returning the configuration for method chaining.
   *
   * @param replicationFactor The resource replication factor.
   * @return The resource configuration.
   * @throws java.lang.IllegalArgumentException If the replication factor is less than {@code -1}
   */
  @SuppressWarnings("unchecked")
  public T withReplicationFactor(int replicationFactor) {
    setReplicationFactor(replicationFactor);
    return (T) this;
  }

}
