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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Partitioned resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionedResourceConfig extends ResourceConfig {
  private Partitioner partitioner;
  private final List<Partition> partitions = new ArrayList<>(128);

  /**
   * Sets the resource partitioner.
   *
   * @param partitioner The resource partitioner.
   */
  protected void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

  /**
   * Returns the resource partitioner.
   *
   * @return The resource partitioner.
   */
  public Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * Sets all partitions.
   *
   * @param partitions The set of partitions.
   */
  protected <T extends Partition<?>> void setPartitions(Collection<T> partitions) {
    this.partitions.clear();
    if (partitions != null)
      this.partitions.addAll(partitions);
  }

  /**
   * Adds a partition to the resource.
   *
   * @param partition The partition to add.
   */
  protected <T extends Partition<?>> void addPartition(T partition) {
    if (partition != null)
      partitions.add(partition);
  }

  /**
   * Returns the resource partitions.
   *
   * @param <T> The resource partition type.
   * @return A list of resource partitions.
   */
  @SuppressWarnings("unchecked")
  public <T extends Partition<?>> List<T> getPartitions() {
    return (List) partitions;
  }

  @Override
  protected PartitionedResourceConfig resolve() {
    if (partitioner == null)
      partitioner = new HashPartitioner();
    if (partitions.isEmpty())
      throw new ConfigurationException("no partitions provided");
    super.resolve();
    return this;
  }

}
