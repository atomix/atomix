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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Partitioned resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PartitionedResource<T extends PartitionedResource<T, U, V>, U extends Partition<?>, V extends Resource<?>> extends AbstractResource<V> {
  protected final Partitioner partitioner;
  protected final List<U> partitions;

  protected PartitionedResource(PartitionedResourceConfig config, List<U> partitions) {
    super(config);
    if (partitions == null)
      throw new NullPointerException("partitions cannot be null");
    this.partitioner = config.resolve().getPartitioner();
    this.partitions = partitions;
    partitions.forEach(p -> p.init(config));
  }

  /**
   * Returns the partition for the given key.
   *
   * @param key The key for which to return the partition.
   * @return The partition for the given key.
   */
  protected U partition(Object key) {
    return partitions.get(partitioner.partition(key, partitions.size()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> open() {
    return super.open().thenCompose(v -> {
      CompletableFuture[] futures = new CompletableFuture[partitions.size()];
      for (int i = 0; i < partitions.size(); i++) {
        futures[i] = partitions.get(i).open();
      }
      return CompletableFuture.allOf(futures);
    }).thenApply(v -> (V) this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    CompletableFuture[] futures = new CompletableFuture[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      futures[i] = partitions.get(i).open();
    }
    return CompletableFuture.allOf(futures)
      .thenCompose(v -> super.close());
  }

  /**
   * Partitioned resource builder.
   *
   * @param <T> The resource builder type.
   * @param <U> The resource type.
   */
  public static abstract class Builder<T extends Builder<T, U, V, W>, U extends PartitionedResource<U, V, ?>, V extends Partition<?>, W extends Resource<?>> extends Resource.Builder<T, U> {
    protected final PartitionedResourceConfig config;

    protected Builder(PartitionedResourceConfig config) {
      super(config);
      this.config = config;
    }

    /**
     * Sets the resource partitioner.
     *
     * @param partitioner The resource partitioner.
     * @return The partitioned resource builder.
     */
    @SuppressWarnings("unchecked")
    public T withPartitioner(Partitioner partitioner) {
      config.setPartitioner(partitioner);
      return (T) this;
    }

    /**
     * Sets the resource partitions.
     *
     * @param partitions The resource partitions.
     * @return The partitioned resource builder.
     */
    @SuppressWarnings("unchecked")
    public T withPartitions(Collection<V> partitions) {
      config.setPartitions(partitions);
      return (T) this;
    }

    /**
     * Adds a partition to the resource.
     *
     * @param partition The partition to add.
     * @return The partitioned resource builder.
     */
    @SuppressWarnings("unchecked")
    public T addPartition(V partition) {
      config.addPartition(partition);
      return (T) this;
    }
  }

}
