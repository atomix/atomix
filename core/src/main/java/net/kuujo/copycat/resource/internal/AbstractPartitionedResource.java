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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.resource.*;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Abstract resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractPartitionedResource<T extends PartitionedResource<T, U>, U extends Partition<U>> implements PartitionedResource<T, U> {
  protected final String name;
  protected final List<U> partitions;
  protected final PartitionedResourceConfig<?> config;
  protected final CopycatSerializer serializer;
  protected final Partitioner partitioner;
  protected final Executor executor;
  private CompletableFuture<T> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  protected AbstractPartitionedResource(PartitionedResourceConfig<?> config, ClusterConfig cluster) {
    this(config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + config.getName())));
  }

  protected AbstractPartitionedResource(PartitionedResourceConfig<?> config, ClusterConfig cluster, Executor executor) {
    if (config == null)
      throw new NullPointerException("config cannot be null");
    if (cluster == null)
      throw new NullPointerException("cluster cannot be null");
    if (executor == null)
      throw new NullPointerException("executor cannot be null");
    this.name = config.getName();
    this.config = config;
    this.serializer = config.getSerializer();
    this.executor = executor;
    this.partitioner = config.getPartitioner();
    this.partitions = createPartitions(cluster);
  }

  /**
   * Creates all resource partitions.
   *
   * @param cluster The cluster configuration.
   * @return A list of partitions for the resource.
   */
  protected List<U> createPartitions(ClusterConfig cluster) {
    List<U> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 1; i <= config.getPartitions(); i++) {
      PartitionConfig config = new PartitionConfig(i, this.config, cluster);
      PartitionContext context = new PartitionContext(this.config, config, cluster);
      partitions.add(createPartition(context));
    }
    return partitions;
  }

  /**
   * Creates a new partition.
   *
   * @param context The partition context.
   * @return The created partition.
   */
  protected abstract U createPartition(PartitionContext context);

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<U> partitions() {
    return partitions;
  }

  @Override
  public U partition(int id) {
    return partitions.get(id);
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
  public CompletableFuture<T> open() {
    if (open)
      return CompletableFuture.completedFuture((T) this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          CompletableFuture[] futures = new CompletableFuture[partitions.size()];
          for (int i = 0; i < partitions.size(); i++) {
            futures[i] = partitions.get(i).open();
          }
          openFuture = CompletableFuture.allOf(futures).thenRun(() -> open = true).thenApply(v -> (T) this);
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          CompletableFuture[] futures = new CompletableFuture[partitions.size()];
          for (int i = 0; i < partitions.size(); i++) {
            futures[i] = partitions.get(i).close();
          }
          closeFuture = CompletableFuture.allOf(futures).thenRun(() -> open = false);
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
