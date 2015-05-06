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
package net.kuujo.copycat.log;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.protocol.ProtocolFilter;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionedCommitLog<KEY, VALUE> extends CommitLog<KEY, VALUE> {

  /**
   * Returns a new partitioned commit log builder.
   *
   * @param <KEY> The commit log key type.
   * @param <VALUE> The commit log entry type.
   * @return The partitioned commit log builder.
   */
  public static <KEY, VALUE> Builder<KEY, VALUE> builder() {
    return new Builder<>();
  }

  private final String name;
  private final ManagedCluster cluster;
  private final List<CommitLogPartition<KEY, VALUE>> partitions;
  final Partitioner partitioner;
  private CompletableFuture<CommitLog<KEY, VALUE>> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  private PartitionedCommitLog(CommitLogConfig config) {
    config.resolve();
    this.name = config.getName();
    this.cluster = config.getCluster();
    this.partitioner = config.getPartitioner();

    List<CommitLogPartition<KEY, VALUE>> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new CommitLogPartition<>(String.format("%s-%d", name, i), new PartitionedCluster(cluster, config.getReplicationStrategy(), i, config.getPartitions()), config.getProtocol(), new ExecutionContext(String.format("%s-%d", name, i))));
    }
    this.partitions = new ImmutableList<>(partitions);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns a list of commit log partitions.
   *
   * @return A list of commit log partitions.
   */
  public List<CommitLogPartition<KEY, VALUE>> partitions() {
    return partitions;
  }

  /**
   * Returns the commit log partition for the given partition ID.
   *
   * @param id The partition ID.
   * @return The partition with the given ID.
   * @throws java.lang.IndexOutOfBoundsException If the given partition ID is outside the bounds of the log's partitions.
   */
  public CommitLogPartition<KEY, VALUE> partition(int id) {
    return partitions.get(id);
  }

  /**
   * Returns the commit log partition for the give key.
   *
   * @param key The key for which to return the partition.
   * @return The partition for the given key.
   */
  public CommitLogPartition<KEY, VALUE> partition(KEY key) {
    return partition(partitioner.partition(key, partitions.size()));
  }

  @Override
  public <RESULT> CompletableFuture<RESULT> commit(KEY key, VALUE value, Persistence persistence, Consistency consistency) {
    return partition(key).commit(key, value, persistence, consistency);
  }

  @Override
  protected CompletableFuture<Buffer> commit(Buffer key, Buffer value, Persistence persistence, Consistency consistency) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CommitLog<KEY, VALUE> filter(ProtocolFilter filter) {
    partitions.forEach(p -> p.filter(filter));
    return this;
  }

  @Override
  public <RESULT> PartitionedCommitLog<KEY, VALUE> handler(CommitHandler<KEY, VALUE, RESULT> handler) {
    partitions.forEach(p -> p.handler(handler));
    return this;
  }

  @Override
  protected CommitLog<KEY, VALUE> handler(RawCommitHandler handler) {
    partitions.forEach(p -> p.handler(handler));
    return this;
  }

  @Override
  public CompletableFuture<CommitLog<KEY, VALUE>> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          openFuture = doOpen();
        }
      }
    }
    return openFuture;
  }

  /**
   * Opens the cluster and partitions.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<CommitLog<KEY, VALUE>> doOpen() {
    return cluster.open().thenCompose(v -> {
      CompletableFuture[] futures = new CompletableFuture[partitions.size()];
      for (int i = 0; i < partitions.size(); i++) {
        futures[i] = partitions.get(i).open();
      }
      return CompletableFuture.allOf(futures);
    }).thenRun(() -> {
      openFuture = null;
      open = true;
    }).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open && openFuture == null)
      return CompletableFuture.completedFuture(null);

    if (openFuture != null) {
      synchronized (this) {
        if (openFuture != null) {
          closeFuture = openFuture.thenCompose(v -> doClose());
        } else if (closeFuture == null) {
          closeFuture = doClose();
        }
      }
    } else if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = doClose();
        }
      }
    }

    return closeFuture;
  }

  /**
   * Closes the cluster and partitions.
   */
  private CompletableFuture<Void> doClose() {
    CompletableFuture[] futures = new CompletableFuture[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      futures[i] = partitions.get(i).open();
    }
    return CompletableFuture.allOf(futures)
      .thenCompose(v -> cluster.close())
      .thenRun(() -> {
        closeFuture = null;
        open = false;
      });
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Commit log builder.
   *
   * @param <KEY> The log key.
   * @param <VALUE> The log value.
   */
  public static class Builder<KEY, VALUE> extends CommitLog.Builder<KEY, VALUE> {

    /**
     * Sets the number of commit log partitions.
     *
     * @param partitions The number of commit log partitions.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withPartitions(int partitions) {
      config.setPartitions(partitions);
      return this;
    }

    /**
     * Sets the commit log replication factor.
     *
     * @param replicationFactor The commit log replication factor.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withReplicationFactor(int replicationFactor) {
      config.setReplicationStrategy(new PartitionedReplicationStrategy(replicationFactor));
      return this;
    }

    /**
     * Sets the commit log replication strategy.
     *
     * @param strategy The commit log replication strategy.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withReplicationStrategy(ReplicationStrategy strategy) {
      config.setReplicationStrategy(strategy);
      return this;
    }

    @Override
    public PartitionedCommitLog<KEY, VALUE> build() {
      return new PartitionedCommitLog<>(config);
    }
  }

}
