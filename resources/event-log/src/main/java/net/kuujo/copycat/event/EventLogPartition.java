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
package net.kuujo.copycat.event;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.resource.Partition;
import net.kuujo.copycat.resource.PartitionedResourceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Event log partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogPartition<K, V> extends Partition<EventLog<K, V>> implements EventLog<K, V> {
  private final EventLogConfig config;
  private final int partitionId;
  private DiscreteEventLog<K, V> eventLog;

  private EventLogPartition(EventLogConfig config, int partitionId) {
    this.config = config;
    this.partitionId = partitionId;
  }

  @Override
  protected void init(PartitionedResourceConfig config) {
    this.config.setName(String.format("%s-%d", config.getName(), partitionId));
    this.config.setCluster(config.getCluster());
    this.config.setPartitions(config.getPartitions().size());
    this.eventLog = new DiscreteEventLog<>(this.config);
  }

  @Override
  public String name() {
    return config.getName();
  }

  @Override
  public Cluster cluster() {
    return eventLog.cluster();
  }

  @Override
  public EventLog<K, V> consumer(EventConsumer<K, V> consumer) {
    eventLog.consumer(consumer);
    return this;
  }

  @Override
  public CompletableFuture<Long> commit(K key, V value) {
    return eventLog.commit(key, value);
  }

  @Override
  public CompletableFuture<EventLog<K, V>> open() {
    return eventLog.open();
  }

  @Override
  public boolean isOpen() {
    return eventLog.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return eventLog.close();
  }

  @Override
  public boolean isClosed() {
    return eventLog.isClosed();
  }

  /**
   * Event log partition builder.
   */
  public static class Builder<K, V> extends Partition.Builder<Builder<K, V>, EventLogPartition<K, V>> {
    private final EventLogConfig config;

    private Builder() {
      this(new EventLogConfig());
    }

    private Builder(EventLogConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public EventLogPartition<K, V> build() {
      return new EventLogPartition<>(config, partitionId);
    }
  }

}
