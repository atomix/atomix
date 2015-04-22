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

import net.kuujo.copycat.resource.PartitionedResource;
import net.kuujo.copycat.resource.PartitionedResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionedEventLog<K, V> extends PartitionedResource<PartitionedEventLog<K, V>, EventLogPartition<K, V>, EventLog<K, V>> implements EventLog<K, V> {

  /**
   * Returns a new partitioned state log builder.
   *
   * @return A new partitioned state log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedEventLog.class);

  public PartitionedEventLog(PartitionedResourceConfig config, List<EventLogPartition<K, V>> partitions) {
    super(config, partitions);
  }

  @Override
  public EventLog<K, V> consumer(EventConsumer<K, V> consumer) {
    partitions.forEach(p -> p.consumer(consumer));
    return this;
  }

  @Override
  public CompletableFuture<Long> commit(K key, V value) {
    if (!isOpen())
      throw new IllegalStateException("event log not open");
    return partition(key).commit(key, value);
  }

  /**
   * Partitioned state log builder.
   *
   * @param <K> The state log key type.
   * @param <V> The state log value type.
   */
  public static class Builder<K, V> extends PartitionedResource.Builder<Builder<K, V>, PartitionedEventLog<K, V>, EventLogPartition<K, V>, EventLog<K, V>> {
    private final PartitionedResourceConfig config;

    private Builder() {
      this(new PartitionedResourceConfig());
    }

    private Builder(PartitionedResourceConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public PartitionedEventLog<K, V> build() {
      return null;
    }
  }

}
