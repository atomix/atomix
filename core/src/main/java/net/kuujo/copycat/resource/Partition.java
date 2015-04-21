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

import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.util.Managed;

/**
 * Resource partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Partition<T> implements Managed<T> {

  /**
   * Initializes the partition.
   *
   * @param config The partition's resource configuration.
   */
  protected abstract void init(PartitionedResourceConfig config);

  /**
   * Partition builder.
   *
   * @param <T> The partition builder type.
   * @param <U> The partition type.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends Partition<U>> {
    protected final PartitionConfig config;

    protected Builder(PartitionConfig config) {
      this.config = config;
    }

    /**
     * Sets the partition ID.
     *
     * @param partitionId The partition ID.
     * @return The partition builder.
     */
    @SuppressWarnings("unchecked")
    public T withPartitionId(int partitionId) {
      config.setPartitionId(partitionId);
      return (T) this;
    }

    /**
     * Sets the partition protocol.
     *
     * @param protocol The partition protocol.
     * @return The partition builder.
     */
    @SuppressWarnings("unchecked")
    public T withProtocol(Protocol protocol) {
      config.setProtocol(protocol);
      return (T) this;
    }

    /**
     * Sets the partition replication strategy.
     *
     * @param replicationStrategy The partition replication strategy.
     * @return The partition builder.
     */
    @SuppressWarnings("unchecked")
    public T withReplicationStrategy(ReplicationStrategy replicationStrategy) {
      config.setReplicationStrategy(replicationStrategy);
      return (T) this;
    }

    /**
     * Sets the partition serializer.
     *
     * @param serializer The partition serializer.
     * @return The partition builder.
     */
    @SuppressWarnings("unchecked")
    public T withSerializer(CopycatSerializer serializer) {
      config.setSerializer(serializer);
      return (T) this;
    }
  }

}
