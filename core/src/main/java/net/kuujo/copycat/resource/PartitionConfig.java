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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.protocol.Protocol;

/**
 * Partition configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PartitionConfig extends Config {
  private int partitionId;
  private Protocol protocol;
  private ReplicationStrategy replicationStrategy;
  private CopycatSerializer serializer;

  /**
   * Sets the partition ID.
   *
   * @param partitionId The partition ID.
   */
  protected void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Returns the partition ID.
   *
   * @return The partition ID.
   */
  public int getPartitionId() {
    return partitionId;
  }

  /**
   * Sets the partition protocol.
   *
   * @param protocol The partition protocol.
   */
  protected void setProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  /**
   * Returns the partition protocol.
   *
   * @return The partition protocol.
   */
  public Protocol getProtocol() {
    return protocol;
  }

  /**
   * Sets the partition replication strategy.
   *
   * @param replicationStrategy The partition replication strategy.
   */
  protected void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
    this.replicationStrategy = replicationStrategy;
  }

  /**
   * Returns the partition replication strategy.
   *
   * @return The partition replication strategy.
   */
  public ReplicationStrategy getReplicationStrategy() {
    return replicationStrategy;
  }

  /**
   * Sets the partition serializer.
   *
   * @param serializer The partition serializer.
   */
  protected void setSerializer(CopycatSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns the partition serializer.
   *
   * @return The partition serializer.
   */
  public CopycatSerializer getSerializer() {
    return serializer;
  }

  @Override
  protected PartitionConfig resolve() {
    if (partitionId < 0)
      throw new ConfigurationException("partition ID cannot be negative");
    if (protocol == null)
      throw new ConfigurationException("protocol not configured");
    if (replicationStrategy == null)
      replicationStrategy = new FullReplicationStrategy();
    if (serializer == null)
      serializer = new CopycatSerializer();
    return this;
  }

}
