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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.protocol.Protocol;

/**
 * Commit log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class CommitLogConfig {
  private static int id;
  private String name;
  private ManagedCluster cluster;
  private Protocol protocol;
  private int partitions = 1;
  private Partitioner partitioner = new HashPartitioner();
  private ReplicationStrategy replicationStrategy = new FullReplicationStrategy();

  /**
   * Sets the commit log name.
   *
   * @param name The commit log name.
   */
  void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the commit log name.
   *
   * @return The commit log name.
   */
  String getName() {
    return name;
  }

  /**
   * Sets the commit log cluster.
   *
   * @param cluster The commit log cluster.
   */
  void setCluster(ManagedCluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns the commit log cluster.
   *
   * @return The commit log cluster.
   */
  ManagedCluster getCluster() {
    return cluster;
  }

  /**
   * Sets the commit log protocol.
   *
   * @param protocol The commit log protocol.
   */
  void setProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  /**
   * Returns the commit log protocol.
   *
   * @return The commit log protocol.
   */
  Protocol getProtocol() {
    return protocol;
  }

  /**
   * Sets the number of partitions.
   *
   * @param partitions The number of partitions.
   */
  void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  /**
   * Returns the number of partitions.
   *
   * @return The number of partitions.
   */
  int getPartitions() {
    return partitions;
  }

  /**
   * Sets the partitioner.
   *
   * @param partitioner The partitioner.
   */
  void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

  /**
   * Returns the partitioner.
   *
   * @return The partitioner.
   */
  Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the commit log replication strategy.
   *
   * @param replicationStrategy The commit log replication strategy.
   */
  void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
    this.replicationStrategy = replicationStrategy;
  }

  /**
   * Returns the commit log replication strategy.
   *
   * @return The commit log replication strategy.
   */
  ReplicationStrategy getReplicationStrategy() {
    return replicationStrategy;
  }

  /**
   * Resolves the commit log configuration.
   *
   * @return The resolved configuration.
   */
  CommitLogConfig resolve() {
    if (name == null)
      name = String.format("copycat-%d", ++id);
    if (cluster == null)
      throw new ConfigurationException("cluster not configured");
    return this;
  }

}
