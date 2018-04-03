/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core;

import io.atomix.cluster.ClusterConfig;
import io.atomix.primitive.PrimitiveTypeConfig;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.utils.Config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Atomix configuration.
 */
public class AtomixConfig implements Config {
  private ClusterConfig cluster = new ClusterConfig();
  private File dataDirectory = new File(System.getProperty("user.dir"), "data");
  private boolean enableShutdownHook;
  private Collection<PartitionGroupConfig> partitionGroups = new ArrayList<>();
  private PrimitiveTypeConfig primitives = new PrimitiveTypeConfig();

  /**
   * Returns the cluster configuration.
   *
   * @return the cluster configuration
   */
  public ClusterConfig getClusterConfig() {
    return cluster;
  }

  /**
   * Sets the cluster configuration.
   *
   * @param cluster the cluster configuration
   * @return the Atomix configuration
   */
  public AtomixConfig setClusterConfig(ClusterConfig cluster) {
    this.cluster = cluster;
    return this;
  }

  /**
   * Returns the data directory.
   *
   * @return the data directory
   */
  public File getDataDirectory() {
    return dataDirectory;
  }

  /**
   * Sets the data directory.
   *
   * @param dataDirectory the data directory
   * @return the Atomix configuration
   */
  public AtomixConfig setDataDirectory(File dataDirectory) {
    this.dataDirectory = dataDirectory;
    return this;
  }

  /**
   * Returns whether to enable the shutdown hook.
   *
   * @return whether to enable the shutdown hook
   */
  public boolean isEnableShutdownHook() {
    return enableShutdownHook;
  }

  /**
   * Sets whether to enable the shutdown hook.
   *
   * @param enableShutdownHook whether to enable the shutdown hook
   * @return the Atomix configuration
   */
  public AtomixConfig setEnableShutdownHook(boolean enableShutdownHook) {
    this.enableShutdownHook = enableShutdownHook;
    return this;
  }

  /**
   * Returns the partition group configurations.
   *
   * @return the partition group configurations
   */
  public Collection<PartitionGroupConfig> getPartitionGroups() {
    return partitionGroups;
  }

  /**
   * Sets the partition group configurations.
   *
   * @param partitionGroups the partition group configurations
   * @return the Atomix configuration
   */
  public AtomixConfig setPartitionGroups(Collection<PartitionGroupConfig> partitionGroups) {
    this.partitionGroups = partitionGroups;
    return this;
  }

  /**
   * Adds a partition group configuration.
   *
   * @param partitionGroup the partition group configuration to add
   * @return the Atomix configuration
   */
  public AtomixConfig addPartitionGroup(PartitionGroupConfig partitionGroup) {
    partitionGroups.add(partitionGroup);
    return this;
  }

  /**
   * Returns the primitive type configuration.
   *
   * @return the primitive type configuration
   */
  public PrimitiveTypeConfig getPrimitives() {
    return primitives;
  }

  /**
   * Sets the primitive type configuration.
   *
   * @param primitives the primitive type configuration
   * @return the Atomix configuration
   */
  public AtomixConfig setPrimitives(PrimitiveTypeConfig primitives) {
    this.primitives = primitives;
    return this;
  }
}
