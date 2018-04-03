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
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.utils.Config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix configuration.
 */
public class AtomixConfig implements Config {
  private ClusterConfig cluster = new ClusterConfig();
  private File dataDirectory = new File(System.getProperty("user.dir"), "data");
  private boolean enableShutdownHook;
  private Collection<PartitionGroupConfig> partitionGroups = new ArrayList<>();
  private Collection<Class<? extends PrimitiveType>> types = new ArrayList<>();
  private Map<String, PrimitiveConfig> primitives = new HashMap<>();

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
  public AtomixConfig setDataDirectory(String dataDirectory) {
    this.dataDirectory = new File(dataDirectory);
    return this;
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
   * Returns the primitive types.
   *
   * @return the primitive types
   */
  public Collection<Class<? extends PrimitiveType>> getPrimitiveTypes() {
    return types;
  }

  /**
   * Sets the primitive types.
   *
   * @param types the primitive types
   * @return the primitive type configuration
   */
  public AtomixConfig setPrimitiveTypes(Collection<Class<? extends PrimitiveType>> types) {
    this.types = types;
    return this;
  }

  /**
   * Adds a primitive type.
   *
   * @param type the type class
   * @return the primitive type configuration
   */
  public AtomixConfig addType(Class<? extends PrimitiveType> type) {
    types.add(type);
    return this;
  }

  /**
   * Returns the primitive configurations.
   *
   * @return the primitive configurations
   */
  public Map<String, PrimitiveConfig> getPrimitives() {
    return primitives;
  }

  /**
   * Sets the primitive configurations.
   *
   * @param primitives the primitive configurations
   * @return the primitive configuration holder
   */
  public AtomixConfig setPrimitives(Map<String, PrimitiveConfig> primitives) {
    this.primitives = checkNotNull(primitives);
    return this;
  }

  /**
   * Adds a primitive configuration.
   *
   * @param name   the primitive name
   * @param config the primitive configuration
   * @return the primitive configuration holder
   */
  public AtomixConfig addPrimitive(String name, PrimitiveConfig config) {
    primitives.put(name, config);
    return this;
  }

  /**
   * Returns a primitive configuration.
   *
   * @param name the primitive name
   * @param <C>  the configuration type
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>> C getPrimitive(String name) {
    return (C) primitives.get(name);
  }
}
