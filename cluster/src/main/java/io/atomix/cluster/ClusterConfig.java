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
package io.atomix.cluster;

import io.atomix.utils.Config;

/**
 * Cluster configuration.
 */
public class ClusterConfig implements Config {
  private static final String DEFAULT_CLUSTER_NAME = "atomix";

  private String name = DEFAULT_CLUSTER_NAME;
  private NodeConfig localNode;
  private CoreConfig core = new CoreConfig();
  private BootstrapConfig bootstrap = new BootstrapConfig();

  /**
   * Returns the cluster name.
   *
   * @return the cluster name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the cluster name.
   *
   * @param name the cluster name
   * @return the cluster configuration
   */
  public ClusterConfig setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Returns the local node configuration.
   *
   * @return the local node configuration
   */
  public NodeConfig getLocalNode() {
    return localNode;
  }

  /**
   * Sets the local node configuration.
   *
   * @param localNode the local node configuration
   * @return the cluster configuration
   */
  public ClusterConfig setLocalNode(NodeConfig localNode) {
    this.localNode = localNode;
    return this;
  }

  /**
   * Returns the core configuration.
   *
   * @return the core configuration.
   */
  public CoreConfig getCoreConfig() {
    return core;
  }

  /**
   * Sets the core configuration.
   *
   * @param core the core configuration
   * @return the cluster configuration
   */
  public ClusterConfig setCoreConfig(CoreConfig core) {
    this.core = core;
    return this;
  }

  /**
   * Returns the bootstrap configuration.
   *
   * @return the bootstrap configuration.
   */
  public BootstrapConfig getBootstrapConfig() {
    return bootstrap;
  }

  /**
   * Sets the bootstrap configuration.
   *
   * @param bootstrap the bootstrap configuration
   * @return the cluster configuration
   */
  public ClusterConfig setBootstrapConfig(BootstrapConfig bootstrap) {
    this.bootstrap = bootstrap;
    return this;
  }
}
