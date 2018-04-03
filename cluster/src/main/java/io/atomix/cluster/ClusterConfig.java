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

import java.util.ArrayList;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class ClusterConfig implements Config {
  private Collection<NodeConfig> nodes = new ArrayList<>();

  /**
   * Returns the collection of nodes.
   *
   * @return the collection of nodes
   */
  public Collection<NodeConfig> getNodes() {
    return nodes;
  }

  /**
   * Sets the collection of nodes.
   *
   * @param nodes the collection of nodes
   * @return the cluster configuration
   */
  public ClusterConfig setNodes(Collection<NodeConfig> nodes) {
    this.nodes = checkNotNull(nodes, "nodes cannot be null");
    return this;
  }
}
