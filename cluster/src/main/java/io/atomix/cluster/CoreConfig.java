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

/**
 * Core configuration.
 */
public class CoreConfig implements Config {
  private Collection<NodeConfig> nodes = new ArrayList<>();

  /**
   * Returns the core nodes.
   *
   * @return the core nodes
   */
  public Collection<NodeConfig> getNodes() {
    return nodes;
  }

  /**
   * Sets the core nodes.
   *
   * @param nodes the core nodes
   * @return the core configuration
   */
  public CoreConfig setNodes(Collection<NodeConfig> nodes) {
    this.nodes = nodes;
    return this;
  }
}
