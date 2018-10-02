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

import io.atomix.utils.config.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * Local cluster bind configuration.
 */
public class NetworkConfig implements Config {
  private List<String> interfaces = new ArrayList<>();
  private Integer port;

  /**
   * Returns the local interfaces to which to bind the node.
   *
   * @return the local interfaces to which to bind the node
   */
  public List<String> getInterfaces() {
    return interfaces;
  }

  /**
   * Sets the local interfaces to which to bind the node.
   *
   * @param interfaces the local interfaces to which to bind the node
   * @return the local cluster configuration
   */
  public NetworkConfig setInterfaces(List<String> interfaces) {
    this.interfaces = interfaces;
    return this;
  }

  /**
   * Returns the local port to which to bind the node.
   *
   * @return the local port to which to bind the node
   */
  public Integer getPort() {
    return port;
  }

  /**
   * Sets the local port to which to bind the node.
   *
   * @param port the local port to which to bind the node
   * @return the local cluster configuration
   */
  public NetworkConfig setPort(Integer port) {
    this.port = port;
    return this;
  }
}
