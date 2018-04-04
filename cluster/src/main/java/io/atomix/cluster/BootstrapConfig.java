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
import io.atomix.utils.net.Address;
import io.atomix.utils.net.MalformedAddressException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Bootstrap configuration.
 */
public class BootstrapConfig implements Config {
  private boolean multicastEnabled = false;
  private Address multicastAddress;
  private Collection<NodeConfig> nodes = new ArrayList<>();

  public BootstrapConfig() {
    try {
      multicastAddress = Address.from("230.0.0.1", 54321);
    } catch (MalformedAddressException e) {
      multicastAddress = Address.from(54321);
    }
  }

  /**
   * Returns whether multicast is enabled.
   *
   * @return whether multicast is enabled
   */
  public boolean isMulticastEnabled() {
    return multicastEnabled;
  }

  /**
   * Sets whether multicast is enabled.
   *
   * @param multicastEnabled whether multicast is enabled
   * @return the bootstrap configuration
   */
  public BootstrapConfig setMulticastEnabled(boolean multicastEnabled) {
    this.multicastEnabled = multicastEnabled;
    return this;
  }

  /**
   * Returns the multicast address.
   *
   * @return the multicast address
   */
  public Address getMulticastAddress() {
    return multicastAddress;
  }

  /**
   * Sets the multicast address.
   *
   * @param multicastAddress the multicast address
   * @return the bootstrap configuration
   */
  public BootstrapConfig setMulticastAddress(Address multicastAddress) {
    this.multicastAddress = multicastAddress;
    return this;
  }

  /**
   * Returns the bootstrap nodes.
   *
   * @return the bootstrap nodes
   */
  public Collection<NodeConfig> getNodes() {
    return nodes;
  }

  /**
   * Sets the bootstrap nodes.
   *
   * @param nodes the bootstrap nodes
   * @return the bootstrap configuration
   */
  public BootstrapConfig setNodes(Collection<NodeConfig> nodes) {
    this.nodes = nodes;
    return this;
  }
}
