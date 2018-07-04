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
import io.atomix.utils.net.Address;

/**
 * Node configuration.
 */
public class NodeConfig implements Config {
  private NodeId id = NodeId.anonymous();
  private Address address;

  /**
   * Returns the node identifier.
   *
   * @return the node identifier
   */
  public NodeId getId() {
    return id;
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node configuration
   */
  public NodeConfig setId(String id) {
    return setId(id != null ? NodeId.from(id) : null);
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node configuration
   */
  public NodeConfig setId(NodeId id) {
    this.id = id != null ? id : NodeId.anonymous();
    return this;
  }

  /**
   * Returns the node address.
   *
   * @return the node address
   */
  public Address getAddress() {
    return address;
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  public NodeConfig setAddress(String address) {
    return setAddress(Address.from(address));
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  public NodeConfig setAddress(Address address) {
    this.address = address;
    return this;
  }
}
