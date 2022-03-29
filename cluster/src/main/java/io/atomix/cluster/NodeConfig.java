// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.config.Config;
import io.atomix.utils.net.Address;

/**
 * Node configuration.
 */
public class NodeConfig implements Config {
  private NodeId id = NodeId.anonymous();
  private String host = "localhost";
  private int port = 5679;

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
   * Returns the node hostname.
   *
   * @return the node hostname
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the node hostname.
   *
   * @param host the node hostname
   * @return the node configuration
   */
  public NodeConfig setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Returns the node port.
   *
   * @return the node port
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the node port.
   *
   * @param port the node port
   * @return the node configuration
   */
  public NodeConfig setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Returns the node address.
   *
   * @return the node address
   */
  public Address getAddress() {
    return Address.from(host, port);
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  @Deprecated
  public NodeConfig setAddress(String address) {
    return setAddress(Address.from(address));
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  @Deprecated
  public NodeConfig setAddress(Address address) {
    this.host = address.host();
    this.port = address.port();
    return this;
  }
}
