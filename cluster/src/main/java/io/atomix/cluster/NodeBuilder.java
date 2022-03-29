// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.Builder;
import io.atomix.utils.net.Address;

/**
 * Node builder.
 */
public class NodeBuilder implements Builder<Node> {
  protected final NodeConfig config;

  protected NodeBuilder(NodeConfig config) {
    this.config = config;
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node builder
   */
  public NodeBuilder withId(String id) {
    config.setId(id);
    return this;
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node builder
   */
  public NodeBuilder withId(NodeId id) {
    config.setId(id);
    return this;
  }

  /**
   * Sets the node host.
   *
   * @param host the node host
   * @return the node builder
   */
  public NodeBuilder withHost(String host) {
    config.setHost(host);
    return this;
  }

  /**
   * Sets the node port.
   *
   * @param port the node port
   * @return the node builder
   */
  public NodeBuilder withPort(int port) {
    config.setPort(port);
    return this;
  }

  /**
   * Sets the node address.
   *
   * @param address a host:port tuple
   * @return the node builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   * @deprecated since 3.1. Use {@link #withHost(String)} and/or {@link #withPort(int)} instead
   */
  @Deprecated
  public NodeBuilder withAddress(String address) {
    return withAddress(Address.from(address));
  }

  /**
   * Sets the node host/port.
   *
   * @param host the host name
   * @param port the port number
   * @return the node builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   * @deprecated since 3.1. Use {@link #withHost(String)} and {@link #withPort(int)} instead
   */
  @Deprecated
  public NodeBuilder withAddress(String host, int port) {
    return withAddress(Address.from(host, port));
  }

  /**
   * Sets the node address using local host.
   *
   * @param port the port number
   * @return the node builder
   * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
   * @deprecated since 3.1. Use {@link #withPort(int)} instead
   */
  @Deprecated
  public NodeBuilder withAddress(int port) {
    return withAddress(Address.from(port));
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node builder
   */
  public NodeBuilder withAddress(Address address) {
    config.setAddress(address);
    return this;
  }

  @Override
  public Node build() {
    return new Node(config);
  }
}
