// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.config.Configured;
import io.atomix.utils.net.Address;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a node.
 */
public class Node implements Configured<NodeConfig> {

  /**
   * Returns a new member builder with no ID.
   *
   * @return the member builder
   */
  public static NodeBuilder builder() {
    return new NodeBuilder(new NodeConfig());
  }

  private final NodeId id;
  private final Address address;

  public Node(NodeConfig config) {
    this.id = config.getId();
    this.address = checkNotNull(config.getAddress(), "address cannot be null");
  }

  protected Node(NodeId id, Address address) {
    this.id = checkNotNull(id, "id cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
  }

  /**
   * Returns the instance identifier.
   *
   * @return instance identifier
   */
  public NodeId id() {
    return id;
  }

  /**
   * Returns the node address.
   *
   * @return the node address
   */
  public Address address() {
    return address;
  }

  @Override
  public NodeConfig config() {
    return new NodeConfig()
        .setId(id)
        .setAddress(address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Node) {
      Node member = (Node) object;
      return member.id().equals(id())
          && member.address().equals(address());
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(Node.class)
        .add("id", id)
        .add("address", address)
        .omitNullValues()
        .toString();
  }
}
