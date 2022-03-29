// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.AbstractIdentifier;

import java.util.Objects;
import java.util.UUID;

/**
 * Node identifier.
 */
public class NodeId extends AbstractIdentifier<String> implements Comparable<NodeId> {

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @return node id
   */
  public static NodeId anonymous() {
    return new NodeId(UUID.randomUUID().toString());
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   * @return node id
   */
  public static NodeId from(String id) {
    return new NodeId(id);
  }

  /**
   * Constructor for serialization.
   */
  private NodeId() {
    this("");
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   */
  public NodeId(String id) {
    super(id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NodeId && ((NodeId) object).id().equals(id());
  }

  @Override
  public int compareTo(NodeId that) {
    return identifier.compareTo(that.identifier);
  }
}
