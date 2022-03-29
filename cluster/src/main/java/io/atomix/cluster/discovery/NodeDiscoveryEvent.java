// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.cluster.Node;
import io.atomix.utils.event.AbstractEvent;

/**
 * Node discovery event.
 */
public class NodeDiscoveryEvent extends AbstractEvent<NodeDiscoveryEvent.Type, Node> {

  /**
   * Node discovery event type.
   */
  public enum Type {
    /**
     * Indicates that the node joined the cluster.
     */
    JOIN,

    /**
     * Indicates that the node left the cluster.
     */
    LEAVE,
  }

  public NodeDiscoveryEvent(Type type, Node subject) {
    super(type, subject);
  }

  public NodeDiscoveryEvent(Type type, Node subject, long time) {
    super(type, subject, time);
  }

  /**
   * Returns the node.
   *
   * @return the node
   */
  public Node node() {
    return subject();
  }
}
