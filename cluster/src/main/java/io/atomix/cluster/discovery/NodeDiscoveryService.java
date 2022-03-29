// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.cluster.Node;
import io.atomix.utils.event.ListenerService;

import java.util.Set;

/**
 * Node discovery service.
 */
public interface NodeDiscoveryService extends ListenerService<NodeDiscoveryEvent, NodeDiscoveryEventListener> {

  /**
   * Returns the set of active nodes.
   *
   * @return the set of active nodes
   */
  Set<Node> getNodes();

}
