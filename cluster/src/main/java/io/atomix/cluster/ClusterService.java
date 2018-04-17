/*
 * Copyright 2014-present Open Networking Foundation
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

import io.atomix.utils.event.ListenerService;

import java.util.Set;

/**
 * Service for obtaining information about the individual nodes within
 * the controller cluster.
 */
public interface ClusterService extends ListenerService<ClusterEvent, ClusterEventListener> {

  /**
   * Returns the local controller node.
   *
   * @return local controller node
   */
  Node getLocalNode();

  /**
   * Returns the set of current cluster members.
   *
   * @return set of cluster members
   */
  Set<Node> getNodes();

  /**
   * Returns the specified controller node.
   * <p>
   * This is a convenience method that wraps the given {@link String} in a {@link NodeId}. To avoid unnecessary
   * object allocation, repeated invocations of this method should instead use {@link #getNode(NodeId)}.
   *
   * @param nodeId the controller node identifier
   * @return the controller node or {@code null} if no node with the given identifier exists
   */
  default Node getNode(String nodeId) {
    return getNode(NodeId.from(nodeId));
  }

  /**
   * Returns the specified controller node.
   *
   * @param nodeId the controller node identifier
   * @return the controller node or {@code null} if no node with the given identifier exists
   */
  Node getNode(NodeId nodeId);

}
