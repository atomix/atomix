/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster heartbeat message.
 */
final class ClusterHeartbeat {
  private final NodeId nodeId;
  private final Node.Type type;

  ClusterHeartbeat(NodeId nodeId, Node.Type type) {
    this.nodeId = nodeId;
    this.type = type;
  }

  /**
   * Returns the identifier of the node that sent the heartbeat.
   *
   * @return the identifier of the node that sent the heartbeat
   */
  public NodeId nodeId() {
    return nodeId;
  }

  /**
   * Returns the type of the node that sent the heartbeat.
   *
   * @return the node type
   */
  public Node.Type nodeType() {
    return type;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("nodeId", nodeId)
        .add("type", type)
        .toString();
  }
}
