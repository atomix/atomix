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
package io.atomix.cluster;

import io.atomix.messaging.Endpoint;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test cluster service.
 */
public class TestClusterService implements ClusterService {
  private final NodeId localNode;
  private final Collection<NodeId> nodes;

  public TestClusterService(NodeId localNode, Collection<NodeId> nodes) {
    this.localNode = localNode;
    this.nodes = nodes;
  }

  @Override
  public Node getLocalNode() {
    return Node.builder(localNode)
        .withType(Node.Type.DATA)
        .withEndpoint(Endpoint.from("localhost", localNode.hashCode()))
        .build();
  }

  @Override
  public Set<Node> getNodes() {
    return nodes.stream()
        .map(node -> Node.builder(node)
            .withType(Node.Type.DATA)
            .withEndpoint(Endpoint.from("localhost", node.hashCode()))
            .build())
        .collect(Collectors.toSet());
  }

  @Override
  public Node getNode(NodeId nodeId) {
    return null;
  }

  @Override
  public void addListener(ClusterEventListener listener) {

  }

  @Override
  public void removeListener(ClusterEventListener listener) {

  }
}
