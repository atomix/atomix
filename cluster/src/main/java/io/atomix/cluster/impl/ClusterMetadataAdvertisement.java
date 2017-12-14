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

import io.atomix.cluster.NodeId;

import java.util.Map;
import java.util.Set;

/**
 * Cluster metadata anti-entropy advertisement.
 */
final class ClusterMetadataAdvertisement {
  private final Map<NodeId, NodeDigest> digests;

  ClusterMetadataAdvertisement(Map<NodeId, NodeDigest> digests) {
    this.digests = digests;
  }

  /**
   * Returns the set of digests in the advertisement.
   *
   * @return the set of digests in the advertisement
   */
  public Set<NodeId> digests() {
    return digests.keySet();
  }

  /**
   * Returns the digest for the given node.
   *
   * @param nodeId the node for which to return the digest
   * @return the digest for the given node
   */
  public NodeDigest digest(NodeId nodeId) {
    return digests.get(nodeId);
  }
}
