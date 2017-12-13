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

import io.atomix.cluster.Node.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster metadata.
 */
public class ClusterMetadata {

  /**
   * Returns a new cluster metadata builder.
   *
   * @return a new cluster metadata builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Collection<Node> bootstrapNodes;

  public ClusterMetadata(Collection<Node> bootstrapNodes) {
    this.bootstrapNodes = bootstrapNodes.stream()
        .filter(node -> node.type() == Type.DATA)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the collection of bootstrap nodes.
   *
   * @return the collection of bootstrap nodes
   */
  public Collection<Node> bootstrapNodes() {
    return bootstrapNodes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("bootstrapNodes", bootstrapNodes)
        .toString();
  }

  /**
   * Cluster metadata builder.
   */
  public static class Builder implements io.atomix.utils.Builder<ClusterMetadata> {
    protected Collection<Node> bootstrapNodes;

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Node... bootstrapNodes) {
      return withBootstrapNodes(Arrays.asList(checkNotNull(bootstrapNodes)));
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Collection<Node> bootstrapNodes) {
      this.bootstrapNodes = checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
      return this;
    }

    @Override
    public ClusterMetadata build() {
      return new ClusterMetadata(bootstrapNodes);
    }
  }
}
